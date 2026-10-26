// Command fence-agent is an optional sidecar reference implementation for
// the atomic OVN membership fence. It observes one central Kubernetes Lease,
// arms the in-process Gate from its current identity, and exposes only the
// canonical membership command handler over TLS.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/fencing"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	var listenAddress string
	var leaseNamespace string
	var leaseName string
	var installationUID string
	var certificateFile string
	var keyFile string
	var clientCAFile string
	var pollInterval time.Duration
	var commandTimeout time.Duration
	flag.StringVar(&listenAddress, "listen-address", ":8443", "HTTPS listen address")
	flag.StringVar(&leaseNamespace, "lease-namespace", "", "Namespace containing the central operation Lease")
	flag.StringVar(&leaseName, "lease-name", "", "Name of the central operation Lease")
	flag.StringVar(&installationUID, "installation-uid", "", "UID of the bound KubeOVNInstallation; when omitted, derive it from the Lease owner reference")
	flag.StringVar(&certificateFile, "tls-cert-file", "", "TLS certificate file")
	flag.StringVar(&keyFile, "tls-key-file", "", "TLS private key file")
	flag.StringVar(&clientCAFile, "tls-client-ca-file", "", "CA bundle used to authenticate operator clients")
	flag.DurationVar(&pollInterval, "lease-poll-interval", time.Second, "Lease observation interval")
	flag.DurationVar(&commandTimeout, "command-timeout", 10*time.Second, "Maximum native command duration")
	flag.Parse()
	if err := validateFlags(leaseNamespace, leaseName, installationUID, certificateFile, keyFile, clientCAFile, pollInterval, commandTimeout); err != nil {
		slog.Error("invalid fence-agent configuration", "error", err)
		os.Exit(2)
	}
	tlsConfig, err := serverTLSConfig(clientCAFile)
	if err != nil {
		slog.Error("load fence-agent client CA", "error", err)
		os.Exit(2)
	}

	config := ctrl.GetConfigOrDie()
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Error("create Kubernetes client", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gate := new(fencing.Gate)
	leaseClient := clientset.CoordinationV1().Leases(leaseNamespace)
	go observeLease(ctx, leaseClient, leaseName, types.UID(installationUID), pollInterval, gate)

	server := &http.Server{
		Addr: listenAddress,
		Handler: &fencing.Handler{
			Gate:    gate,
			Execute: fencing.NativeExecutor{}.Execute,
			Restore: fencing.NativeExecutor{}.Restore,
			Timeout: commandTimeout,
		},
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown fence-agent", "error", err)
		}
	}()

	slog.Info("starting fence-agent", "address", listenAddress, "leaseNamespace", leaseNamespace, "leaseName", leaseName)
	if err := server.ListenAndServeTLS(certificateFile, keyFile); err != nil && err != http.ErrServerClosed {
		slog.Error("fence-agent stopped", "error", err)
		os.Exit(1)
	}
	gate.Clear()
}

func validateFlags(namespace, name, installationUID, certificateFile, keyFile, clientCAFile string, pollInterval, commandTimeout time.Duration) error {
	for label, value := range map[string]string{"lease-namespace": namespace, "lease-name": name, "tls-cert-file": certificateFile, "tls-key-file": keyFile, "tls-client-ca-file": clientCAFile} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("--%s is required", label)
		}
	}
	if pollInterval <= 0 || commandTimeout <= 0 {
		return errors.New("lease-poll-interval and command-timeout must be positive")
	}
	return nil
}

func serverTLSConfig(clientCAFile string) (*tls.Config, error) {
	data, err := os.ReadFile(strings.TrimSpace(clientCAFile))
	if err != nil {
		return nil, fmt.Errorf("read TLS client CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("TLS client CA file contains no PEM certificates")
	}
	return &tls.Config{
		MinVersion:               tls.VersionTLS13,
		ClientAuth:               tls.RequireAndVerifyClientCert,
		ClientCAs:                pool,
		PreferServerCipherSuites: true,
	}, nil
}

func observeLease(ctx context.Context, leases coordinationclient.LeaseInterface, name string, installationUID types.UID, pollInterval time.Duration, gate *fencing.Gate) {
	for {
		if err := observeLeaseStream(ctx, leases, name, installationUID, gate); err != nil {
			gate.Clear()
			if ctx.Err() != nil {
				return
			}
			slog.Error("observe central operation Lease", "error", err)
		}
		if err := waitRetry(ctx, pollInterval); err != nil {
			gate.Clear()
			return
		}
	}
}

func observeLeaseStream(ctx context.Context, leases coordinationclient.LeaseInterface, name string, installationUID types.UID, gate *fencing.Gate) error {
	if leases == nil || strings.TrimSpace(name) == "" || installationUID == "" || gate == nil {
		return errors.New("Lease observer requires a client, name, installation UID and gate")
	}
	record, err := leases.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		gate.Clear()
		return errors.New("central operation Lease does not exist")
	}
	if err != nil {
		return err
	}
	if err := armFromRecord(record, name, installationUID, gate); err != nil {
		return err
	}
	expiry := leaseExpiryTimer(record)
	defer expiry.Stop()
	resourceVersion := record.ResourceVersion
	if resourceVersion == "" {
		return errors.New("central operation Lease has no resource version")
	}
	stream, err := leases.Watch(ctx, metav1.ListOptions{
		FieldSelector:       "metadata.name=" + name,
		ResourceVersion:     resourceVersion,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		return fmt.Errorf("watch central operation Lease: %w", err)
	}
	defer stream.Stop()
	for {
		select {
		case <-ctx.Done():
			gate.Clear()
			return ctx.Err()
		case <-expiry.C:
			gate.Clear()
			return errors.New("central operation Lease expired")
		case event, ok := <-stream.ResultChan():
			if !ok {
				return errors.New("central operation Lease watch closed")
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				leaseRecord, ok := event.Object.(*coordinationv1.Lease)
				if !ok {
					return errors.New("central operation Lease watch returned an unexpected object")
				}
				if err := armFromRecord(leaseRecord, name, installationUID, gate); err != nil {
					return err
				}
				resetLeaseExpiry(expiry, leaseRecord)
			case watch.Deleted:
				gate.Clear()
				return errors.New("central operation Lease was deleted")
			case watch.Bookmark:
				continue
			case watch.Error:
				status, ok := event.Object.(*metav1.Status)
				if !ok {
					return errors.New("central operation Lease watch returned an unknown error")
				}
				return apierrors.FromObject(status)
			default:
				return fmt.Errorf("central operation Lease watch returned event %q", event.Type)
			}
		}
	}
}

func armFromLease(ctx context.Context, leases coordinationclient.LeaseInterface, name string, installationUID types.UID, gate *fencing.Gate) error {
	if leases == nil {
		return errors.New("Lease client is required")
	}
	record, err := leases.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		gate.Clear()
		return err
	}
	return armFromRecord(record, name, installationUID, gate)
}

func armFromRecord(record *coordinationv1.Lease, name string, installationUID types.UID, gate *fencing.Gate) error {
	if record == nil || record.Name != name {
		return errors.New("central operation Lease identity does not match the configured name")
	}
	if installationUID == "" {
		installationUID = installationUIDFromOwner(record)
		if installationUID == "" {
			return errors.New("central operation Lease has no KubeOVNInstallation owner UID")
		}
	}
	if !leaseHeld(record, time.Now()) {
		gate.Clear()
		return errors.New("central operation Lease is not currently held")
	}
	if record.Spec.LeaseTransitions == nil {
		return errors.New("central operation Lease has no transition epoch")
	}
	holder := strings.TrimSpace(*record.Spec.HolderIdentity)
	epoch := *record.Spec.LeaseTransitions
	token, err := fencing.TokenFromLease(installationUID, record.UID, holder, epoch)
	if err != nil {
		return err
	}
	return gate.Arm(token)
}

func installationUIDFromOwner(record *coordinationv1.Lease) types.UID {
	if record == nil {
		return ""
	}
	for _, owner := range record.OwnerReferences {
		if owner.APIVersion == "operator.kubeovn.io/v1alpha1" && owner.Kind == "KubeOVNInstallation" && owner.UID != "" {
			return owner.UID
		}
	}
	return ""
}

func waitRetry(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func leaseHeld(record *coordinationv1.Lease, now time.Time) bool {
	if record == nil || record.Spec.HolderIdentity == nil || strings.TrimSpace(*record.Spec.HolderIdentity) == "" || record.Spec.LeaseDurationSeconds == nil || *record.Spec.LeaseDurationSeconds <= 0 || record.Spec.RenewTime == nil {
		return false
	}
	return now.Before(record.Spec.RenewTime.Time.Add(time.Duration(*record.Spec.LeaseDurationSeconds) * time.Second))
}

func leaseExpiryTimer(record *coordinationv1.Lease) *time.Timer {
	duration := time.Nanosecond
	if record != nil && record.Spec.RenewTime != nil && record.Spec.LeaseDurationSeconds != nil && *record.Spec.LeaseDurationSeconds > 0 {
		duration = time.Until(record.Spec.RenewTime.Time.Add(time.Duration(*record.Spec.LeaseDurationSeconds) * time.Second))
		if duration < 0 {
			duration = 0
		}
	}
	return time.NewTimer(duration)
}

func resetLeaseExpiry(timer *time.Timer, record *coordinationv1.Lease) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	duration := time.Nanosecond
	if record != nil && record.Spec.RenewTime != nil && record.Spec.LeaseDurationSeconds != nil && *record.Spec.LeaseDurationSeconds > 0 {
		duration = time.Until(record.Spec.RenewTime.Time.Add(time.Duration(*record.Spec.LeaseDurationSeconds) * time.Second))
		if duration < 0 {
			duration = 0
		}
	}
	timer.Reset(duration)
}
