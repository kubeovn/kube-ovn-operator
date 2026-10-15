// Package central starts and supervises an explicitly prepared OVN member.
// Authorization, remote quorum and Kubernetes lifecycle state belong to the
// caller; this module never bootstraps, repairs or removes database files.
package central

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

// Request must be persisted and authorized by the lifecycle executor before a
// workload starts. Prepared is the exact result previously recorded by it.
// TLS contains mounted file paths only, never inline credential material.
type Request struct {
	FormatVersion     int            `json:"formatVersion"`
	Preparation       prepare.Plan   `json:"preparation"`
	Prepared          prepare.Result `json:"prepared"`
	NorthboundAddress string         `json:"northboundAddress"`
	SouthboundAddress string         `json:"southboundAddress"`
	NorthboundRemotes []string       `json:"northboundRemotes"`
	SouthboundRemotes []string       `json:"southboundRemotes"`
	TLS               *TLSFiles      `json:"tls,omitempty"`
}

type TLSFiles struct {
	PrivateKey  string `json:"privateKey"`
	Certificate string `json:"certificate"`
	CA          string `json:"ca"`
}

func (r Request) validate() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.TLS != nil {
		for _, path := range []string{r.TLS.PrivateKey, r.TLS.Certificate, r.TLS.CA} {
			// Projected Kubernetes Secret mounts use symlinks. Only inspect file
			// metadata here; native OVN consumes the existing credentials.
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
				return errors.New("TLS files must already exist as nonempty regular files")
			}
		}
	}
	return nil
}

// Validate checks the portable launch contract. File existence and native
// identity checks take place inside the member immediately before startup.
func (r Request) Validate() error {
	if r.FormatVersion != 1 || len(r.Preparation.Databases) != 2 {
		return errors.New("launch requires format version 1 and both prepared databases")
	}
	addresses := []string{r.NorthboundAddress, r.SouthboundAddress}
	for _, db := range r.Preparation.Databases {
		addresses = append(addresses, db.LocalAddress)
	}
	used := make(map[string]bool)
	ssl := false
	for _, address := range addresses {
		if _, err := passiveAddress(address); err != nil {
			return err
		}
		protocol, endpoint, _ := strings.Cut(address, ":")
		if used[endpoint] {
			return errors.New("NB/SB client and RAFT listeners must use distinct endpoints")
		}
		used[endpoint] = true
		ssl = ssl || protocol == "ssl"
	}
	if ssl != (r.TLS != nil) {
		return errors.New("SSL listeners require explicit TLS files; TCP-only launch must omit TLS")
	}
	for i, remotes := range [][]string{r.NorthboundRemotes, r.SouthboundRemotes} {
		local := []string{r.NorthboundAddress, r.SouthboundAddress}[i]
		if len(remotes) < 2 || len(remotes) > 8 || !slices.Contains(remotes, local) {
			return errors.New("northd requires explicit peer client endpoints including the new member")
		}
		seen := make(map[string]bool)
		protocol, _, _ := strings.Cut(local, ":")
		for _, remote := range remotes {
			if _, err := passiveAddress(remote); err != nil {
				return err
			}
			remoteProtocol, _, _ := strings.Cut(remote, ":")
			if seen[remote] || remoteProtocol != protocol {
				return errors.New("northd peer endpoints must be distinct and use the listener protocol")
			}
			seen[remote] = true
		}
	}
	if r.TLS != nil {
		for _, path := range []string{r.TLS.PrivateKey, r.TLS.Certificate, r.TLS.CA} {
			if !filepath.IsAbs(path) {
				return errors.New("TLS files must use absolute mounted paths")
			}
		}
	}
	return nil
}

func passiveAddress(address string) (string, error) {
	if len(address) > 80 {
		return "", errors.New("client address exceeds 80 bytes")
	}
	if err := membership.ValidateMembers([]membership.Member{{ServerID: "11111111-1111-1111-1111-111111111111", Address: address}}); err != nil {
		return "", fmt.Errorf("invalid launch address: %w", err)
	}
	protocol, endpoint, _ := strings.Cut(address, ":")
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", err
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return "p" + protocol + ":" + port + ":" + host, nil
}
