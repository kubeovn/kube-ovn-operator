package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type CommandRunner interface {
	Run(context.Context, string, string, string, []string) ([]byte, error)
}

// FencedCommandRunner is implemented only by a remote executor that can
// atomically validate the supplied lease fence before starting a mutating
// command. A plain PodExecutor intentionally does not implement it: cancelling
// a pods/exec stream cannot revoke a command already accepted by the container.
type FencedCommandRunner interface {
	CommandRunner
	RunFenced(context.Context, string, string, string, []string, lease.FenceToken) ([]byte, error)
}

type StreamingCommandRunner interface {
	Stream(context.Context, string, string, string, []string, io.Writer) error
}

// InputStreamingCommandRunner is the explicit destructive transport used by
// native OVSDB restore. A normal pods/exec reader cannot be upgraded into this
// capability merely by adding arguments to a command.
type InputStreamingCommandRunner interface {
	StreamInput(context.Context, string, string, string, []string, io.Reader) error
}

// FencedInputStreamingCommandRunner is the destructive restore transport. It
// must atomically compare the supplied Lease token on the server side before
// feeding the input to the native process.
type FencedInputStreamingCommandRunner interface {
	InputStreamingCommandRunner
	StreamInputFenced(context.Context, string, string, string, string, io.Reader, lease.FenceToken) error
}

type PodExecutor struct {
	config *rest.Config
	client coreclient.CoreV1Interface
}

func NewPodExecutor(config *rest.Config) (*PodExecutor, error) {
	config = rest.CopyConfig(config)
	client, err := coreclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &PodExecutor{config: config, client: client}, nil
}

func (e *PodExecutor) Run(ctx context.Context, namespace, pod, container string, command []string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output := new(boundedBuffer)
	if err := e.Stream(ctx, namespace, pod, container, command, output); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

// Stream avoids buffering database snapshots in memory. The caller supplies a
// bounded destination. Writer errors cancel exec and remain visible even when
// client-go only logs a stdout-copy error or the remote process exits zero.
func (e *PodExecutor) Stream(ctx context.Context, namespace, pod, container string, command []string, destination io.Writer) error {
	if destination == nil {
		return errors.New("pod stream destination is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	request := e.client.RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(e.config, "POST", request.URL())
	if err != nil {
		return fmt.Errorf("create pod executor: %w", err)
	}
	output := &streamOutput{destination: destination, cancel: cancel}
	streamErr := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: output, Stderr: io.Discard})
	if err := errors.Join(output.finish(), streamErr, ctx.Err()); err != nil {
		return fmt.Errorf("stream from pod %s: %w", pod, err)
	}
	return nil
}

// StreamInput sends a bounded, already verified native backup to a pinned
// container. The command is accepted only after the caller has validated the
// Pod/container identity and operation lease.
func (e *PodExecutor) StreamInput(ctx context.Context, namespace, pod, container string, command []string, input io.Reader) error {
	if input == nil {
		return errors.New("pod stream input is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	request := e.client.RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdin: true, Stderr: true}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(e.config, "POST", request.URL())
	if err != nil {
		return fmt.Errorf("create pod input executor: %w", err)
	}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: input, Stderr: io.Discard}); err != nil {
		return fmt.Errorf("stream input to pod %s: %w", pod, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stream input to pod %s: %w", pod, err)
	}
	return nil
}

type streamOutput struct {
	mu          sync.Mutex
	destination io.Writer
	cancel      context.CancelFunc
	err         error
	finished    bool
}

func (w *streamOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return 0, io.ErrClosedPipe
	}
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.destination.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		w.cancel()
	}
	return n, err
}

func (w *streamOutput) finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.finished = true
	return w.err
}

type boundedBuffer struct {
	buffer bytes.Buffer
	err    error
}

func (b *boundedBuffer) Bytes() []byte { return b.buffer.Bytes() }

func (b *boundedBuffer) Write(data []byte) (int, error) {
	if b.buffer.Len()+len(data) > 64*1024 {
		b.err = fmt.Errorf("OVN metadata exceeds 64 KiB")
		return 0, b.err
	}
	return b.buffer.Write(data)
}
