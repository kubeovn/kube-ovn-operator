package health

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type CommandRunner interface {
	Run(context.Context, string, string, string, []string) ([]byte, error)
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
	request := e.client.RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(e.config, "POST", request.URL())
	if err != nil {
		return nil, fmt.Errorf("create pod executor: %w", err)
	}
	output := new(boundedBuffer)
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: output, Stderr: io.Discard}); err != nil {
		return nil, fmt.Errorf("read OVN metadata from pod %s: %w", pod, err)
	}
	// client-go logs stdout writer errors without returning them from Stream.
	// Reject truncated output even when the remote command reports success.
	if output.err != nil {
		return nil, output.err
	}
	return output.Bytes(), nil
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
