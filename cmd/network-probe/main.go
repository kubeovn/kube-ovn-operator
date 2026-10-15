package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/probe"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	identity := probe.Identity{Pod: os.Getenv("POD_NAME"), PodUID: os.Getenv("POD_UID"), Node: os.Getenv("NODE_NAME")}
	if identity.Pod == "" || identity.PodUID == "" || identity.Node == "" {
		return fmt.Errorf("pod identity environment is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) == 2 && os.Args[1] == "serve" {
		server := &http.Server{Addr: fmt.Sprintf(":%d", probe.Port), Handler: probe.Handler(identity), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4096}
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		}()
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
	if len(os.Args) != 3 || os.Args[1] != "check" || len(os.Args[2]) > 4096 {
		return fmt.Errorf("usage: network-probe serve | check REQUEST_JSON")
	}
	var request probe.Request
	if err := json.Unmarshal([]byte(os.Args[2]), &request, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("invalid request JSON")
	}
	result, err := probe.Check(ctx, identity, request)
	if err != nil {
		return err
	}
	return json.MarshalWrite(os.Stdout, result)
}
