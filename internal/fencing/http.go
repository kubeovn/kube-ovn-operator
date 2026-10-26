package fencing

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/types"
)

const (
	ExecutePath     = "/v1/fence/execute"
	RestorePath     = "/v1/fence/restore"
	HealthPath      = "/healthz"
	ReadyPath       = "/readyz"
	maxRequestBytes = 8 << 10
	maxReplyBytes   = 128 << 10
	maxRestoreBytes = 1 << 30
)

// Request is the wire contract shared by the Operator client and the remote
// fence service. The mutation handler requires a non-nil Token.
type Request struct {
	Command []string          `json:"command"`
	Token   *lease.FenceToken `json:"token,omitempty"`
}

// Response is returned for both successful and failed requests. Error
// responses use Code and Message; Output is bounded and intended for native
// command diagnostics, not arbitrary file transfer.
type Response struct {
	Output  string `json:"output,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// CommandExecutor executes a command after Gate has admitted its token. It
// must not launch work asynchronously after returning.
type CommandExecutor func(context.Context, []string) ([]byte, error)

// RestoreExecutor consumes one verified native backup and writes it into the
// already fenced OVN database process. The handler keeps the fence lock until
// this callback returns, so Lease takeover cannot race with restore admission.
type RestoreExecutor func(context.Context, string, io.Reader) error

// Handler is a small HTTP adapter around Gate. It deliberately has no arm
// endpoint: only the trusted Lease controller that owns the process may call
// Gate.Arm and Gate.Disarm.
type Handler struct {
	Gate     *Gate
	Execute  CommandExecutor
	Restore  RestoreExecutor
	Validate func([]string) error
	Timeout  time.Duration
	// restoreLimit is an internal test seam; production handlers use the
	// fixed maxRestoreBytes limit and cannot loosen it through the wire.
	restoreLimit int64
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == HealthPath {
		writeHealthResponse(writer, http.StatusOK)
		return
	}
	if request.Method == http.MethodGet && request.URL.Path == ReadyPath {
		if h == nil || h.Gate == nil {
			writeHealthResponse(writer, http.StatusServiceUnavailable)
			return
		}
		_, armed := h.Gate.Current()
		if !armed {
			writeHealthResponse(writer, http.StatusServiceUnavailable)
			return
		}
		writeHealthResponse(writer, http.StatusOK)
		return
	}
	if request.Method == http.MethodPost && request.URL.Path == RestorePath {
		h.serveRestore(writer, request)
		return
	}
	if request.Method != http.MethodPost || request.URL.Path != ExecutePath {
		writeResponse(writer, http.StatusNotFound, Response{Code: "not_found", Message: "fence endpoint not found"})
		return
	}
	if h == nil || h.Gate == nil || h.Execute == nil {
		writeResponse(writer, http.StatusServiceUnavailable, Response{Code: "fence_not_armed", Message: "fence handler is not configured"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes+1))
	if err != nil {
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_request", Message: err.Error()})
		return
	}
	if len(body) > maxRequestBytes {
		writeResponse(writer, http.StatusRequestEntityTooLarge, Response{Code: "invalid_request", Message: "fence request exceeds 8 KiB"})
		return
	}
	var payload Request
	if err := json.Unmarshal(body, &payload); err != nil {
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_request", Message: "invalid JSON request"})
		return
	}
	validate := h.Validate
	if validate == nil {
		validate = ValidateMembershipCommand
	}
	if err := validate(payload.Command); err != nil {
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_command", Message: err.Error()})
		return
	}
	if payload.Token == nil {
		writeResponse(writer, http.StatusConflict, Response{Code: "fence_not_armed", Message: "mutating command requires a fence token"})
		return
	}
	ctx := request.Context()
	if h.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}
	output, err := h.Gate.Execute(ctx, *payload.Token, func(ctx context.Context) ([]byte, error) {
		return h.Execute(ctx, payload.Command)
	})
	if err != nil {
		code, message := wrapCode(err)
		status := http.StatusInternalServerError
		if errors.Is(err, ErrNotArmed) || errors.Is(err, ErrStale) {
			status = http.StatusConflict
		} else if errors.Is(err, ErrInvalid) || errors.Is(err, ErrNoCommand) {
			status = http.StatusBadRequest
		}
		writeResponse(writer, status, Response{Code: code, Message: message})
		return
	}
	if len(output) > maxReplyBytes {
		writeResponse(writer, http.StatusInternalServerError, Response{Code: "output_too_large", Message: "fence command output exceeds 128 KiB"})
		return
	}
	writeResponse(writer, http.StatusOK, Response{Output: string(output)})
}

func (h *Handler) serveRestore(writer http.ResponseWriter, request *http.Request) {
	if h == nil || h.Gate == nil || h.Restore == nil {
		writeResponse(writer, http.StatusServiceUnavailable, Response{Code: "fence_not_armed", Message: "restore fence handler is not configured"})
		return
	}
	token, err := tokenFromHeaders(request)
	if err != nil {
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_fence", Message: err.Error()})
		return
	}
	database := strings.TrimSpace(request.Header.Get("X-Kube-OVN-Database"))
	if err := ValidateRestoreDatabase(database); err != nil {
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_database", Message: err.Error()})
		return
	}
	if request.Body == nil {
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_request", Message: "restore body is required"})
		return
	}
	limit := h.restoreBodyLimit()
	if request.ContentLength > limit {
		writeResponse(writer, http.StatusRequestEntityTooLarge, Response{Code: "restore_too_large", Message: "restore body exceeds 1 GiB"})
		return
	}
	ctx := request.Context()
	if h.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}
	input, err := stageRestoreInput(ctx, request.Body, limit)
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeResponse(writer, http.StatusRequestEntityTooLarge, Response{Code: "restore_too_large", Message: "restore body exceeds 1 GiB"})
			return
		}
		writeResponse(writer, http.StatusBadRequest, Response{Code: "invalid_request", Message: err.Error()})
		return
	}
	defer func() {
		_ = input.Close()
		_ = os.Remove(input.Name())
	}()
	err = h.Gate.ExecuteRestore(ctx, token, func(ctx context.Context) error {
		return h.Restore(ctx, database, input)
	})
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeResponse(writer, http.StatusRequestEntityTooLarge, Response{Code: "restore_too_large", Message: "restore body exceeds 1 GiB"})
			return
		}
		code, message := wrapCode(err)
		status := http.StatusInternalServerError
		if errors.Is(err, ErrNotArmed) || errors.Is(err, ErrStale) {
			status = http.StatusConflict
		} else if errors.Is(err, ErrInvalid) || errors.Is(err, ErrNoCommand) {
			status = http.StatusBadRequest
		}
		writeResponse(writer, status, Response{Code: code, Message: message})
		return
	}
	writeResponse(writer, http.StatusOK, Response{})
}

func (h *Handler) restoreBodyLimit() int64 {
	if h != nil && h.restoreLimit > 0 {
		return h.restoreLimit
	}
	return maxRestoreBytes
}

// stageRestoreInput fully receives a restore body before the native executor
// sees it. This prevents an unknown-length request that exceeds the limit from
// feeding a partial snapshot into ovsdb-client before the size error arrives.
func stageRestoreInput(ctx context.Context, input io.Reader, limit int64) (*os.File, error) {
	if ctx == nil {
		return nil, errors.New("restore context is required")
	}
	if input == nil {
		return nil, errors.New("restore body is required")
	}
	if limit < 1 {
		return nil, errors.New("restore body limit must be positive")
	}
	temporary, err := os.CreateTemp("", "kube-ovn-fence-restore-*")
	if err != nil {
		return nil, fmt.Errorf("stage restore body: %w", err)
	}
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporary.Name())
	}
	reader := &contextReader{ctx: ctx, reader: io.LimitReader(input, limit+1)}
	written, err := io.Copy(temporary, reader)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("stage restore body: %w", err)
	}
	if written > limit {
		cleanup()
		return nil, &http.MaxBytesError{Limit: limit}
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return nil, fmt.Errorf("sync staged restore body: %w", err)
	}
	if _, err := temporary.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, fmt.Errorf("rewind staged restore body: %w", err)
	}
	return temporary, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(buffer)
	}
}

func writeHealthResponse(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte("ok\n"))
}

func tokenFromHeaders(request *http.Request) (lease.FenceToken, error) {
	epoch, err := strconv.ParseInt(strings.TrimSpace(request.Header.Get("X-Kube-OVN-Fence-Epoch")), 10, 32)
	if err != nil || epoch < 1 {
		return lease.FenceToken{}, ErrInvalid
	}
	token := lease.FenceToken{
		InstallationUID: types.UID(strings.TrimSpace(request.Header.Get("X-Kube-OVN-Installation-UID"))),
		LeaseUID:        types.UID(strings.TrimSpace(request.Header.Get("X-Kube-OVN-Lease-UID"))),
		Scope:           lease.Scope(strings.TrimSpace(request.Header.Get("X-Kube-OVN-Fence-Scope"))),
		Holder:          strings.TrimSpace(request.Header.Get("X-Kube-OVN-Fence-Holder")),
		Epoch:           int32(epoch),
	}
	if err := ValidateToken(token); err != nil {
		return lease.FenceToken{}, err
	}
	return token, nil
}

func ValidateCommand(command []string) error {
	if len(command) == 0 || len(command) > 16 {
		return errors.New("fenced command must contain between one and sixteen arguments")
	}
	total := 0
	for _, argument := range command {
		if strings.TrimSpace(argument) == "" || len(argument) > 512 {
			return errors.New("fenced command contains an empty or oversized argument")
		}
		total += len(argument)
	}
	if total > 4096 {
		return errors.New("fenced command arguments exceed 4 KiB")
	}
	return nil
}

// ValidateMembershipCommand restricts the reference handler to the exact
// ovs-appctl operations emitted by ovnadmin.CommandClient. A production
// service must not turn the generic fence endpoint into an arbitrary command
// runner.
func ValidateMembershipCommand(command []string) error {
	if err := ValidateCommand(command); err != nil {
		return err
	}
	if len(command) < 6 {
		return errors.New("fenced membership command is incomplete")
	}
	if command[0] != "ovs-appctl" || command[1] != "--timeout=5" || command[2] != "-t" {
		return errors.New("fenced membership command must use the fixed ovs-appctl invocation")
	}
	database := ""
	switch command[3] {
	case "/var/run/ovn/ovnnb_db.ctl":
		database = "OVN_Northbound"
	case "/var/run/ovn/ovnsb_db.ctl":
		database = "OVN_Southbound"
	default:
		return errors.New("fenced membership command must use an OVN database control socket")
	}
	switch command[4] {
	case "cluster/leave":
		if len(command) != 6 || command[5] != database {
			return errors.New("cluster/leave command is not canonical")
		}
	case "cluster/kick":
		if len(command) != 7 || command[5] != database || !validServerID(command[6]) {
			return errors.New("cluster/kick command is not canonical")
		}
	default:
		return errors.New("unsupported fenced membership operation")
	}
	return nil
}

func ValidateRestoreDatabase(database string) error {
	if database != "OVN_Northbound" && database != "OVN_Southbound" {
		return errors.New("restore database must be OVN_Northbound or OVN_Southbound")
	}
	return nil
}

func validServerID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return false
			}
			continue
		}
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') && (char < 'A' || char > 'F') {
			return false
		}
	}
	return true
}

func writeResponse(writer http.ResponseWriter, status int, response Response) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	body, err := json.Marshal(response)
	if err != nil {
		return
	}
	_, _ = writer.Write(body)
}

var _ http.Handler = (*Handler)(nil)
