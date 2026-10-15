package prepare

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"time"
)

// Prepared holds exclusive access to an already published pair. Close it only
// after every child has been reaped. Children started with Command inherit the
// directory and lock, keeping the files pinned and excluding another actor even
// if their supervisor crashes. The trusted native tools must keep inherited fds.
type Prepared struct {
	directory *os.File
	lock      *os.File
	result    Result
}

// Acquire validates the complete published request/result against the caller's
// persisted identities and the native files. It never prepares missing data.
func (p *Preparer) Acquire(ctx context.Context, requested Plan, expected Result) (*Prepared, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	plan, digest, err := canonical(requested)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lock, err := p.lock()
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = lock.Close()
		}
	}()
	var recorded Plan
	if err := p.readJSON(plan.OperationID+"/request.json", &recorded); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(recorded, plan) {
		return nil, errors.New("prepared request differs from the authorized launch request")
	}
	var result Result
	if err := p.readJSON(plan.OperationID+"/prepared.json", &result); err != nil {
		return nil, err
	}
	if result.PlanDigest != digest || len(result.Databases) != 2 || !reflect.DeepEqual(result, expected) {
		return nil, errors.New("prepared result differs from the authorized launch identities")
	}
	for i, db := range plan.Databases {
		observed, err := p.inspect(ctx, plan.OperationID, db)
		if err != nil {
			return nil, err
		}
		if observed != result.Databases[i] {
			return nil, errors.New("prepared database identity changed before launch")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory, err := p.root.Open(".")
	if err != nil {
		return nil, err
	}
	success = true
	return &Prepared{directory: directory, lock: lock, result: result}, nil
}

func (p *Prepared) Close() error { return errors.Join(p.directory.Close(), p.lock.Close()) }

// DatabasePath is meaningful only in a child created through Command.
func (p *Prepared) DatabasePath(database string) (string, error) {
	for _, db := range p.result.Databases {
		if db.Name == database {
			return commandPath(db.Path), nil
		}
	}
	return "", fmt.Errorf("no prepared database %q", database)
}

// Command constructs a foreground native command with the pinned storage fds.
// The caller owns cancellation, supervision and process reaping. It may append
// ExtraFiles but must not replace or reorder the inherited storage descriptors.
func (p *Prepared) Command(binary string, args ...string) *exec.Cmd {
	cmd := exec.Command(binary, args...)
	cmd.ExtraFiles = []*os.File{p.directory, p.lock}
	return cmd
}
