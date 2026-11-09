package recovery

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// IsolationProof records plan-bound isolation audit metadata. Validation
// checks its binding and freshness; it does not establish enforced isolation
// and must never replace the live Isolation capability.
type IsolationProof struct {
	PlanDigest      string    `json:"planDigest"`
	OperationID     string    `json:"operationID"`
	RetainedMembers []string  `json:"retainedMembers"`
	VerifiedAt      time.Time `json:"verifiedAt"`
}

const isolationProofMaxAge = 5 * time.Minute

func (p IsolationProof) Validate(plan Plan) error {
	return p.validateAt(plan, time.Now().UTC())
}

func (p IsolationProof) validateAt(plan Plan, now time.Time) error {
	canonical, digest, err := plan.Canonical()
	if err != nil {
		return err
	}
	if p.PlanDigest != digest || p.OperationID != plan.OperationID {
		return errors.New("isolation proof is not bound to the recovery plan")
	}
	if p.VerifiedAt.IsZero() || p.VerifiedAt.After(now.Add(time.Minute)) || p.VerifiedAt.Before(now.Add(-isolationProofMaxAge)) {
		return errors.New("isolation proof timestamp is invalid or expired")
	}
	expected := make([]string, 0, len(canonical.RetainedMembers))
	for _, member := range canonical.RetainedMembers {
		expected = append(expected, member.Name)
	}
	actual := slices.Clone(p.RetainedMembers)
	slices.Sort(actual)
	if !slices.Equal(actual, expected) || len(actual) == 0 || strings.TrimSpace(actual[0]) == "" {
		return fmt.Errorf("isolation proof does not cover all retained members")
	}
	return nil
}
