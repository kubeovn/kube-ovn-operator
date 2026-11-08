package recovery

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// IsolationProof is produced after the retained OVN processes have been
// stopped or fenced. It is mounted through an immutable ConfigMap and is bound
// to the exact canonical plan; a generic "quiesced" flag is insufficient.
type IsolationProof struct {
	PlanDigest      string    `json:"planDigest"`
	OperationID     string    `json:"operationID"`
	RetainedMembers []string  `json:"retainedMembers"`
	VerifiedAt      time.Time `json:"verifiedAt"`
}

func (p IsolationProof) Validate(plan Plan) error {
	_, digest, err := plan.Canonical()
	if err != nil {
		return err
	}
	if p.PlanDigest != digest || p.OperationID != plan.OperationID {
		return errors.New("isolation proof is not bound to the recovery plan")
	}
	if p.VerifiedAt.IsZero() || p.VerifiedAt.After(time.Now().UTC().Add(time.Minute)) {
		return errors.New("isolation proof timestamp is invalid")
	}
	expected := make([]string, 0, len(plan.RetainedMembers))
	for _, member := range plan.RetainedMembers {
		expected = append(expected, member.Name)
	}
	actual := slices.Clone(p.RetainedMembers)
	slices.Sort(actual)
	if !slices.Equal(actual, expected) || len(actual) == 0 || strings.TrimSpace(actual[0]) == "" {
		return fmt.Errorf("isolation proof does not cover all retained members")
	}
	return nil
}
