package applier

import (
	"encoding/json"
	"testing"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

func TestApplyFinalizeRollbackOnPatchError(t *testing.T) {
	a := &Applier{
		store:            NewStore(),
		pending:          NewPendingWindow(),
		status:           map[string]OpStatus{},
		appliedConflicts: map[string]bool{},
	}

	a.store.Set("k1", json.RawMessage(`{"v":1}`))
	rec := &pb.FinalizeRecord{
		ConflictId:   "c1",
		Key:          "k1",
		DecidedOrder: []string{"op-1"},
		Outcomes: []*pb.OpOutcome{
			{
				OpId:      "op-1",
				Outcome:   pb.OutcomeType_OUTCOME_COMMIT_PATCH,
				PatchJson: []byte(`{`),
			},
		},
	}

	if err := a.ApplyFinalize(rec); err == nil {
		t.Fatalf("expected apply error")
	}

	got, _ := a.store.Get("k1")
	if string(got) != `{"v":1}` {
		t.Fatalf("store mutated on failed apply: %s", string(got))
	}
	if _, ok := a.status["op-1"]; ok {
		t.Fatalf("status should not be finalized on failed apply")
	}
	if a.appliedConflicts["c1"] {
		t.Fatalf("conflict should not be marked applied on failed apply")
	}
}

func TestApplyFinalizeSuccess(t *testing.T) {
	a := &Applier{
		store:            NewStore(),
		pending:          NewPendingWindow(),
		status:           map[string]OpStatus{},
		appliedConflicts: map[string]bool{},
	}

	a.store.Set("k1", json.RawMessage(`{"v":1}`))
	rec := &pb.FinalizeRecord{
		ConflictId:   "c2",
		Key:          "k1",
		DecidedOrder: []string{"op-2"},
		Outcomes: []*pb.OpOutcome{
			{
				OpId:      "op-2",
				Outcome:   pb.OutcomeType_OUTCOME_COMMIT_PATCH,
				PatchJson: []byte(`{"v":2}`),
			},
		},
	}

	if err := a.ApplyFinalize(rec); err != nil {
		t.Fatalf("apply finalize: %v", err)
	}

	got, _ := a.store.Get("k1")
	if string(got) != `{"v":2}` {
		t.Fatalf("unexpected store value: %s", string(got))
	}
	st, ok := a.status["op-2"]
	if !ok {
		t.Fatalf("missing status for op-2")
	}
	if st.Status != "FINALIZED" || st.Outcome != "COMMIT_PATCH" {
		t.Fatalf("unexpected status: %+v", st)
	}
	if !a.appliedConflicts["c2"] {
		t.Fatalf("conflict should be marked applied")
	}
}
