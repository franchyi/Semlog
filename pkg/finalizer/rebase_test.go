package finalizer

import (
	"encoding/json"
	"testing"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

func TestRebaseTwoOps(t *testing.T) {
	cert := &pb.ConflictCertificate{ConflictId: "c1", Key: "user:1"}
	ops := []*pb.AcceptedRecord{
		{OpId: "b", Region: "B", OpType: pb.OpType_OP_PATCH, ArgsJson: json.RawMessage(`{"patches":[{"path":"/phone","value":"123"}]}`), Hlc: &pb.HLC{PhysicalMs: 2, Logical: 0}},
		{OpId: "a", Region: "A", OpType: pb.OpType_OP_PATCH, ArgsJson: json.RawMessage(`{"patches":[{"path":"/email","value":"x@y.com"}]}`), Hlc: &pb.HLC{PhysicalMs: 1, Logical: 0}},
	}
	base := json.RawMessage(`{"email":"old","phone":"000"}`)
	rec, err := Rebase(cert, ops, base)
	if err != nil {
		t.Fatal(err)
	}
	if rec.FinalizeType != pb.FinalizeType_FINALIZE_REBASE {
		t.Fatal("wrong finalize type")
	}
	if len(rec.Outcomes) != 2 {
		t.Fatalf("expected 2 outcomes")
	}
}

func TestRebaseLWW(t *testing.T) {
	cert := &pb.ConflictCertificate{ConflictId: "c2", Key: "user:1"}
	ops := []*pb.AcceptedRecord{
		{
			OpId:   "a",
			Region: "A",
			OpType: pb.OpType_OP_PATCH,
			ArgsJson: json.RawMessage(
				`{"patches":[{"path":"/email","value":"a@example.com"}]}`,
			),
			Hlc: &pb.HLC{PhysicalMs: 1, Logical: 0},
		},
		{
			OpId:   "b",
			Region: "B",
			OpType: pb.OpType_OP_PATCH,
			ArgsJson: json.RawMessage(
				`{"patches":[{"path":"/email","value":"b@example.com"}]}`,
			),
			Hlc: &pb.HLC{PhysicalMs: 2, Logical: 0},
		},
	}
	base := json.RawMessage(`{"email":"old@example.com"}`)

	rec, err := RebaseWithMode(RebaseModeLWW, cert, ops, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Outcomes) != 2 {
		t.Fatalf("expected 2 outcomes, got %d", len(rec.Outcomes))
	}
	commitCount := 0
	for _, out := range rec.Outcomes {
		if out.Outcome == pb.OutcomeType_OUTCOME_COMMIT_PATCH {
			commitCount++
			if out.OpId != "b" {
				t.Fatalf("expected latest writer b to commit, got %s", out.OpId)
			}
		}
	}
	if commitCount != 1 {
		t.Fatalf("expected exactly one commit winner, got %d", commitCount)
	}
}
