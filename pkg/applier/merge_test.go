package applier

import (
	"encoding/json"
	"testing"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

func TestComputeMergeDisjointPatch(t *testing.T) {
	opA := &pb.AcceptedRecord{
		OpId:     "a",
		Region:   "A",
		Key:      "user:1",
		OpType:   pb.OpType_OP_PATCH,
		WriteSet: []string{"/email"},
		ArgsJson: json.RawMessage(`{"patches":[{"path":"/email","value":"a@x.com"}]}`),
		Hlc:      &pb.HLC{PhysicalMs: 1, Logical: 0},
	}
	opB := &pb.AcceptedRecord{
		OpId:     "b",
		Region:   "B",
		Key:      "user:1",
		OpType:   pb.OpType_OP_PATCH,
		WriteSet: []string{"/phone"},
		ArgsJson: json.RawMessage(`{"patches":[{"path":"/phone","value":"555"}]}`),
		Hlc:      &pb.HLC{PhysicalMs: 2, Logical: 0},
	}
	base := json.RawMessage(`{"email":"old","phone":"111"}`)

	rec, err := ComputeMerge([]*pb.AcceptedRecord{opB, opA}, base, "user:1", "A")
	if err != nil {
		t.Fatal(err)
	}
	if rec.FinalizeType != pb.FinalizeType_FINALIZE_MERGE {
		t.Fatalf("wrong finalize type")
	}
	if len(rec.Outcomes) != 2 {
		t.Fatalf("expected 2 outcomes")
	}
}
