package engine

import (
	"encoding/json"
	"testing"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

func TestApplyPatchAndInc(t *testing.T) {
	state := json.RawMessage(`{"stats":{"views":10},"email":"old"}`)

	patchArgs := json.RawMessage(`{"patches":[{"path":"/email","value":"new"}]}`)
	opPatch := &pb.AcceptedRecord{OpType: pb.OpType_OP_PATCH, ArgsJson: patchArgs}
	next, out := Apply(opPatch, state)
	if out.Type != pb.OutcomeType_OUTCOME_COMMIT_PATCH {
		t.Fatalf("patch failed: %+v", out)
	}

	incArgs := json.RawMessage(`{"path":"/stats/views","delta":5}`)
	opInc := &pb.AcceptedRecord{OpType: pb.OpType_OP_INC, ArgsJson: incArgs}
	next2, out2 := Apply(opInc, next)
	if out2.Type != pb.OutcomeType_OUTCOME_COMMIT_PATCH {
		t.Fatalf("inc failed: %+v", out2)
	}
	if string(next2) != `{"email":"new","stats":{"views":15}}` && string(next2) != `{"stats":{"views":15},"email":"new"}` {
		t.Fatalf("unexpected state: %s", string(next2))
	}
}

func TestPutIfAbsentNoop(t *testing.T) {
	op := &pb.AcceptedRecord{OpType: pb.OpType_OP_PUT_IF_ABSENT, ArgsJson: json.RawMessage(`{"value":{"x":1}}`)}
	_, out := Apply(op, json.RawMessage(`{"x":2}`))
	if out.Type != pb.OutcomeType_OUTCOME_NOOP {
		t.Fatalf("expected noop, got %+v", out)
	}
}

func TestReserve(t *testing.T) {
	op := &pb.AcceptedRecord{OpType: pb.OpType_OP_RESERVE, ArgsJson: json.RawMessage(`{"n":3}`)}
	next, out := Apply(op, json.RawMessage(`{"stock":10,"reserved":4}`))
	if out.Type != pb.OutcomeType_OUTCOME_COMMIT_PATCH {
		t.Fatalf("reserve failed: %+v", out)
	}
	if string(next) != `{"reserved":7,"stock":10}` && string(next) != `{"stock":10,"reserved":7}` {
		t.Fatalf("unexpected state: %s", next)
	}
}
