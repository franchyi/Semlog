package finalizer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
)

type mockLLMClient struct {
	resp string
	err  error
}

func (m mockLLMClient) CompleteJSON(_ context.Context, _ string, _ int) (string, error) {
	return m.resp, m.err
}

func TestTryLLMRepairFailToNoop(t *testing.T) {
	cert := &pb.ConflictCertificate{
		ConflictId: "conf-1",
		Key:        "user:1",
	}
	base := json.RawMessage(`{"ver":1}`)
	op1 := &pb.AcceptedRecord{
		OpId:     "op-1",
		Region:   "A",
		Key:      "user:1",
		OpType:   pb.OpType_OP_PATCH,
		ArgsJson: json.RawMessage(`{"patches":[{"path":"/email","value":"alice@example.com"}]}`),
		WriteSet: []string{"/email"},
		Hlc:      &pb.HLC{PhysicalMs: 1, Logical: 0},
	}
	op2 := &pb.AcceptedRecord{
		OpId:     "op-2",
		Region:   "B",
		Key:      "user:1",
		OpType:   pb.OpType_OP_CAS,
		ArgsJson: json.RawMessage(`{"path":"/ver","expected":2,"new_value":3}`),
		WriteSet: []string{"/ver"},
		Hlc:      &pb.HLC{PhysicalMs: 2, Logical: 0},
	}
	dp2, err := RebaseWithMode(RebaseModeRebase, cert, []*pb.AcceptedRecord{op1, op2}, base)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	if got := outcomeByID(dp2.Outcomes, "op-2").Outcome; got != pb.OutcomeType_OUTCOME_FAIL {
		t.Fatalf("expected dp2 fail for op-2, got %v", got)
	}

	f := &Finalizer{
		llm:        mockLLMClient{resp: `{"conflict_id":"conf-1","key":"user:1","decided_order":["op-1","op-2"],"proposals":[{"op_id":"op-2","action":"NOOP","reason":"CAS mismatch"}]}`},
		llmTimeout: 2 * time.Second,
		llmTokens:  256,
	}
	out, err := f.tryLLMRepair(context.Background(), cert, []*pb.AcceptedRecord{op1, op2}, base, dp2)
	if err != nil {
		t.Fatalf("llm repair: %v", err)
	}
	op2Out := outcomeByID(out.Outcomes, "op-2")
	if op2Out == nil || op2Out.Outcome != pb.OutcomeType_OUTCOME_NOOP {
		t.Fatalf("expected op-2 noop, got %#v", op2Out)
	}
	if out.FinalizeType != pb.FinalizeType_FINALIZE_REBASE_LLM {
		t.Fatalf("expected llm finalize type, got %v", out.FinalizeType)
	}
}

func TestTryLLMRepairRejectOutOfWriteSetPatch(t *testing.T) {
	cert := &pb.ConflictCertificate{
		ConflictId: "conf-2",
		Key:        "user:1",
	}
	base := json.RawMessage(`{"ver":1}`)
	op1 := &pb.AcceptedRecord{
		OpId:     "op-1",
		Region:   "A",
		Key:      "user:1",
		OpType:   pb.OpType_OP_PATCH,
		ArgsJson: json.RawMessage(`{"patches":[{"path":"/email","value":"alice@example.com"}]}`),
		WriteSet: []string{"/email"},
		Hlc:      &pb.HLC{PhysicalMs: 1, Logical: 0},
	}
	op2 := &pb.AcceptedRecord{
		OpId:     "op-2",
		Region:   "B",
		Key:      "user:1",
		OpType:   pb.OpType_OP_CAS,
		ArgsJson: json.RawMessage(`{"path":"/ver","expected":2,"new_value":3}`),
		WriteSet: []string{"/ver"},
		Hlc:      &pb.HLC{PhysicalMs: 2, Logical: 0},
	}
	dp2, err := RebaseWithMode(RebaseModeRebase, cert, []*pb.AcceptedRecord{op1, op2}, base)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}

	f := &Finalizer{
		llm:        mockLLMClient{resp: `{"conflict_id":"conf-2","key":"user:1","decided_order":["op-1","op-2"],"proposals":[{"op_id":"op-2","action":"PATCH","patch_json":{"email":"bad@x"},"reason":"oops"}]}`},
		llmTimeout: 2 * time.Second,
		llmTokens:  256,
	}
	if _, err := f.tryLLMRepair(context.Background(), cert, []*pb.AcceptedRecord{op1, op2}, base, dp2); err == nil {
		t.Fatalf("expected write_set validation error")
	}
}

func outcomeByID(outcomes []*pb.OpOutcome, opID string) *pb.OpOutcome {
	for _, out := range outcomes {
		if out.OpId == opID {
			return out
		}
	}
	return nil
}
