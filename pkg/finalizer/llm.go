package finalizer

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/applier"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

const maxLLMResponseBytes = 64 * 1024

type LLMClient interface {
	CompleteJSON(ctx context.Context, prompt string, maxTokens int) (string, error)
}

type CandidateResolution struct {
	ConflictID   string              `json:"conflict_id"`
	Key          string              `json:"key"`
	DecidedOrder []string            `json:"decided_order"`
	Proposals    []CandidateProposal `json:"proposals"`
	Notes        string              `json:"notes,omitempty"`
}

type CandidateProposal struct {
	OpID      string          `json:"op_id"`
	Action    string          `json:"action"`
	PatchJSON json.RawMessage `json:"patch_json,omitempty"`
	Reason    string          `json:"reason,omitempty"`
}

func (f *Finalizer) tryLLMRepair(ctx context.Context, cert *pb.ConflictCertificate, contenders []*pb.AcceptedRecord, baseState json.RawMessage, dp2 *pb.FinalizeRecord) (*pb.FinalizeRecord, error) {
	if cert == nil || dp2 == nil {
		return nil, fmt.Errorf("nil conflict context")
	}
	if f.llm == nil {
		return nil, fmt.Errorf("llm is not configured")
	}
	failOps := make(map[string]*pb.AcceptedRecord)
	opsByID := make(map[string]*pb.AcceptedRecord, len(contenders))
	for _, op := range contenders {
		if op != nil {
			opsByID[op.OpId] = op
		}
	}
	outcomeByID := make(map[string]*pb.OpOutcome, len(dp2.Outcomes))
	for _, out := range dp2.Outcomes {
		if out == nil {
			continue
		}
		outcomeByID[out.OpId] = out
		if out.Outcome == pb.OutcomeType_OUTCOME_FAIL {
			if op := opsByID[out.OpId]; op != nil {
				failOps[out.OpId] = op
			}
		}
	}
	if len(failOps) == 0 {
		return dp2, nil
	}

	prompt, err := buildLLMPrompt(cert, baseState, contenders, dp2, failOps)
	if err != nil {
		return nil, err
	}

	timeout := f.llmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := f.llm.CompleteJSON(callCtx, prompt, f.llmTokens)
	if err != nil {
		return nil, err
	}
	candidate, err := strictParseCandidate(raw, maxLLMResponseBytes)
	if err != nil {
		return nil, err
	}
	if candidate.ConflictID != cert.ConflictId {
		return nil, fmt.Errorf("candidate conflict_id mismatch")
	}
	if candidate.Key != cert.Key {
		return nil, fmt.Errorf("candidate key mismatch")
	}
	if !equalStringSlice(candidate.DecidedOrder, dp2.DecidedOrder) {
		return nil, fmt.Errorf("candidate decided_order mismatch")
	}

	proposals := make(map[string]CandidateProposal, len(candidate.Proposals))
	for _, p := range candidate.Proposals {
		p.Action = strings.ToUpper(strings.TrimSpace(p.Action))
		if _, ok := failOps[p.OpID]; !ok {
			return nil, fmt.Errorf("proposal op_id=%s is not a failed op", p.OpID)
		}
		switch p.Action {
		case "NOOP":
			if strings.TrimSpace(p.Reason) == "" {
				return nil, fmt.Errorf("noop proposal for op_id=%s missing reason", p.OpID)
			}
		case "PATCH":
			if len(p.PatchJSON) == 0 {
				return nil, fmt.Errorf("patch proposal for op_id=%s missing patch_json", p.OpID)
			}
			if !isJSONObject(p.PatchJSON) {
				return nil, fmt.Errorf("patch proposal for op_id=%s must be a JSON object", p.OpID)
			}
			if !patchWithinWriteSet(p.PatchJSON, failOps[p.OpID].WriteSet) {
				return nil, fmt.Errorf("patch proposal for op_id=%s violates write_set", p.OpID)
			}
		default:
			return nil, fmt.Errorf("unsupported proposal action %q", p.Action)
		}
		proposals[p.OpID] = p
	}

	state := cloneRaw(baseState)
	updated := make([]*pb.OpOutcome, 0, len(dp2.DecidedOrder))
	for _, opID := range dp2.DecidedOrder {
		baseOutcome := outcomeByID[opID]
		if baseOutcome == nil {
			continue
		}
		out := cloneOutcome(baseOutcome)
		if prop, ok := proposals[opID]; ok {
			if out.Outcome != pb.OutcomeType_OUTCOME_FAIL {
				return nil, fmt.Errorf("proposal op_id=%s attempted to modify non-fail outcome", opID)
			}
			switch prop.Action {
			case "NOOP":
				out.Outcome = pb.OutcomeType_OUTCOME_NOOP
				out.Reason = "llm: " + strings.TrimSpace(prop.Reason)
				out.PatchJson = nil
				out.TransformJson = nil
			case "PATCH":
				next, err := applyMergePatch(state, prop.PatchJSON)
				if err != nil {
					return nil, fmt.Errorf("invalid llm patch for op_id=%s: %w", opID, err)
				}
				if !applier.CheckInvariants(cert.Key, next) {
					return nil, fmt.Errorf("llm patch violates invariants for op_id=%s", opID)
				}
				out.Outcome = pb.OutcomeType_OUTCOME_COMMIT_PATCH
				out.Reason = "llm repair"
				out.PatchJson = cloneRaw(prop.PatchJSON)
				out.TransformJson = nil
			}
		}
		if out.Outcome == pb.OutcomeType_OUTCOME_COMMIT_PATCH {
			next, err := applyMergePatch(state, out.PatchJson)
			if err != nil {
				return nil, fmt.Errorf("failed replay patch for op_id=%s: %w", opID, err)
			}
			if !applier.CheckInvariants(cert.Key, next) {
				return nil, fmt.Errorf("replay violates invariants for op_id=%s", opID)
			}
			state = next
		}
		updated = append(updated, out)
	}

	rec := &pb.FinalizeRecord{
		ConflictId:     dp2.ConflictId,
		Key:            dp2.Key,
		DecidedOrder:   append([]string(nil), dp2.DecidedOrder...),
		Outcomes:       updated,
		FinalStateHash: jsonutil.CanonicalHash(state),
		BaseStateHash:  cloneRaw(dp2.BaseStateHash),
		CreatedTsMs:    time.Now().UnixMilli(),
		FinalizeType:   pb.FinalizeType_FINALIZE_REBASE_LLM,
		ProducerRegion: "finalizer",
	}
	rec.VerifierDigest = verifierDigest(rec.BaseStateHash, rec.DecidedOrder, rec.Outcomes)
	return rec, nil
}

func strictParseCandidate(raw string, maxBytes int) (*CandidateResolution, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("empty llm response")
	}
	if maxBytes > 0 && len(raw) > maxBytes {
		return nil, fmt.Errorf("llm response too large: %d", len(raw))
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var c CandidateResolution
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("decode candidate: %w", err)
	}
	if c.ConflictID == "" || c.Key == "" {
		return nil, fmt.Errorf("candidate missing conflict_id or key")
	}
	return &c, nil
}

func buildLLMPrompt(cert *pb.ConflictCertificate, baseState json.RawMessage, contenders []*pb.AcceptedRecord, dp2 *pb.FinalizeRecord, failOps map[string]*pb.AcceptedRecord) (string, error) {
	type failOp struct {
		OpID       string          `json:"op_id"`
		OpType     string          `json:"op_type"`
		ArgsJSON   json.RawMessage `json:"args_json"`
		WriteSet   []string        `json:"write_set"`
		FailReason string          `json:"fail_reason"`
	}
	fails := make([]failOp, 0, len(failOps))
	outcomes := make(map[string]*pb.OpOutcome, len(dp2.Outcomes))
	for _, out := range dp2.Outcomes {
		if out != nil {
			outcomes[out.OpId] = out
		}
	}
	ids := make([]string, 0, len(failOps))
	for opID := range failOps {
		ids = append(ids, opID)
	}
	sort.Strings(ids)
	for _, opID := range ids {
		op := failOps[opID]
		reason := ""
		if out := outcomes[opID]; out != nil {
			reason = out.Reason
		}
		fails = append(fails, failOp{
			OpID:       op.OpId,
			OpType:     op.OpType.String(),
			ArgsJSON:   cloneRaw(op.ArgsJson),
			WriteSet:   append([]string(nil), op.WriteSet...),
			FailReason: reason,
		})
	}

	promptPayload := map[string]any{
		"instructions": []string{
			"Output only valid JSON that matches the required schema.",
			"Do not modify decided_order.",
			"Only propose repairs for failed operations.",
			"Do not modify fields outside each op write_set.",
			"Prefer minimal changes and preserve invariants.",
		},
		"schema": map[string]any{
			"conflict_id":   "string",
			"key":           "string",
			"decided_order": []string{},
			"proposals": []map[string]any{
				{
					"op_id":      "string",
					"action":     "PATCH|NOOP",
					"patch_json": map[string]any{},
					"reason":     "string",
				},
			},
		},
		"conflict_id":       cert.ConflictId,
		"key":               cert.Key,
		"decided_order":     dp2.DecidedOrder,
		"stable_state_json": json.RawMessage(cloneRaw(baseState)),
		"failed_ops":        fails,
	}
	b, err := json.Marshal(promptPayload)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func cloneOutcome(in *pb.OpOutcome) *pb.OpOutcome {
	if in == nil {
		return nil
	}
	out := *in
	out.PatchJson = cloneRaw(in.PatchJson)
	out.TransformJson = cloneRaw(in.TransformJson)
	return &out
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isJSONObject(raw json.RawMessage) bool {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return false
	}
	_, ok := v.(map[string]any)
	return ok
}

func patchWithinWriteSet(patch json.RawMessage, writeSet []string) bool {
	paths := modifiedPathsFromMergePatch(patch)
	for _, path := range paths {
		ok := false
		for _, ws := range writeSet {
			if ws == "*" || pathWithin(ws, path) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func modifiedPathsFromMergePatch(patch json.RawMessage) []string {
	var v any
	if err := json.Unmarshal(patch, &v); err != nil {
		return nil
	}
	var out []string
	var walk func(cur any, prefix string)
	walk = func(cur any, prefix string) {
		m, ok := cur.(map[string]any)
		if !ok {
			if prefix != "" {
				out = append(out, prefix)
			}
			return
		}
		if prefix != "" {
			out = append(out, prefix)
		}
		for k, child := range m {
			next := "/" + k
			if prefix != "" {
				next = prefix + "/" + k
			}
			walk(child, next)
		}
	}
	walk(v, "")
	return out
}

func pathWithin(parent, child string) bool {
	if parent == "*" {
		return true
	}
	p := splitPathSegments(parent)
	c := splitPathSegments(child)
	if len(p) == 0 {
		return false
	}
	if len(p) > len(c) {
		return false
	}
	for i := range p {
		if p[i] != c[i] {
			return false
		}
	}
	return true
}

func splitPathSegments(path string) []string {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return nil
	}
	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}
