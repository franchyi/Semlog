package applier

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/engine"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

func ComputeMerge(ops []*pb.AcceptedRecord, stableState json.RawMessage, key string, producerRegion string) (*pb.FinalizeRecord, error) {
	if len(ops) == 0 {
		return nil, fmt.Errorf("no ops")
	}
	ordered := append([]*pb.AcceptedRecord(nil), ops...)
	sort.Slice(ordered, func(i, j int) bool { return compareAccepted(ordered[i], ordered[j]) < 0 })

	state := cloneRaw(stableState)
	outcomes := make([]*pb.OpOutcome, 0, len(ordered))
	order := make([]string, 0, len(ordered))
	ids := make([]string, 0, len(ordered))

	for _, op := range ordered {
		before := cloneRaw(state)
		next, outcome := engine.Apply(op, state)
		switch outcome.Type {
		case pb.OutcomeType_OUTCOME_COMMIT_PATCH:
			patch := engine.Diff(before, next)
			state = next
			outcomes = append(outcomes, &pb.OpOutcome{
				OpId:      op.OpId,
				Outcome:   pb.OutcomeType_OUTCOME_COMMIT_PATCH,
				PatchJson: patch,
			})
		case pb.OutcomeType_OUTCOME_NOOP:
			outcomes = append(outcomes, &pb.OpOutcome{OpId: op.OpId, Outcome: pb.OutcomeType_OUTCOME_NOOP, Reason: outcome.Reason})
		default:
			outcomes = append(outcomes, &pb.OpOutcome{OpId: op.OpId, Outcome: pb.OutcomeType_OUTCOME_FAIL, Reason: outcome.Reason})
		}
		order = append(order, op.OpId)
		ids = append(ids, op.OpId)
	}

	if !CheckInvariants(key, state) {
		return nil, fmt.Errorf("invariants failed")
	}

	baseHash := jsonutil.CanonicalHash(stableState)
	finalHash := jsonutil.CanonicalHash(state)
	conflictID := ConflictID(key, ids, baseHash)
	verifier := verifierDigest(baseHash, order, outcomes)

	return &pb.FinalizeRecord{
		ConflictId:     conflictID,
		Key:            key,
		DecidedOrder:   order,
		Outcomes:       outcomes,
		FinalStateHash: finalHash,
		VerifierDigest: verifier,
		BaseStateHash:  baseHash,
		CreatedTsMs:    time.Now().UnixMilli(),
		FinalizeType:   pb.FinalizeType_FINALIZE_MERGE,
		ProducerRegion: producerRegion,
	}, nil
}

func compareAccepted(a, b *pb.AcceptedRecord) int {
	if a.Hlc == nil && b.Hlc == nil {
		if a.Region < b.Region {
			return -1
		}
		if a.Region > b.Region {
			return 1
		}
		if a.OpId < b.OpId {
			return -1
		}
		if a.OpId > b.OpId {
			return 1
		}
		return 0
	}
	if a.Hlc == nil {
		return -1
	}
	if b.Hlc == nil {
		return 1
	}
	if a.Hlc.PhysicalMs < b.Hlc.PhysicalMs {
		return -1
	}
	if a.Hlc.PhysicalMs > b.Hlc.PhysicalMs {
		return 1
	}
	if a.Hlc.Logical < b.Hlc.Logical {
		return -1
	}
	if a.Hlc.Logical > b.Hlc.Logical {
		return 1
	}
	if a.Region < b.Region {
		return -1
	}
	if a.Region > b.Region {
		return 1
	}
	if a.OpId < b.OpId {
		return -1
	}
	if a.OpId > b.OpId {
		return 1
	}
	return 0
}

func verifierDigest(baseHash []byte, decidedOrder []string, outcomes []*pb.OpOutcome) []byte {
	payload, _ := json.Marshal(struct {
		Base     []byte          `json:"base"`
		Order    []string        `json:"order"`
		Outcomes []*pb.OpOutcome `json:"outcomes"`
	}{Base: baseHash, Order: decidedOrder, Outcomes: outcomes})
	s := sha256.Sum256(payload)
	return s[:]
}

func cloneRaw(in json.RawMessage) json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
