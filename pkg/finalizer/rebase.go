package finalizer

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/applier"
	"github.com/yourorg/redpanda-mm/pkg/engine"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

func Rebase(cert *pb.ConflictCertificate, contenders []*pb.AcceptedRecord, baseState json.RawMessage) (*pb.FinalizeRecord, error) {
	if cert == nil {
		return nil, fmt.Errorf("nil certificate")
	}
	ordered := append([]*pb.AcceptedRecord(nil), contenders...)
	sort.Slice(ordered, func(i, j int) bool { return CompareAccepted(ordered[i], ordered[j]) < 0 })

	state := cloneRaw(baseState)
	outcomes := make([]*pb.OpOutcome, 0, len(ordered))
	ids := make([]string, 0, len(ordered))

	for _, op := range ordered {
		before := cloneRaw(state)
		next, out := engine.Apply(op, state)
		switch out.Type {
		case pb.OutcomeType_OUTCOME_COMMIT_PATCH:
			if !applier.CheckInvariants(cert.Key, next) {
				outcomes = append(outcomes, &pb.OpOutcome{OpId: op.OpId, Outcome: pb.OutcomeType_OUTCOME_FAIL, Reason: "invariant violation"})
				ids = append(ids, op.OpId)
				continue
			}
			patch := engine.Diff(before, next)
			outcomes = append(outcomes, &pb.OpOutcome{OpId: op.OpId, Outcome: pb.OutcomeType_OUTCOME_COMMIT_PATCH, PatchJson: patch})
			state = next
		case pb.OutcomeType_OUTCOME_NOOP:
			outcomes = append(outcomes, &pb.OpOutcome{OpId: op.OpId, Outcome: pb.OutcomeType_OUTCOME_NOOP, Reason: out.Reason})
		default:
			outcomes = append(outcomes, &pb.OpOutcome{OpId: op.OpId, Outcome: pb.OutcomeType_OUTCOME_FAIL, Reason: out.Reason})
		}
		ids = append(ids, op.OpId)
	}

	baseHash := jsonutil.CanonicalHash(baseState)
	finalHash := jsonutil.CanonicalHash(state)
	if cert.ConflictId == "" {
		cert.ConflictId = applier.ConflictID(cert.Key, ids, baseHash)
	}
	verifier := verifierDigest(baseHash, ids, outcomes)

	return &pb.FinalizeRecord{
		ConflictId:     cert.ConflictId,
		Key:            cert.Key,
		DecidedOrder:   ids,
		Outcomes:       outcomes,
		FinalStateHash: finalHash,
		VerifierDigest: verifier,
		BaseStateHash:  baseHash,
		CreatedTsMs:    time.Now().UnixMilli(),
		FinalizeType:   pb.FinalizeType_FINALIZE_REBASE,
		ProducerRegion: "finalizer",
	}, nil
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
