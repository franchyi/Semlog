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

const (
	RebaseModeRebase = "rebase"
	RebaseModeLWW    = "lww"
)

func NormalizeRebaseMode(mode string) string {
	switch mode {
	case RebaseModeLWW:
		return RebaseModeLWW
	default:
		return RebaseModeRebase
	}
}

func Rebase(cert *pb.ConflictCertificate, contenders []*pb.AcceptedRecord, baseState json.RawMessage) (*pb.FinalizeRecord, error) {
	return RebaseWithMode(RebaseModeRebase, cert, contenders, baseState)
}

func RebaseWithMode(mode string, cert *pb.ConflictCertificate, contenders []*pb.AcceptedRecord, baseState json.RawMessage) (*pb.FinalizeRecord, error) {
	if NormalizeRebaseMode(mode) == RebaseModeLWW {
		return rebaseLWW(cert, contenders, baseState)
	}
	return rebaseFull(cert, contenders, baseState)
}

func rebaseFull(cert *pb.ConflictCertificate, contenders []*pb.AcceptedRecord, baseState json.RawMessage) (*pb.FinalizeRecord, error) {
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
	return buildFinalizeRecord(cert, ids, outcomes, baseHash, finalHash)
}

func rebaseLWW(cert *pb.ConflictCertificate, contenders []*pb.AcceptedRecord, baseState json.RawMessage) (*pb.FinalizeRecord, error) {
	if cert == nil {
		return nil, fmt.Errorf("nil certificate")
	}
	ordered := append([]*pb.AcceptedRecord(nil), contenders...)
	sort.Slice(ordered, func(i, j int) bool { return CompareAccepted(ordered[i], ordered[j]) < 0 })
	if len(ordered) == 0 {
		return nil, fmt.Errorf("no contenders")
	}

	winner := ordered[len(ordered)-1]
	ids := make([]string, 0, len(ordered))
	outcomes := make([]*pb.OpOutcome, 0, len(ordered))

	nextState, winnerOutcome := engine.Apply(winner, baseState)
	commitWinner := winnerOutcome.Type == pb.OutcomeType_OUTCOME_COMMIT_PATCH && applier.CheckInvariants(cert.Key, nextState)
	if winnerOutcome.Type == pb.OutcomeType_OUTCOME_COMMIT_PATCH && !commitWinner {
		winnerOutcome = engine.Outcome{Type: pb.OutcomeType_OUTCOME_FAIL, Reason: "invariant violation"}
	}

	for _, op := range ordered {
		ids = append(ids, op.OpId)
		if op.OpId != winner.OpId {
			outcomes = append(outcomes, &pb.OpOutcome{
				OpId:    op.OpId,
				Outcome: pb.OutcomeType_OUTCOME_NOOP,
				Reason:  "lww: not the latest writer",
			})
			continue
		}
		switch winnerOutcome.Type {
		case pb.OutcomeType_OUTCOME_COMMIT_PATCH:
			outcomes = append(outcomes, &pb.OpOutcome{
				OpId:      op.OpId,
				Outcome:   pb.OutcomeType_OUTCOME_COMMIT_PATCH,
				PatchJson: engine.Diff(baseState, nextState),
			})
		case pb.OutcomeType_OUTCOME_NOOP:
			outcomes = append(outcomes, &pb.OpOutcome{
				OpId:    op.OpId,
				Outcome: pb.OutcomeType_OUTCOME_NOOP,
				Reason:  winnerOutcome.Reason,
			})
		default:
			outcomes = append(outcomes, &pb.OpOutcome{
				OpId:    op.OpId,
				Outcome: pb.OutcomeType_OUTCOME_FAIL,
				Reason:  winnerOutcome.Reason,
			})
		}
	}

	baseHash := jsonutil.CanonicalHash(baseState)
	finalState := cloneRaw(baseState)
	if commitWinner {
		finalState = nextState
	}
	finalHash := jsonutil.CanonicalHash(finalState)
	return buildFinalizeRecord(cert, ids, outcomes, baseHash, finalHash)
}

func buildFinalizeRecord(cert *pb.ConflictCertificate, ids []string, outcomes []*pb.OpOutcome, baseHash, finalHash []byte) (*pb.FinalizeRecord, error) {
	if cert.ConflictId == "" {
		cert.ConflictId = applier.ConflictID(cert.Key, ids, baseHash)
	}
	verifier := verifierDigest(baseHash, ids, outcomes)

	rec := &pb.FinalizeRecord{
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
	}
	return rec, nil
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
