package applier

import (
	"encoding/json"
	"errors"
	"log"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
)

func (a *Applier) ApplyFinalize(rec *pb.FinalizeRecord) error {
	if rec == nil {
		return errors.New("nil finalize record")
	}
	if rec.ConflictId == "" {
		return errors.New("missing conflict_id")
	}

	a.appliedMu.Lock()
	if a.appliedConflicts[rec.ConflictId] {
		a.appliedMu.Unlock()
		return nil
	}
	a.appliedMu.Unlock()

	state, _ := a.store.Get(rec.Key)
	curHash := jsonutil.CanonicalHash(state)
	if len(rec.BaseStateHash) > 0 && !bytesEq(curHash, rec.BaseStateHash) {
		log.Printf("warning: base hash mismatch key=%s conflict=%s", rec.Key, rec.ConflictId)
	}

	outcomeByID := make(map[string]*pb.OpOutcome, len(rec.Outcomes))
	for _, out := range rec.Outcomes {
		outcomeByID[out.OpId] = out
	}

	ordered := rec.DecidedOrder
	if len(ordered) == 0 {
		for _, out := range rec.Outcomes {
			ordered = append(ordered, out.OpId)
		}
	}

	for _, opID := range ordered {
		out := outcomeByID[opID]
		if out == nil {
			continue
		}
		switch out.Outcome {
		case pb.OutcomeType_OUTCOME_COMMIT_PATCH:
			if len(out.PatchJson) == 0 {
				continue
			}
			next, err := applyMergePatch(state, out.PatchJson)
			if err != nil {
				return err
			}
			state = next
		case pb.OutcomeType_OUTCOME_TRANSFORM:
			if len(out.TransformJson) == 0 {
				continue
			}
			next, err := applyMergePatch(state, out.TransformJson)
			if err != nil {
				return err
			}
			state = next
		case pb.OutcomeType_OUTCOME_NOOP, pb.OutcomeType_OUTCOME_FAIL:
			// no state change
		}

		a.setFinalized(opID, out)
	}

	a.store.Set(rec.Key, state)
	if len(rec.FinalStateHash) > 0 {
		newHash := jsonutil.CanonicalHash(state)
		if !bytesEq(newHash, rec.FinalStateHash) {
			log.Printf("warning: final hash mismatch key=%s conflict=%s", rec.Key, rec.ConflictId)
		}
	}

	a.pending.RemoveOps(rec.Key, ordered)
	a.appliedMu.Lock()
	a.appliedConflicts[rec.ConflictId] = true
	a.appliedMu.Unlock()
	return nil
}

func bytesEq(a, b []byte) bool {
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

func applyMergePatch(doc, patch json.RawMessage) (json.RawMessage, error) {
	if len(patch) == 0 {
		return cloneRaw(doc), nil
	}
	var patchAny any
	if err := json.Unmarshal(patch, &patchAny); err != nil {
		return nil, err
	}
	if _, ok := patchAny.(map[string]any); !ok {
		return cloneRaw(patch), nil
	}

	var docAny any
	if len(doc) == 0 {
		docAny = map[string]any{}
	} else if err := json.Unmarshal(doc, &docAny); err != nil {
		return nil, err
	}
	merged := mergeAny(docAny, patchAny)
	out, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func mergeAny(doc, patch any) any {
	pObj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	dObj, ok := doc.(map[string]any)
	if !ok {
		dObj = map[string]any{}
	}
	for k, pv := range pObj {
		if pv == nil {
			delete(dObj, k)
			continue
		}
		dObj[k] = mergeAny(dObj[k], pv)
	}
	return dObj
}
