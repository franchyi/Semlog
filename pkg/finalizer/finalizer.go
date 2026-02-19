package finalizer

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

type Finalizer struct {
	brokers  []string
	producer *kafka.Producer

	stableMu sync.RWMutex
	stable   map[string]json.RawMessage

	seenMu sync.Mutex
	seen   map[string]bool
}

func New(brokers []string, producer *kafka.Producer) *Finalizer {
	return &Finalizer{
		brokers:  brokers,
		producer: producer,
		stable:   map[string]json.RawMessage{},
		seen:     map[string]bool{},
	}
}

func (f *Finalizer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		f.consumeFinalSnapshot(ctx)
	}()
	go func() {
		defer wg.Done()
		f.consumeCerts(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (f *Finalizer) consumeFinalSnapshot(ctx context.Context) {
	c := kafka.NewConsumer(f.brokers, "arb.final", "finalizer-arb-final")
	defer c.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := c.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("finalizer snapshot read error: %v", err)
			continue
		}
		var rec pb.FinalizeRecord
		if err := pb.UnmarshalJSON(msg.Value, &rec); err == nil {
			f.applyFinalize(&rec)
		}
		_ = c.Commit(ctx, msg)
	}
}

func (f *Finalizer) consumeCerts(ctx context.Context) {
	c := kafka.NewConsumer(f.brokers, "arb.cert", "finalizer")
	defer c.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := c.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("cert read error: %v", err)
			continue
		}

		var cert pb.ConflictCertificate
		if err := pb.UnmarshalJSON(msg.Value, &cert); err != nil {
			log.Printf("invalid cert: %v", err)
			_ = c.Commit(ctx, msg)
			continue
		}

		if f.isSeen(cert.ConflictId) {
			_ = c.Commit(ctx, msg)
			continue
		}

		contenders := make([]*pb.AcceptedRecord, 0, len(cert.Contenders))
		for _, ref := range cert.Contenders {
			recMsg, err := kafka.FetchRecord(f.brokers, ref.Topic, int(ref.Partition), ref.Offset)
			if err != nil {
				log.Printf("fetch contender failed conflict=%s op=%s: %v", cert.ConflictId, ref.OpId, err)
				continue
			}
			var op pb.AcceptedRecord
			if err := pb.UnmarshalJSON(recMsg.Value, &op); err != nil {
				log.Printf("decode contender failed: %v", err)
				continue
			}
			contenders = append(contenders, &op)
		}
		if len(contenders) == 0 {
			_ = c.Commit(ctx, msg)
			continue
		}

		base := f.getStable(cert.Key)
		finalRec, err := Rebase(&cert, contenders, base)
		if err != nil {
			log.Printf("rebase failed conflict=%s: %v", cert.ConflictId, err)
			_ = c.Commit(ctx, msg)
			continue
		}
		b, _ := pb.MarshalJSON(finalRec)
		if _, _, err := f.producer.Write(ctx, "arb.final", cert.Key, b); err != nil {
			log.Printf("produce finalize failed conflict=%s: %v", cert.ConflictId, err)
			_ = c.Commit(ctx, msg)
			continue
		}
		f.markSeen(cert.ConflictId)
		_ = c.Commit(ctx, msg)
	}
}

func (f *Finalizer) getStable(key string) json.RawMessage {
	f.stableMu.RLock()
	defer f.stableMu.RUnlock()
	v := f.stable[key]
	if len(v) == 0 {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func (f *Finalizer) applyFinalize(rec *pb.FinalizeRecord) {
	f.stableMu.Lock()
	defer f.stableMu.Unlock()
	state := cloneRaw(f.stable[rec.Key])
	for _, opID := range rec.DecidedOrder {
		var out *pb.OpOutcome
		for _, candidate := range rec.Outcomes {
			if candidate.OpId == opID {
				out = candidate
				break
			}
		}
		if out == nil {
			continue
		}
		if out.Outcome == pb.OutcomeType_OUTCOME_COMMIT_PATCH {
			next, err := applyMergePatch(state, out.PatchJson)
			if err == nil {
				state = next
			}
		}
		if out.Outcome == pb.OutcomeType_OUTCOME_TRANSFORM {
			next, err := applyMergePatch(state, out.TransformJson)
			if err == nil {
				state = next
			}
		}
	}
	f.stable[rec.Key] = state
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

func (f *Finalizer) isSeen(conflictID string) bool {
	if conflictID == "" {
		return false
	}
	f.seenMu.Lock()
	defer f.seenMu.Unlock()
	return f.seen[conflictID]
}

func (f *Finalizer) markSeen(conflictID string) {
	if conflictID == "" {
		return
	}
	f.seenMu.Lock()
	defer f.seenMu.Unlock()
	f.seen[conflictID] = true
}
