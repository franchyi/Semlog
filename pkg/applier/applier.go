package applier

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kgo "github.com/segmentio/kafka-go"
	pb "github.com/yourorg/redpanda-mm/gen/proto"
	"github.com/yourorg/redpanda-mm/pkg/jsonutil"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

type KeyLocks struct {
	locks sync.Map
}

func (kl *KeyLocks) Lock(key string) {
	mu, _ := kl.locks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
}

func (kl *KeyLocks) Unlock(key string) {
	mu, _ := kl.locks.Load(key)
	mu.(*sync.Mutex).Unlock()
}

type Applier struct {
	region           string
	classifyMode     string
	brokers          []string
	producer         *kafka.Producer
	store            *Store
	pending          *PendingWindow
	locks            KeyLocks
	statusMu         sync.RWMutex
	status           map[string]OpStatus
	appliedMu        sync.Mutex
	appliedConflicts map[string]bool

	certEmitted            atomic.Int64
	mergeFinalizeEmitted   atomic.Int64
	finalRecordsConsumed   atomic.Int64
	finalMergeConsumed     atomic.Int64
	finalRebaseConsumed    atomic.Int64
	finalRebaseLLMConsumed atomic.Int64
	outcomeCommitPatch     atomic.Int64
	outcomeNoop            atomic.Int64
	outcomeFail            atomic.Int64
	outcomeTransform       atomic.Int64
	finalApplyErrors       atomic.Int64
	finalizeDuplicateSkips atomic.Int64
	baseHashMismatches     atomic.Int64
	finalHashMismatches    atomic.Int64
}

func New(region string, brokers []string, producer *kafka.Producer, classifyMode string) (*Applier, error) {
	region = strings.ToUpper(strings.TrimSpace(region))
	if region != "A" && region != "B" {
		return nil, fmt.Errorf("region must be A or B")
	}
	return &Applier{
		region:           region,
		classifyMode:     NormalizeClassifyMode(classifyMode),
		brokers:          brokers,
		producer:         producer,
		store:            NewStore(),
		pending:          NewPendingWindow(),
		status:           map[string]OpStatus{},
		appliedConflicts: map[string]bool{},
	}, nil
}

func (a *Applier) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		a.consumeIngest(ctx, "ingest.A", fmt.Sprintf("applier-%s-ingest-A", a.region))
	}()
	go func() {
		defer wg.Done()
		a.consumeIngest(ctx, "ingest.B", fmt.Sprintf("applier-%s-ingest-B", a.region))
	}()
	go func() {
		defer wg.Done()
		a.consumeFinal(ctx, "arb.final", fmt.Sprintf("applier-%s-arb-final", a.region))
	}()
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (a *Applier) consumeIngest(ctx context.Context, topic, group string) {
	c := kafka.NewConsumer(a.brokers, topic, group)
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
			log.Printf("ingest read error topic=%s: %v", topic, err)
			continue
		}
		a.onAccepted(ctx, topic, msg)
		if err := c.Commit(ctx, msg); err != nil {
			log.Printf("ingest commit error topic=%s: %v", topic, err)
		}
	}
}

func (a *Applier) onAccepted(ctx context.Context, topic string, msg kgo.Message) {
	var rec pb.AcceptedRecord
	if err := pb.UnmarshalJSON(msg.Value, &rec); err != nil {
		log.Printf("failed to decode accepted record: %v", err)
		return
	}
	if rec.Key == "" || rec.OpId == "" {
		return
	}

	a.locks.Lock(rec.Key)
	defer a.locks.Unlock(rec.Key)

	a.setAccepted(rec.OpId)
	entry := PendingEntry{Op: &rec, Topic: topic, Partition: msg.Partition, Offset: msg.Offset}
	a.pending.Add(rec.Key, entry)

	stableState, _ := a.store.Get(rec.Key)
	other := a.pending.GetOtherRegion(rec.Key, rec.Region)
	for _, contender := range other {
		classification, reason := ClassifyWithMode(a.classifyMode, &rec, contender.Op, stableState)
		switch classification {
		case ClassificationMergeable:
			ops := []*pb.AcceptedRecord{&rec, contender.Op}
			sorted := append([]*pb.AcceptedRecord(nil), ops...)
			sort.Slice(sorted, func(i, j int) bool { return compareAccepted(sorted[i], sorted[j]) < 0 })
			if sorted[0].Region == a.region {
				finalRec, err := ComputeMerge(ops, stableState, rec.Key, a.region)
				if err != nil {
					log.Printf("merge failed, escalating conflict: key=%s err=%v", rec.Key, err)
					a.emitConflict(ctx, rec.Key, reason, stableState, entry, contender)
					continue
				}
				if err := a.emitFinalize(ctx, finalRec); err != nil {
					log.Printf("failed to emit merge finalize: %v", err)
				}
			}
		case ClassificationConflicting:
			a.emitConflict(ctx, rec.Key, reason, stableState, entry, contender)
		}
	}
}

func (a *Applier) emitConflict(ctx context.Context, key string, reason pb.ConflictReason, stableState json.RawMessage, aEntry, bEntry PendingEntry) {
	opIDs := []string{aEntry.Op.OpId, bEntry.Op.OpId}
	base := jsonutil.CanonicalHash(stableState)
	conflictID := ConflictID(key, opIDs, base)
	cert := &pb.ConflictCertificate{
		ConflictId:        conflictID,
		Key:               key,
		Reason:            reason,
		StableVersionHash: base,
		Contenders: []*pb.OpRef{
			{Region: aEntry.Op.Region, Topic: aEntry.Topic, Partition: int32(aEntry.Partition), Offset: aEntry.Offset, OpId: aEntry.Op.OpId, Hlc: aEntry.Op.Hlc},
			{Region: bEntry.Op.Region, Topic: bEntry.Topic, Partition: int32(bEntry.Partition), Offset: bEntry.Offset, OpId: bEntry.Op.OpId, Hlc: bEntry.Op.Hlc},
		},
	}
	b, err := pb.MarshalJSON(cert)
	if err != nil {
		log.Printf("failed to serialize cert: %v", err)
		return
	}
	if _, _, err := a.producer.Write(ctx, "arb.cert", key, b); err != nil {
		log.Printf("failed to write arb.cert: %v", err)
		return
	}
	a.certEmitted.Add(1)
}

func (a *Applier) emitFinalize(ctx context.Context, rec *pb.FinalizeRecord) error {
	b, err := pb.MarshalJSON(rec)
	if err != nil {
		return err
	}
	_, _, err = a.producer.Write(ctx, "arb.final", rec.Key, b)
	if err == nil && rec != nil && rec.FinalizeType == pb.FinalizeType_FINALIZE_MERGE {
		a.mergeFinalizeEmitted.Add(1)
	}
	return err
}

func (a *Applier) consumeFinal(ctx context.Context, topic, group string) {
	c := kafka.NewConsumer(a.brokers, topic, group)
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
			log.Printf("final read error: %v", err)
			continue
		}
		var rec pb.FinalizeRecord
		if err := pb.UnmarshalJSON(msg.Value, &rec); err != nil {
			log.Printf("failed to decode finalize: %v", err)
			_ = c.Commit(ctx, msg)
			continue
		}
		a.finalRecordsConsumed.Add(1)
		switch rec.FinalizeType {
		case pb.FinalizeType_FINALIZE_MERGE:
			a.finalMergeConsumed.Add(1)
		case pb.FinalizeType_FINALIZE_REBASE:
			a.finalRebaseConsumed.Add(1)
		case pb.FinalizeType_FINALIZE_REBASE_LLM:
			a.finalRebaseLLMConsumed.Add(1)
		}
		a.locks.Lock(rec.Key)
		if err := a.ApplyFinalize(&rec); err != nil {
			a.finalApplyErrors.Add(1)
			log.Printf("failed apply finalize key=%s conflict=%s err=%v", rec.Key, rec.ConflictId, err)
			a.locks.Unlock(rec.Key)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		a.locks.Unlock(rec.Key)

		if err := c.Commit(ctx, msg); err != nil {
			log.Printf("final commit error: %v", err)
		}
	}
}

func (a *Applier) setAccepted(opID string) {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	if st, ok := a.status[opID]; ok && st.Status == "FINALIZED" {
		return
	}
	a.status[opID] = OpStatus{Status: "ACCEPTED"}
}

func (a *Applier) setFinalized(opID string, out *pb.OpOutcome) {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	outcome := "UNKNOWN"
	switch out.Outcome {
	case pb.OutcomeType_OUTCOME_COMMIT_PATCH:
		outcome = "COMMIT_PATCH"
		a.outcomeCommitPatch.Add(1)
	case pb.OutcomeType_OUTCOME_NOOP:
		outcome = "NOOP"
		a.outcomeNoop.Add(1)
	case pb.OutcomeType_OUTCOME_FAIL:
		outcome = "FAIL"
		a.outcomeFail.Add(1)
	case pb.OutcomeType_OUTCOME_TRANSFORM:
		outcome = "TRANSFORM"
		a.outcomeTransform.Add(1)
	}
	a.status[opID] = OpStatus{Status: "FINALIZED", Outcome: outcome, Reason: out.Reason}
}

type MetricsSnapshot struct {
	CertEmitted            int64 `json:"cert_emitted"`
	MergeFinalizeEmitted   int64 `json:"merge_finalize_emitted"`
	FinalRecordsConsumed   int64 `json:"final_records_consumed"`
	FinalMergeConsumed     int64 `json:"final_merge_consumed"`
	FinalRebaseConsumed    int64 `json:"final_rebase_consumed"`
	FinalRebaseLLMConsumed int64 `json:"final_rebase_llm_consumed"`
	OutcomeCommitPatch     int64 `json:"outcome_commit_patch"`
	OutcomeNoop            int64 `json:"outcome_noop"`
	OutcomeFail            int64 `json:"outcome_fail"`
	OutcomeTransform       int64 `json:"outcome_transform"`
	FinalApplyErrors       int64 `json:"final_apply_errors"`
	FinalizeDuplicateSkips int64 `json:"finalize_duplicate_skips"`
	BaseHashMismatches     int64 `json:"base_hash_mismatches"`
	FinalHashMismatches    int64 `json:"final_hash_mismatches"`
}

func (a *Applier) MetricsSnapshot() MetricsSnapshot {
	return MetricsSnapshot{
		CertEmitted:            a.certEmitted.Load(),
		MergeFinalizeEmitted:   a.mergeFinalizeEmitted.Load(),
		FinalRecordsConsumed:   a.finalRecordsConsumed.Load(),
		FinalMergeConsumed:     a.finalMergeConsumed.Load(),
		FinalRebaseConsumed:    a.finalRebaseConsumed.Load(),
		FinalRebaseLLMConsumed: a.finalRebaseLLMConsumed.Load(),
		OutcomeCommitPatch:     a.outcomeCommitPatch.Load(),
		OutcomeNoop:            a.outcomeNoop.Load(),
		OutcomeFail:            a.outcomeFail.Load(),
		OutcomeTransform:       a.outcomeTransform.Load(),
		FinalApplyErrors:       a.finalApplyErrors.Load(),
		FinalizeDuplicateSkips: a.finalizeDuplicateSkips.Load(),
		BaseHashMismatches:     a.baseHashMismatches.Load(),
		FinalHashMismatches:    a.finalHashMismatches.Load(),
	}
}
