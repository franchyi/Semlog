package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

type BurstProfile struct{}

const (
	finalizeSampleTimeout      = 30 * time.Second
	finalizeSampleTickInterval = 100 * time.Millisecond
	finalizeSampleBatchSize    = 256
	finalizeSampleOpCap        = 1000
)

type WorkloadConfig struct {
	KeyspaceSize         int
	ZipfTheta            float64
	SharedHotFraction    float64
	OpMix                map[string]float64
	DisjointFieldProb    float64
	OpsPerSecond         int
	DurationSec          int
	Baseline             string
	CrossRegionLatencyMS int
	BurstProfile         *BurstProfile
}

type WriteRequest struct {
	Key      string          `json:"key"`
	OpType   string          `json:"op_type"`
	Args     json.RawMessage `json:"args"`
	WriteSet []string        `json:"write_set"`
	ClientID string          `json:"client_id,omitempty"`
}

type WorkloadModel interface {
	InitValue(key string) json.RawMessage
	NextOp(region string, key string, rng *rand.Rand) WriteRequest
	CheckInvariants(value json.RawMessage) bool
}

type Harness struct {
	Config      WorkloadConfig
	Model       WorkloadModel
	IngestAURL  string
	IngestBURL  string
	ApplierAURL string
	ApplierBURL string
	Metrics     *Metrics
	Client      *http.Client
}

type ArbitrationSummary struct {
	CertCount              int64   `json:"cert_count"`
	CertPerS               float64 `json:"cert_per_sec"`
	FinalRecordCount       int64   `json:"final_record_count"`
	FinalPerS              float64 `json:"final_per_sec"`
	MergeFinalizeCount     int64   `json:"merge_finalize_count"`
	RebaseFinalizeCount    int64   `json:"rebase_finalize_count"`
	RebaseLLMFinalizeCount int64   `json:"rebase_llm_finalize_count"`
	AutoMergeRate          float64 `json:"auto_merge_rate"`
	OutcomeCommitPatch     int64   `json:"arb_outcome_commit_patch"`
	OutcomeNoop            int64   `json:"arb_outcome_noop"`
	OutcomeFail            int64   `json:"arb_outcome_fail"`
	OutcomeTransform       int64   `json:"arb_outcome_transform"`
}

type RunResult struct {
	Baseline         string             `json:"baseline"`
	Summary          MetricsSummary     `json:"summary"`
	Arbitration      ArbitrationSummary `json:"arbitration"`
	ArbitrationError string             `json:"arbitration_error,omitempty"`
	VerifyPassed     bool               `json:"verify_passed"`
	VerifyError      string             `json:"verify_error,omitempty"`
}

func (h *Harness) Run(ctx context.Context) (RunResult, error) {
	if h.Client == nil {
		h.Client = &http.Client{Timeout: 3 * time.Second}
	}
	if h.Metrics == nil {
		h.Metrics = NewMetrics()
	}
	h.applyBaselineConfig()

	start := time.Now()
	deadline := start.Add(time.Duration(h.Config.DurationSec) * time.Second)
	if h.Config.OpsPerSecond <= 0 {
		h.Config.OpsPerSecond = 1
	}
	if h.Config.KeyspaceSize <= 0 {
		h.Config.KeyspaceSize = 1
	}

	kp := NewKeyPicker(h.Config.KeyspaceSize, h.Config.ZipfTheta, h.Config.SharedHotFraction, time.Now().UnixNano())
	finalizeJobs := make(chan finalizeJob, 4096)
	submitted := make([]finalizeJob, 0, 8192)
	var submittedMu sync.Mutex
	var collectorWG sync.WaitGroup
	collectorWG.Add(1)
	go func() {
		defer collectorWG.Done()
		for job := range finalizeJobs {
			submittedMu.Lock()
			submitted = append(submitted, job)
			submittedMu.Unlock()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		h.runRegion(ctx, "A", h.IngestAURL, kp, deadline, finalizeJobs)
	}()
	go func() {
		defer wg.Done()
		h.runRegion(ctx, "B", h.IngestBURL, kp, deadline, finalizeJobs)
	}()
	wg.Wait()
	close(finalizeJobs)
	collectorWG.Wait()
	submittedMu.Lock()
	snapshot := append([]finalizeJob(nil), submitted...)
	submittedMu.Unlock()
	h.collectFinalization(ctx, snapshot, finalizeSampleTimeout)

	dur := time.Since(start)
	summary := h.Metrics.Summary(dur)
	arb, arbErr := h.fetchArbitrationSummary(dur)

	verifyErr := Verify(h.ApplierAURL, h.ApplierBURL)
	result := RunResult{
		Baseline:     h.Config.Baseline,
		Summary:      summary,
		Arbitration:  arb,
		VerifyPassed: verifyErr == nil,
	}
	if arbErr != nil {
		result.ArbitrationError = arbErr.Error()
	}
	if verifyErr != nil {
		result.VerifyError = verifyErr.Error()
	}
	return result, nil
}

func (h *Harness) runRegion(ctx context.Context, region, ingestURL string, kp *KeyPicker, deadline time.Time, finalizeJobs chan<- finalizeJob) {
	interval := time.Second / time.Duration(max(1, h.Config.OpsPerSecond))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(region[0])))

	for {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return
		}
		<-ticker.C
		key := kp.Pick(region)
		req := h.Model.NextOp(region, key, rng)
		if h.Config.Baseline == "b4" && region == "B" && h.Config.CrossRegionLatencyMS > 0 {
			time.Sleep(time.Duration(h.Config.CrossRegionLatencyMS) * time.Millisecond)
		}
		receipt, acceptedAt, lat, err := h.sendWrite(ctx, ingestURL, req)
		if err == nil {
			h.Metrics.RecordAccepted(lat)
			finalizeJobs <- finalizeJob{
				OpID:       receipt.OpID,
				AcceptedAt: acceptedAt,
				ApplierURL: h.applierURLForRegion(receipt.Region),
			}
		}
	}
}

func (h *Harness) applyBaselineConfig() {
	switch h.normalizeBaseline() {
	case "b4":
		h.IngestBURL = h.IngestAURL
		if h.Config.CrossRegionLatencyMS <= 0 {
			h.Config.CrossRegionLatencyMS = 80
		}
	default:
		if h.Config.CrossRegionLatencyMS < 0 {
			h.Config.CrossRegionLatencyMS = 0
		}
	}
}

func (h *Harness) normalizeBaseline() string {
	switch h.Config.Baseline {
	case "b1", "b2", "b3", "b4", "full":
		return h.Config.Baseline
	default:
		h.Config.Baseline = "full"
		return "full"
	}
}

type writeReceipt struct {
	OpID   string `json:"op_id"`
	Region string `json:"region"`
}

type finalizeJob struct {
	OpID       string
	AcceptedAt time.Time
	ApplierURL string
}

type applierMetrics struct {
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
}

type opStatusResponse struct {
	Status  string `json:"status"`
	Outcome string `json:"outcome"`
}

var errStatusNotFound = errors.New("status not found")

func (h *Harness) sendWrite(ctx context.Context, ingestURL string, req WriteRequest) (writeReceipt, time.Time, time.Duration, error) {
	b, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ingestURL+"/write", bytes.NewReader(b))
	if err != nil {
		return writeReceipt{}, time.Time{}, 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := h.Client.Do(httpReq)
	lat := time.Since(start)
	if err != nil {
		return writeReceipt{}, time.Time{}, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return writeReceipt{}, time.Time{}, 0, fmt.Errorf("write failed status=%d", resp.StatusCode)
	}
	var receipt writeReceipt
	if err := json.NewDecoder(resp.Body).Decode(&receipt); err != nil {
		return writeReceipt{}, time.Time{}, 0, err
	}
	if receipt.OpID == "" {
		return writeReceipt{}, time.Time{}, 0, fmt.Errorf("write response missing op_id")
	}
	if receipt.Region == "" {
		receipt.Region = "A"
	}
	return receipt, time.Now(), lat, nil
}

func (h *Harness) collectFinalization(ctx context.Context, submitted []finalizeJob, timeout time.Duration) {
	if len(submitted) == 0 {
		return
	}
	if len(submitted) > finalizeSampleOpCap {
		step := len(submitted) / finalizeSampleOpCap
		if step < 1 {
			step = 1
		}
		sampled := make([]finalizeJob, 0, finalizeSampleOpCap)
		for i := 0; i < len(submitted) && len(sampled) < finalizeSampleOpCap; i += step {
			sampled = append(sampled, submitted[i])
		}
		submitted = sampled
	}
	pending := make(map[string]finalizeJob, len(submitted))
	for _, job := range submitted {
		if job.OpID == "" || job.ApplierURL == "" {
			continue
		}
		pending[job.OpID] = job
	}
	if len(pending) == 0 {
		return
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(finalizeSampleTickInterval)
	defer ticker.Stop()

	keys := make([]string, 0, len(pending))
	offset := 0
	for len(pending) > 0 && time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		keys = keys[:0]
		for opID := range pending {
			keys = append(keys, opID)
		}
		if len(keys) == 0 {
			break
		}

		batch := finalizeSampleBatchSize
		if batch > len(keys) {
			batch = len(keys)
		}
		start := offset % len(keys)
		for i := 0; i < batch; i++ {
			idx := (start + i) % len(keys)
			opID := keys[idx]
			job, ok := pending[opID]
			if !ok {
				continue
			}
			status, outcome, err := h.fetchStatus(ctx, job.ApplierURL, opID)
			if err != nil {
				if errors.Is(err, errStatusNotFound) {
					continue
				}
				continue
			}
			if status == "FINALIZED" {
				h.Metrics.RecordFinalized(time.Since(job.AcceptedAt), outcome)
				delete(pending, opID)
			}
		}
		offset += batch
		<-ticker.C
	}

	for range pending {
		h.Metrics.RecordFinalizeTimeout()
	}
}

func (h *Harness) fetchStatus(ctx context.Context, applierURL, opID string) (string, string, error) {
	url := strings.TrimSuffix(applierURL, "/") + "/status?op_id=" + opID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", "", err
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", "", errStatusNotFound
	}
	if resp.StatusCode/100 != 2 {
		return "", "", fmt.Errorf("status endpoint returned %d", resp.StatusCode)
	}
	var s opStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return "", "", err
	}
	return s.Status, s.Outcome, nil
}

func (h *Harness) fetchArbitrationSummary(dur time.Duration) (ArbitrationSummary, error) {
	url := strings.TrimSuffix(h.ApplierAURL, "/") + "/metrics"
	resp, err := h.Client.Get(url)
	if err != nil {
		return ArbitrationSummary{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return ArbitrationSummary{}, fmt.Errorf("metrics endpoint returned %d", resp.StatusCode)
	}
	var m applierMetrics
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return ArbitrationSummary{}, err
	}
	sec := dur.Seconds()
	if sec <= 0 {
		sec = 1
	}
	out := ArbitrationSummary{
		CertCount:              m.CertEmitted,
		CertPerS:               float64(m.CertEmitted) / sec,
		FinalRecordCount:       m.FinalRecordsConsumed,
		FinalPerS:              float64(m.FinalRecordsConsumed) / sec,
		MergeFinalizeCount:     m.FinalMergeConsumed,
		RebaseFinalizeCount:    m.FinalRebaseConsumed,
		RebaseLLMFinalizeCount: m.FinalRebaseLLMConsumed,
		OutcomeCommitPatch:     m.OutcomeCommitPatch,
		OutcomeNoop:            m.OutcomeNoop,
		OutcomeFail:            m.OutcomeFail,
		OutcomeTransform:       m.OutcomeTransform,
	}
	totalDecisions := m.CertEmitted + m.MergeFinalizeEmitted
	if totalDecisions > 0 {
		out.AutoMergeRate = float64(m.MergeFinalizeEmitted) / float64(totalDecisions)
	}
	return out, nil
}

func (h *Harness) applierURLForRegion(region string) string {
	if strings.EqualFold(strings.TrimSpace(region), "B") {
		if h.ApplierBURL != "" {
			return h.ApplierBURL
		}
	}
	return h.ApplierAURL
}
