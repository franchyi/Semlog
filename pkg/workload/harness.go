package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type BurstProfile struct{}

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

type RunResult struct {
	Baseline     string         `json:"baseline"`
	Summary      MetricsSummary `json:"summary"`
	VerifyPassed bool           `json:"verify_passed"`
	VerifyError  string         `json:"verify_error,omitempty"`
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
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		h.runRegion(ctx, "A", h.IngestAURL, kp, deadline)
	}()
	go func() {
		defer wg.Done()
		h.runRegion(ctx, "B", h.IngestBURL, kp, deadline)
	}()
	wg.Wait()

	dur := time.Since(start)
	summary := h.Metrics.Summary(dur)

	verifyErr := Verify(h.ApplierAURL, h.ApplierBURL)
	result := RunResult{Baseline: h.Config.Baseline, Summary: summary, VerifyPassed: verifyErr == nil}
	if verifyErr != nil {
		result.VerifyError = verifyErr.Error()
	}
	return result, nil
}

func (h *Harness) runRegion(ctx context.Context, region, ingestURL string, kp *KeyPicker, deadline time.Time) {
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
		if err := h.sendWrite(ctx, ingestURL, req); err == nil {
			h.Metrics.RecordFinalized()
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

func (h *Harness) sendWrite(ctx context.Context, ingestURL string, req WriteRequest) error {
	b, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ingestURL+"/write", bytes.NewReader(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := h.Client.Do(httpReq)
	lat := time.Since(start)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("write failed status=%d", resp.StatusCode)
	}
	h.Metrics.RecordAccepted(lat)
	return nil
}
