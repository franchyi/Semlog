package workload

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	accepted  atomic.Int64
	finalized atomic.Int64
	timeouts  atomic.Int64

	mu                 sync.Mutex
	acceptedLatencies  []time.Duration
	finalizedLatencies []time.Duration

	outcomeCommitPatch atomic.Int64
	outcomeNoop        atomic.Int64
	outcomeFail        atomic.Int64
	outcomeTransform   atomic.Int64
	outcomeUnknown     atomic.Int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) RecordAccepted(latency time.Duration) {
	m.accepted.Add(1)
	m.mu.Lock()
	m.acceptedLatencies = append(m.acceptedLatencies, latency)
	m.mu.Unlock()
}

func (m *Metrics) RecordFinalized(latency time.Duration, outcome string) {
	m.finalized.Add(1)
	m.mu.Lock()
	m.finalizedLatencies = append(m.finalizedLatencies, latency)
	m.mu.Unlock()
	switch outcome {
	case "COMMIT_PATCH":
		m.outcomeCommitPatch.Add(1)
	case "NOOP":
		m.outcomeNoop.Add(1)
	case "FAIL":
		m.outcomeFail.Add(1)
	case "TRANSFORM":
		m.outcomeTransform.Add(1)
	default:
		m.outcomeUnknown.Add(1)
	}
}

func (m *Metrics) RecordFinalizeTimeout() {
	m.timeouts.Add(1)
}

type MetricsSummary struct {
	Accepted           int64         `json:"accepted"`
	Finalized          int64         `json:"finalized"`
	FinalizeTimeouts   int64         `json:"finalize_timeouts"`
	P50Accepted        time.Duration `json:"p50_accepted"`
	P95Accepted        time.Duration `json:"p95_accepted"`
	P99Accepted        time.Duration `json:"p99_accepted"`
	P50Finalized       time.Duration `json:"p50_finalized"`
	P95Finalized       time.Duration `json:"p95_finalized"`
	P99Finalized       time.Duration `json:"p99_finalized"`
	AcceptedPerS       float64       `json:"accepted_per_sec"`
	FinalizedPerS      float64       `json:"finalized_per_sec"`
	OutcomeCommitPatch int64         `json:"outcome_commit_patch"`
	OutcomeNoop        int64         `json:"outcome_noop"`
	OutcomeFail        int64         `json:"outcome_fail"`
	OutcomeTransform   int64         `json:"outcome_transform"`
	OutcomeUnknown     int64         `json:"outcome_unknown"`
}

func (m *Metrics) Summary(duration time.Duration) MetricsSummary {
	m.mu.Lock()
	acceptedLat := append([]time.Duration(nil), m.acceptedLatencies...)
	finalizedLat := append([]time.Duration(nil), m.finalizedLatencies...)
	m.mu.Unlock()
	sort.Slice(acceptedLat, func(i, j int) bool { return acceptedLat[i] < acceptedLat[j] })
	sort.Slice(finalizedLat, func(i, j int) bool { return finalizedLat[i] < finalizedLat[j] })

	p := func(v []time.Duration, q float64) time.Duration {
		if len(v) == 0 {
			return 0
		}
		idx := int(float64(len(v)-1) * q)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(v) {
			idx = len(v) - 1
		}
		return v[idx]
	}

	sec := duration.Seconds()
	if sec <= 0 {
		sec = 1
	}
	a := m.accepted.Load()
	f := m.finalized.Load()
	return MetricsSummary{
		Accepted:           a,
		Finalized:          f,
		FinalizeTimeouts:   m.timeouts.Load(),
		P50Accepted:        p(acceptedLat, 0.50),
		P95Accepted:        p(acceptedLat, 0.95),
		P99Accepted:        p(acceptedLat, 0.99),
		P50Finalized:       p(finalizedLat, 0.50),
		P95Finalized:       p(finalizedLat, 0.95),
		P99Finalized:       p(finalizedLat, 0.99),
		AcceptedPerS:       float64(a) / sec,
		FinalizedPerS:      float64(f) / sec,
		OutcomeCommitPatch: m.outcomeCommitPatch.Load(),
		OutcomeNoop:        m.outcomeNoop.Load(),
		OutcomeFail:        m.outcomeFail.Load(),
		OutcomeTransform:   m.outcomeTransform.Load(),
		OutcomeUnknown:     m.outcomeUnknown.Load(),
	}
}
