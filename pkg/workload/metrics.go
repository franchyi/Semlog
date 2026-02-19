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

	mu                sync.Mutex
	acceptedLatencies []time.Duration
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

func (m *Metrics) RecordFinalized() {
	m.finalized.Add(1)
}

type MetricsSummary struct {
	Accepted      int64         `json:"accepted"`
	Finalized     int64         `json:"finalized"`
	P50Accepted   time.Duration `json:"p50_accepted"`
	P95Accepted   time.Duration `json:"p95_accepted"`
	P99Accepted   time.Duration `json:"p99_accepted"`
	AcceptedPerS  float64       `json:"accepted_per_sec"`
	FinalizedPerS float64       `json:"finalized_per_sec"`
}

func (m *Metrics) Summary(duration time.Duration) MetricsSummary {
	m.mu.Lock()
	lat := append([]time.Duration(nil), m.acceptedLatencies...)
	m.mu.Unlock()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	p := func(q float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		idx := int(float64(len(lat)-1) * q)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(lat) {
			idx = len(lat) - 1
		}
		return lat[idx]
	}

	sec := duration.Seconds()
	if sec <= 0 {
		sec = 1
	}
	a := m.accepted.Load()
	f := m.finalized.Load()
	return MetricsSummary{
		Accepted:      a,
		Finalized:     f,
		P50Accepted:   p(0.50),
		P95Accepted:   p(0.95),
		P99Accepted:   p(0.99),
		AcceptedPerS:  float64(a) / sec,
		FinalizedPerS: float64(f) / sec,
	}
}
