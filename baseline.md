# baseline.md — Performance Baselines (same codebase, flag-controlled)

## Overview

All baselines are implemented as **runtime flags** on the existing codebase. No separate binaries or systems. This ensures identical infrastructure (Redpanda, serialization, network path) so measurements isolate the protocol contribution, not implementation differences.

---

## Baseline Matrix

| ID | Name | DP1 (Classifier) | DP2 (Rebase) | Write Path | What it measures |
|----|------|-------------------|--------------|------------|-----------------|
| **SLX** | Semlog core | ON | ON (`rebase`) | Both regions | Combined benefit of DP1+DP2 without LLM |
| **SLX-L** | Semlog + LLM repair | ON | ON (`rebase+llm`) | Both regions | Incremental impact of LLM-assisted repair |
| **B1** | Naive conflict | OFF (all same-key = conflict) | ON | Both regions | Value of structural classification |
| **B2** | LWW | ON | OFF (last writer wins) | Both regions | Value of deterministic rebase |
| **B3** | Naive + LWW | OFF | OFF | Both regions | Worst multi-master (pure LWW) |
| **B4** | Single-master | N/A | N/A | One region only | Cost of strong consistency |

B3 (Naive + LWW) is the combined ablation — shows what you get from a naive multi-master with no intelligence. Comparing SLX vs B3 shows the total contribution of both design points together.

---

## Flags

### Applier flags

| Flag | Default | Values | Effect |
|------|---------|--------|--------|
| `--classify-mode` | `structural` | `structural`, `naive` | `naive`: skip classification, treat all same-key concurrent ops as CONFLICTING |

### Finalizer flags

| Flag | Default | Values | Effect |
|------|---------|--------|--------|
| `--rebase-mode` | `rebase` | `rebase`, `lww` | `lww`: highest-HLC op wins, all others NOOP |

### Harness flags

| Flag | Default | Values | Effect |
|------|---------|--------|--------|
| `--baseline` | `slx` | `slx`, `slx-l`, `b1`, `b2`, `b3`, `b4` | Convenience flag that sets classify-mode + rebase-mode + write routing |

The `--baseline` flag is sugar that configures the appropriate combination:

```
--baseline=slx  →  classify-mode=structural, rebase-mode=rebase, both regions write
--baseline=slx-l →  classify-mode=structural, rebase-mode=rebase+llm, both regions write
--baseline=b1    →  classify-mode=naive,      rebase-mode=rebase, both regions write
--baseline=b2    →  classify-mode=structural, rebase-mode=lww,    both regions write
--baseline=b3    →  classify-mode=naive,      rebase-mode=lww,    both regions write
--baseline=b4    →  classify-mode=structural, rebase-mode=rebase, single region write
```

---

## B1: Naive Conflict (classify-mode=naive)

### What changes

In `pkg/applier/classify.go`, when `--classify-mode=naive`:

```go
func (a *Applier) Classify(op1, op2 *AcceptedRecord, stableState json.RawMessage) Classification {
    if a.classifyMode == "naive" {
        // All same-key concurrent ops from different regions are CONFLICTING.
        // No write-set analysis, no commutativity check.
        return CONFLICTING
    }
    // ... normal structural classification
}
```

### What stays the same

- Ingest, Kafka, protobuf, topic layout — all identical
- Finalizer rebase engine — identical (still re-executes in decided order)
- ApplyFinalize — identical (still applies patches from arb.final)
- Workloads — identical

### What to measure

- `arb.cert/sec` and `arb.final/sec` (should be much higher than SLX)
- Finalize latency P50/P95/P99 (should be higher due to more arbitration traffic)
- Throughput (may be lower if finalizer becomes bottleneck)
- **Correctness must still hold** (convergence, invariants) — this is just less efficient, not incorrect

### Expected result

Under Workload B (Profile Patch, disjoint_field_prob=0.8):
- B1 sends ~5x-10x more certificates to arb.cert than SLX
- Finalize latency increases because ops that could have been merged locally now round-trip through the Finalizer
- Throughput may drop if Finalizer saturates

---

## B2: Last-Writer-Wins (rebase-mode=lww)

### What changes

In `pkg/finalizer/rebase.go`, when `--rebase-mode=lww`:

```go
func (f *Finalizer) Rebase(stableState json.RawMessage, ops []*AcceptedRecord, key string) *FinalizeRecord {
    if f.rebaseMode == "lww" {
        return f.rebaseLWW(stableState, ops, key)
    }
    return f.rebaseFull(stableState, ops, key)
}

func (f *Finalizer) rebaseLWW(stableState json.RawMessage, ops []*AcceptedRecord, key string) *FinalizeRecord {
    // Sort by HLC (same CompareOps — deterministic)
    sort.Slice(ops, func(i, j int) bool {
        return CompareOps(opRefFrom(ops[i]), opRefFrom(ops[j])) < 0
    })

    // Last op (highest HLC) wins. Apply it, NOOP everything else.
    winner := ops[len(ops)-1]
    outcomes := make([]*OpOutcome, len(ops))

    for i, op := range ops {
        if op.OpId == winner.OpId {
            // Apply winner to stable state, compute patch
            newState, outcome := engine.Apply(winner, stableState)
            if outcome == OK {
                patch := engine.Diff(stableState, newState)
                outcomes[i] = &OpOutcome{
                    OpId:      op.OpId,
                    Outcome:   OUTCOME_COMMIT_PATCH,
                    PatchJson: patch,
                }
            } else {
                // Even winner can fail (e.g., CAS mismatch). Then everyone NOOPs.
                outcomes[i] = &OpOutcome{
                    OpId:    op.OpId,
                    Outcome: OUTCOME_FAIL,
                    Reason:  outcome.Reason,
                }
            }
        } else {
            outcomes[i] = &OpOutcome{
                OpId:    op.OpId,
                Outcome: OUTCOME_NOOP,
                Reason:  "lww: not the latest writer",
            }
        }
    }

    // ... build and return FinalizeRecord
}
```

### What stays the same

- Classifier (DP1) still runs — MERGEABLE ops still get fast-path merge
- Only CONFLICTING ops go through LWW instead of rebase
- ApplyFinalize — identical (patches from arb.final)
- Everything else — identical

### What to measure

- **Op survival rate**: % of ops in conflict groups that get COMMIT_PATCH
  - Under rebase: ~60-80% survive (many "losers" still apply cleanly)
  - Under LWW: exactly 1/N survive (where N = contenders per conflict)
- **Application-visible failure rate**: % of all submitted ops that end up NOOP or FAIL
- Invariant correctness (must still hold — LWW winner is applied, losers are NOOPed)

### Expected result

Under Workload C (Task Queue, concurrent CLAIMs):
- SLX: one CLAIM wins, the other FAILs (both attempted, one precondition fails) — correct and explicit
- B2: one CLAIM wins (latest HLC), the other silently NOOPed — correct but information-losing

Under Workload D (Inventory, concurrent RESERVEs):
- SLX: first RESERVE succeeds, second may also succeed if stock permits (rebase re-checks)
- B2: only latest RESERVE wins, earlier one silently NOOPed even if both could have succeeded

---

## B3: Naive + LWW (worst multi-master)

### What changes

Both flags active:
- `--classify-mode=naive` on applier
- `--rebase-mode=lww` on finalizer

### What this represents

This is the "dumb" multi-master baseline: every same-key concurrent write goes to arbitration, and the arbitrator just picks the latest writer. This is approximately what you get from a Dynamo-style LWW system (without vector clocks).

### What to measure

This is the **lower bound** for multi-master quality. Compare SLX vs B3 to show the total value of both design points combined:
- SLX has higher op survival rate
- SLX has lower arb traffic (most merges never reach arbitrator)
- SLX has lower finalize latency (merges skip the arbitration round-trip)
- Both have identical write-acceptance latency (both are multi-master)

---

## B4: Single-Master Serialization

### What changes

No code changes. Harness configuration only:

```go
// In workload harness, when --baseline=b4:
// Both region A and region B producers send writes to ingest-A only.
// Ingest-B is either not started or receives no traffic.
//
// This means:
// - All writes go through one region's log
// - No concurrent ops from different regions on the same key
// - No conflicts, no certificates, no arbitration
// - Applier-B consumes ingest.A and converges trivially
```

Harness flag:
```
--baseline=b4  →  --ingest-a-url=http://localhost:8081 --ingest-b-url=http://localhost:8081
```

Both producers POST to ingest-A. Region B's producer simulates a remote client paying cross-region latency.

### Simulating cross-region write latency for B4

To make this realistic, add artificial latency for "Region B" writes in the harness:

```go
// In harness, for B4 mode, Region B producer:
if baseline == "b4" && region == "B" {
    time.Sleep(crossRegionLatency)  // e.g., 80ms
}
// Then POST to ingest-A
```

This simulates the real cost: a Region B client must cross the WAN to reach the single master.

### What to measure

- **Write latency for Region B clients**: under B4, Region B pays cross-region RTT on every write. Under SLX, Region B writes locally (fast ack), finalization happens asynchronously.
- **Write throughput**: B4 is limited by single-region ingest capacity. SLX has 2x ingest capacity (both regions accept writes).
- **Read consistency**: B4 gives strong consistency (all writes serialized). SLX gives CSCC (weaker, but with formal guarantees).

### Expected result

- Region B write latency: B4 ≈ 80-120ms (WAN RTT), SLX ≈ 1-5ms (local append)
- Throughput: B4 ≈ X ops/sec, SLX ≈ 1.5-2X ops/sec (two ingest pipelines)
- Conflict rate: B4 = 0, SLX > 0 (but handled efficiently)

---

## Implementation Plan

### Step 1: Add flags to existing binaries

```go
// cmd/applier/main.go
classifyMode := flag.String("classify-mode", "structural", "structural|naive")

// cmd/finalizer/main.go
rebaseMode := flag.String("rebase-mode", "rebase", "rebase|lww")

// cmd/harness/main.go
baseline := flag.String("baseline", "slx", "slx|slx-l|b1|b2|b3|b4")
```

### Step 2: Implement naive classification (B1)

In `pkg/applier/classify.go`:

```go
func (a *Applier) Classify(op1, op2 *AcceptedRecord, stableState json.RawMessage) Classification {
    if a.classifyMode == "naive" {
        return CONFLICTING
    }
    return a.classifyStructural(op1, op2, stableState)
}
```

~5 lines of code. The existing structural classifier moves to `classifyStructural()`.

### Step 3: Implement LWW rebase (B2)

In `pkg/finalizer/rebase.go`:

```go
func (f *Finalizer) Rebase(stableState json.RawMessage, ops []*AcceptedRecord, key string) *FinalizeRecord {
    if f.rebaseMode == "lww" {
        return f.rebaseLWW(stableState, ops, key)
    }
    return f.rebaseFull(stableState, ops, key)
}
```

`rebaseLWW()` is ~30 lines (shown in B2 section above).

### Step 4: Implement baseline routing in harness

In `pkg/workload/harness.go`:

```go
func (h *Harness) resolveBaseline() {
    switch h.config.Baseline {
    case "slx":
        // defaults: structural + rebase + both regions
    case "slx-l":
        // defaults: structural + rebase+llm + both regions
    case "b1":
        h.applierFlags = append(h.applierFlags, "--classify-mode=naive")
    case "b2":
        h.finalizerFlags = append(h.finalizerFlags, "--rebase-mode=lww")
    case "b3":
        h.applierFlags = append(h.applierFlags, "--classify-mode=naive")
        h.finalizerFlags = append(h.finalizerFlags, "--rebase-mode=lww")
    case "b4":
        h.config.IngestBURL = h.config.IngestAURL  // both producers → ingest-A
        h.config.CrossRegionLatencyMS = 80           // simulate WAN for region B
    }
}
```

### Step 5: Add baseline-aware metrics collection

```go
// pkg/workload/metrics.go

type RunMetrics struct {
    Baseline          string
    Config            WorkloadConfig

    // Throughput
    AcceptedOpsPerSec float64
    FinalizedOpsPerSec float64

    // Latency
    AcceptedLatencyP50  time.Duration
    AcceptedLatencyP95  time.Duration
    AcceptedLatencyP99  time.Duration
    FinalizedLatencyP50 time.Duration
    FinalizedLatencyP95 time.Duration
    FinalizedLatencyP99 time.Duration

    // Arbitration traffic
    ArbCertPerSec  float64
    ArbFinalPerSec float64
    AutoMergeRate  float64  // % of same-key concurrent ops that were auto-merged (0 for B1/B3)

    // Op survival
    TotalOps        int
    CommitCount     int
    NoopCount       int
    FailCount       int
    OpSurvivalRate  float64  // CommitCount / TotalOps

    // Correctness (must pass for ALL baselines)
    ChecksumMatch   bool
    InvariantPass   bool
    FinalizeCoverage bool
    NoDuplicateWins  bool
}
```

---

## Evaluation Script

### scripts/run-baselines.sh

```bash
#!/bin/bash
set -e

WORKLOADS="A B C D"
BASELINES="slx slx-l b1 b2 b3 b4"
DURATION="60s"
OPS="500"
OUTPUT_DIR="results/$(date +%Y%m%d-%H%M%S)"

mkdir -p $OUTPUT_DIR

for workload in $WORKLOADS; do
  for baseline in $BASELINES; do
    echo "=== Workload $workload, Baseline $baseline ==="

    # Start services with appropriate flags
    # (In practice, the harness manages this via --baseline flag)

    go run ./cmd/harness \
      --workload=$workload \
      --baseline=$baseline \
      --duration=$DURATION \
      --ops-per-sec=$OPS \
      --output=$OUTPUT_DIR/w${workload}_${baseline}.json \
      2>&1 | tee $OUTPUT_DIR/w${workload}_${baseline}.log

    echo "--- Cooling down ---"
    sleep 5
  done
done

echo "Results in $OUTPUT_DIR"
```

### Output format per run

```json
{
  "baseline": "slx",
  "workload": "B",
  "config": { "keyspace_size": 1000, "zipf_theta": 0.8, ... },
  "metrics": {
    "accepted_ops_per_sec": 950.2,
    "finalized_ops_per_sec": 948.7,
    "accepted_latency_p50_ms": 2.1,
    "accepted_latency_p99_ms": 8.3,
    "finalized_latency_p50_ms": 45.2,
    "finalized_latency_p99_ms": 112.5,
    "arb_cert_per_sec": 12.3,
    "arb_final_per_sec": 14.1,
    "auto_merge_rate": 0.87,
    "op_survival_rate": 0.96,
    "commit_count": 28412,
    "noop_count": 805,
    "fail_count": 383
  },
  "correctness": {
    "checksum_match": true,
    "invariant_pass": true,
    "finalize_coverage": true,
    "no_duplicate_wins": true
  }
}
```

---

## Target Graphs (for the paper)

### Graph 1: Arb Traffic vs Conflict Rate

X-axis: `shared_hot_fraction` (0 to 0.5)
Y-axis: `arb.cert/sec`
Lines: SLX, B1, B3
Workload: B (Profile Patch)

Shows: SLX has dramatically lower arb traffic than B1/B3 because the classifier auto-merges disjoint field updates.

### Graph 2: Op Survival Rate vs Conflict Rate

X-axis: `shared_hot_fraction` (0 to 0.5)
Y-axis: `op_survival_rate` (fraction of ops that get COMMIT_PATCH)
Lines: SLX, B2, B3
Workload: D (Inventory)

Shows: SLX saves ops that LWW would discard, because rebase re-executes and many ops still satisfy preconditions.

### Graph 3: Finalize Latency CDF

X-axis: latency (ms)
Y-axis: CDF (0 to 1)
Lines: SLX, B1, B3, B4
Workload: B (Profile Patch)

Shows: SLX has lower finalize latency than B1/B3 (less arbitration), but all multi-master baselines have lower write-acceptance latency than B4 (single-master).

### Graph 4: Write Latency — Region B Clients

X-axis: percentile (P50, P75, P90, P95, P99)
Y-axis: write-acceptance latency (ms)
Lines: SLX (multi-master), B4 (single-master)
Workload: any

Shows: Under B4, Region B clients pay ~80ms cross-region RTT on every write. Under SLX, they pay ~2ms local append. This is the fundamental multi-master advantage.

### Graph 5: Throughput Under Increasing Conflict Rate

X-axis: `shared_hot_fraction` (0 to 0.5)
Y-axis: finalized ops/sec
Lines: SLX, B1, B2, B3
Workload: A (Geo-YCSB++)

Shows: SLX degrades gracefully. B1 drops faster (finalizer bottleneck). B3 drops fastest.

### Graph 6: Ablation Breakdown (Stacked)

X-axis: Workload (A, B, C, D)
Y-axis: % of same-key concurrent ops
Stacked bars per workload:
  - Auto-merged by DP1 (SLX only)
  - Survived rebase by DP2 (SLX only)
  - NOOP/FAIL

Shows the progressive reduction pipeline: DP1 handles most, DP2 rescues more, only a small fraction actually fails.

---

## Correctness Requirement

**ALL baselines must pass ALL correctness checks.** The baselines are less efficient or lose more information, but they must never violate:
- Convergence (checksum match between appliers)
- Invariants (stable state valid)
- Finalize coverage (every op gets an outcome)
- No duplicate winners (task queue)

If a baseline fails correctness, the bug is in your implementation, not the baseline design. Fix it.

---

## Implementation Order

Add baselines **after M6** (end-to-end sanity) and **before M7** (workload harness). The harness should be baseline-aware from the start.

```
After M6, before M7:
  B1. Add --classify-mode flag to applier (~5 lines)
  B2. Add --rebase-mode flag to finalizer + rebaseLWW() (~35 lines)
  B3. (Combination of B1+B2, no additional code)
  B4. Add --baseline=b4 logic to harness (route both producers to ingest-A + latency injection) (~10 lines)
  B5. Add --baseline flag to harness that sets all sub-flags (~15 lines)
  B6. Add baseline field to metrics output (~5 lines)

Then proceed to M7 (workload harness) with baseline support built in.
```

Total new code: ~70 lines. All baselines share the same codebase, infrastructure, and verification.
