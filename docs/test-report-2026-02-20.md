# Semlog Baseline Comparison Report

Date: February 20, 2026  
System labels: `slx` (Semlog core), `slx-l` (Semlog + LLM repair)

## Executive Summary

- A fresh short-run matrix (`A/B/C/D` x `slx/slx-l/b1/b2/b3/b4`) completed successfully: `24/24` pass.
- In this 5-second setting, no single mode dominates all workloads.
- `slx-l` showed **zero** accepted LLM finalizations in all runs (`rebase_llm_finalize_count = 0`).
- The main bottleneck in conflict-heavy settings appears to be arbitration/finalizer pressure, not ingest.

## Experiment Setup

- Workloads: `A`, `B`, `C`, `D`
- Baselines: `slx`, `slx-l`, `b1`, `b2`, `b3`, `b4`
- Duration: `5s`
- Rate: `100 ops/sec` per region
- Keyspace: `1000`
- Zipf theta: `0.8`
- Shared hot fraction: `0.05`
- Disjoint field probability: `0.8`
- Verification condition: `verify_passed=true`

## Result Artifacts

- Summary index: `results/compare-5s-abcd-20260220-023053/summary.csv`
- Parsed metrics: `results/compare-5s-abcd-20260220-023053/metrics.csv`
- Raw logs: `results/compare-5s-abcd-20260220-023053/`
- Example logs:
  - `results/compare-5s-abcd-20260220-023053/wA_slx_a1.log`
  - `results/compare-5s-abcd-20260220-023053/wA_slx-l_a1.log`
  - `results/compare-5s-abcd-20260220-023053/wB_b2_a1.log`

## Completion Status

- Total cells: `24`
- Passed: `24`
- Failed: `0`

## Primary Comparison (finalized/sec)

| Workload | slx | slx-l | b1 | b2 | b3 | b4 |
|---|---:|---:|---:|---:|---:|---:|
| A | 13.797 | 6.684 | 0.057 | 9.427 | 1.200 | 0.000 |
| B | 5.742 | 6.313 | 0.714 | 13.540 | 0.000 | 0.000 |
| C | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| D | 0.057 | 0.000 | 0.000 | 0.714 | 0.000 | 0.000 |

## LLM Utilization Observation

- `slx-l` produced `rebase_llm_finalize_count = 0` in every workload.
- Interpretation: the current LLM path did not produce accepted LLM finalization records in this matrix.
- Practical implication: `slx-l` behavior is effectively close to deterministic rebase in these runs, with possible overhead but no measured LLM gain.

## Interpretation

- `A` (moderate conflict profile): `slx` outperformed `b1/b3/b4` and exceeded `slx-l` in this run.
- `B` (profile patch): `b2` was highest on finalized/sec in this short window.
- `C` and `D` (conflict-heavy): all modes had near-zero finalized sampling in 5 seconds, suggesting insufficient settle time rather than pure ingest bottleneck.
- `b4` remained lower on accepted throughput due to single-master routing.

## Why Duration Matters

Yes, duration affects measured performance significantly.

- `5s` is good for fast smoke checks.
- `5s` can severely under-represent finalization throughput when arbitration queues form.
- `15s` is a better quick comparison target.
- `30s` is preferable for advisor/report-quality claims.

## Suggested Next Measurements

1. Re-run this same matrix at `15s` for stable ranking without long turnaround.
2. Re-run selected stress workloads (`C`, `D`) at `30s` to separate backlog effects from algorithmic differences.
3. Add counters for LLM attempt lifecycle:
   - LLM attempted
   - API call made
   - validation rejected
   - fallback reason
   - accepted as `FINALIZE_REBASE_LLM`

## Questions for Advisor (LLM Utilization)

1. Should we prioritize LLM quality gains only on selected conflict classes (for example `CAS`/precondition failures), instead of all rebase failures?
2. Should merge-path failures be escalated deliberately to a rebase+LLM lane so LLM has meaningful opportunities?
3. Is the right objective to improve finalized throughput, operation survival rate, or semantic quality under fixed latency budget?
4. Should we evaluate LLM value with workload-specific policies and allowlists rather than a single generic repair prompt?
5. For publication-quality evaluation, is `15s` sufficient for main charts, with `30s` only for high-contention workloads?

