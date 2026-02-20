# Harness Testing Guide

This document describes how to run workload harness tests and baseline comparisons.

## Prerequisites

- Go installed
- Redpanda + `rpk` installed and reachable at `127.0.0.1:9092`

## Key Metrics

- `accepted/sec`: successful `POST /write` rate at ingest.
- `finalized/sec`: rate of operations observed as `FINALIZED` via applier `/status`.
  - This is a sampled finalize throughput signal from harness finalize polling.
- `cert/sec`: `arb.cert` emission rate from applier metrics.
  - Higher `cert/sec` means more conflict escalation to arbitration.
  - Lower `cert/sec` means more fast-path merge behavior.

## Fast E2E Run

```bash
./scripts/run-e2e.sh
```

This script performs clean reset, launches services, runs harness, requires `"verify_passed": true`, then cleans up.
Default is `--workload B --baseline slx --duration 30s`.

## Run One Baseline

```bash
./scripts/run-e2e.sh \
  --workload B \
  --baseline slx \
  --duration 30s \
  --ops-per-sec 100 \
  --shared-hot-fraction 0.05
```

Notes:

- `slx` maps to `classify-mode=structural` + `rebase-mode=rebase`.
- `slx-l` maps to `classify-mode=structural` + `rebase-mode=rebase+llm`.
- If `OPENAI_API_KEY` is not set, finalizer logs fallback and behaves as deterministic `rebase`.

## Full Comparison (All Workloads x All Baselines)

This runs `A/B/C/D` across `slx`, `slx-l`, `b1`, `b2`, `b3`, `b4` and writes logs to a new timestamped directory under `results/`.

Quick (smoke) matrix:

```bash
WORKLOADS="A B C D" BASELINES="slx slx-l b1 b2 b3 b4" DURATION="5s" OPS_PER_SEC="100" ./scripts/run-baselines.sh
```

More stable (recommended) matrix:

```bash
WORKLOADS="A B C D" BASELINES="slx slx-l b1 b2 b3 b4" DURATION="15s" OPS_PER_SEC="100" ./scripts/run-baselines.sh
```

## LLM Mode (SLX-L)

To enable real OpenAI calls in SLX-L (`rebase+llm`), set API key in shell:

```bash
export OPENAI_API_KEY="..."
./scripts/run-e2e.sh --workload B --baseline slx-l --duration 5s --ops-per-sec 100
```

## Interpreting Results

- Compare `slx` / `slx-l` vs `b1` / `b2` / `b3` using:
  - `summary.finalized_per_sec`
  - `summary.p95_finalized`
  - `arbitration.cert_per_sec`
  - `arbitration.auto_merge_rate`
- Keep `verify_passed=true` as a hard validity condition.

## Quick Regression Test

```bash
./scripts/run-test.sh
```
