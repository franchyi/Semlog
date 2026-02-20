# Harness Testing Guide

This document describes how to run workload harness tests and baseline comparisons.

## Prerequisites

- Go installed
- Redpanda + `rpk` installed and reachable at `127.0.0.1:9092`
- `protoc` + `protoc-gen-go` installed (only needed when proto changes)

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

## Run One Baseline

```bash
./scripts/run-e2e.sh \
  --workload B \
  --baseline full \
  --duration 30s \
  --ops-per-sec 100 \
  --shared-hot-fraction 0.05
```

Notes:

- `full` currently maps to `classify-mode=structural` + `rebase-mode=rebase+llm`.
- If `OPENAI_API_KEY` is not set, finalizer logs fallback and behaves as deterministic `rebase`.

## Run All Baselines (Matrix)

Standard matrix script:

```bash
WORKLOADS="A B C D" BASELINES="full b1 b2 b3 b4" DURATION="30s" OPS_PER_SEC="100" ./scripts/run-baselines.sh
```

Per-workload matrix loop (same script pattern used in recent comparisons):

```bash
for b in full b1 b2 b3 b4; do
  ./scripts/run-e2e.sh --workload B --baseline "$b" --duration 5s --ops-per-sec 100 --shared-hot-fraction 0.05
done
```

## LLM Mode (FULL)

To enable real OpenAI calls in FULL (`rebase+llm`), set API key in shell:

```bash
export OPENAI_API_KEY="..."
./scripts/run-e2e.sh --workload B --baseline full --duration 5s --ops-per-sec 100
```

## Interpreting Results

- Compare FULL vs B1/B2/B3 using:
  - `summary.finalized_per_sec`
  - `summary.p95_finalized`
  - `arbitration.cert_per_sec`
  - `arbitration.auto_merge_rate`
- Keep `verify_passed=true` as a hard validity condition.

## Quick Regression Test

```bash
./scripts/run-test.sh
```
