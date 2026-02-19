# Harness Testing Guide

This document describes how to run workload harness tests for the multi-master Redpanda prototype.

## Prerequisites

- Go toolchain installed (`go`)
- Protobuf tooling installed (`protoc`, `protoc-gen-go`)
- Redpanda + `rpk` installed
- Repository dependencies resolved (`go mod tidy`)

## Fast Path (Recommended)

Use the automated end-to-end script:

```bash
./scripts/run-e2e.sh
```

What it does:

1. Starts Redpanda if not already running.
2. Deletes and recreates topics (`ingest.A`, `ingest.B`, `arb.cert`, `arb.final`).
3. Resets known consumer groups.
4. Starts services:
   - `cmd/ingest` region A/B
   - `cmd/applier` region A/B
   - `cmd/finalizer`
5. Waits for service health checks.
6. Runs harness (`cmd/harness`) and requires `"verify_passed": true`.
7. Cleans up launched processes on exit.

## Useful Script Options

```bash
./scripts/run-e2e.sh \
  --workload B \
  --duration 30s \
  --ops-per-sec 100 \
  --keyspace-size 1000 \
  --zipf-theta 0.8 \
  --shared-hot-fraction 0.05 \
  --disjoint-field-prob 0.8
```

## Manual Flow (Debug-Friendly)

If you want to inspect each component manually:

1. Start Redpanda and create topics:

```bash
./scripts/setup-redpanda.sh
./scripts/create-topics.sh
```

2. Start services in separate terminals:

```bash
go run ./cmd/ingest --region=A --port=8181 --broker=127.0.0.1:9092
go run ./cmd/ingest --region=B --port=8182 --broker=127.0.0.1:9092
go run ./cmd/applier --region=A --port=8091 --broker=127.0.0.1:9092
go run ./cmd/applier --region=B --port=8092 --broker=127.0.0.1:9092
go run ./cmd/finalizer --broker=127.0.0.1:9092
```

3. Run harness:

```bash
go run ./cmd/harness \
  --workload=B \
  --duration=30s \
  --ops-per-sec=100 \
  --ingest-a-url=http://127.0.0.1:8181 \
  --ingest-b-url=http://127.0.0.1:8182 \
  --applier-a-url=http://127.0.0.1:8091 \
  --applier-b-url=http://127.0.0.1:8092
```

4. Validate convergence:

```bash
curl -s http://127.0.0.1:8091/hash
curl -s http://127.0.0.1:8092/hash
```

The hashes must match, and harness output should include:

```json
"verify_passed": true
```

## Notes

- In this environment, Redpanda binds `8081/8082`, so ingest services use `8181/8182`.
- For quick local regression checks, you can also run:

```bash
./scripts/run-test.sh
```

This executes `go test ./...`.
