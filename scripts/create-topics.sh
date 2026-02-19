#!/bin/bash
set -euo pipefail

BROKER="127.0.0.1:9092"

rpk topic create ingest.A  --partitions 16 --replicas 1 --brokers "$BROKER" || true
rpk topic create ingest.B  --partitions 16 --replicas 1 --brokers "$BROKER" || true
rpk topic create arb.cert  --partitions 4  --replicas 1 --brokers "$BROKER" || true
rpk topic create arb.final --partitions 4  --replicas 1 --brokers "$BROKER" || true

echo "Topics:"
rpk topic list --brokers "$BROKER"
