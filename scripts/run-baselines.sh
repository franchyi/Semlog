#!/usr/bin/env bash
set -euo pipefail

WORKLOADS="${WORKLOADS:-A B C D}"
BASELINES="${BASELINES:-full b1 b2 b3 b4}"
DURATION="${DURATION:-60s}"
OPS_PER_SEC="${OPS_PER_SEC:-500}"
KEYSPACE_SIZE="${KEYSPACE_SIZE:-1000}"
ZIPF_THETA="${ZIPF_THETA:-0.8}"
SHARED_HOT_FRACTION="${SHARED_HOT_FRACTION:-0.05}"
DISJOINT_FIELD_PROB="${DISJOINT_FIELD_PROB:-0.8}"
CROSS_REGION_LATENCY_MS="${CROSS_REGION_LATENCY_MS:-80}"

OUTPUT_DIR="results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUTPUT_DIR}"

echo "Writing baseline results to ${OUTPUT_DIR}"

for workload in ${WORKLOADS}; do
  for baseline in ${BASELINES}; do
    echo "=== Workload ${workload}, Baseline ${baseline} ==="

    set +e
    ./scripts/run-e2e.sh \
      --workload "${workload}" \
      --baseline "${baseline}" \
      --duration "${DURATION}" \
      --ops-per-sec "${OPS_PER_SEC}" \
      --keyspace-size "${KEYSPACE_SIZE}" \
      --zipf-theta "${ZIPF_THETA}" \
      --shared-hot-fraction "${SHARED_HOT_FRACTION}" \
      --disjoint-field-prob "${DISJOINT_FIELD_PROB}" \
      --cross-region-latency-ms "${CROSS_REGION_LATENCY_MS}" \
      2>&1 | tee "${OUTPUT_DIR}/w${workload}_${baseline}.log"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ ${rc} -ne 0 ]]; then
      echo "Run failed for workload=${workload} baseline=${baseline}" >&2
      exit ${rc}
    fi

    sleep 3
  done
done

echo "All baseline runs completed: ${OUTPUT_DIR}"
