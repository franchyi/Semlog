#!/usr/bin/env bash
set -euo pipefail

BROKER="127.0.0.1:9092"
WORKLOAD="B"
BASELINE="full"
DURATION="30s"
OPS_PER_SEC="100"
KEYSPACE_SIZE="1000"
ZIPF_THETA="0.8"
SHARED_HOT_FRACTION="0.05"
DISJOINT_FIELD_PROB="0.8"
CROSS_REGION_LATENCY_MS="80"
INGEST_A_PORT="8181"
INGEST_B_PORT="8182"
APPLIER_A_PORT="8091"
APPLIER_B_PORT="8092"
APPLIER_CLASSIFY_MODE="structural"
FINALIZER_REBASE_MODE="rebase"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --broker <host:port>           Kafka broker (default: ${BROKER})
  --workload <A|B|C|D|E>         Workload ID (default: ${WORKLOAD})
  --baseline <full|b1|b2|b3|b4>  Baseline mode (default: ${BASELINE})
  --duration <Go duration>        Harness duration (default: ${DURATION})
  --ops-per-sec <int>            Per-region OPS (default: ${OPS_PER_SEC})
  --keyspace-size <int>          Keyspace size (default: ${KEYSPACE_SIZE})
  --zipf-theta <float>           Zipf theta (default: ${ZIPF_THETA})
  --shared-hot-fraction <float>  Shared hot fraction (default: ${SHARED_HOT_FRACTION})
  --disjoint-field-prob <float>  Disjoint field probability (default: ${DISJOINT_FIELD_PROB})
  --cross-region-latency-ms <int> Simulated latency for B4 region-B writes (default: ${CROSS_REGION_LATENCY_MS})
  -h, --help                     Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker)
      BROKER="$2"
      shift 2
      ;;
    --workload)
      WORKLOAD="$2"
      shift 2
      ;;
    --baseline)
      BASELINE="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --ops-per-sec)
      OPS_PER_SEC="$2"
      shift 2
      ;;
    --keyspace-size)
      KEYSPACE_SIZE="$2"
      shift 2
      ;;
    --zipf-theta)
      ZIPF_THETA="$2"
      shift 2
      ;;
    --shared-hot-fraction)
      SHARED_HOT_FRACTION="$2"
      shift 2
      ;;
    --disjoint-field-prob)
      DISJOINT_FIELD_PROB="$2"
      shift 2
      ;;
    --cross-region-latency-ms)
      CROSS_REGION_LATENCY_MS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

case "${BASELINE}" in
  full)
    APPLIER_CLASSIFY_MODE="structural"
    FINALIZER_REBASE_MODE="rebase"
    ;;
  b1)
    APPLIER_CLASSIFY_MODE="naive"
    FINALIZER_REBASE_MODE="rebase"
    ;;
  b2)
    APPLIER_CLASSIFY_MODE="structural"
    FINALIZER_REBASE_MODE="lww"
    ;;
  b3)
    APPLIER_CLASSIFY_MODE="naive"
    FINALIZER_REBASE_MODE="lww"
    ;;
  b4)
    APPLIER_CLASSIFY_MODE="structural"
    FINALIZER_REBASE_MODE="rebase"
    ;;
  *)
    echo "Invalid baseline: ${BASELINE}. Must be one of: full|b1|b2|b3|b4" >&2
    exit 1
    ;;
esac

if ! command -v go >/dev/null 2>&1; then
  echo "go is required" >&2
  exit 1
fi
if ! command -v rpk >/dev/null 2>&1; then
  echo "rpk is required" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
PIDS=()
SERVICE_PATTERN="/tmp/go-build.*/exe/(ingest|applier|finalizer)|go run ./cmd/(ingest|applier|finalizer)"

stop_service_processes() {
  pkill -INT -f "${SERVICE_PATTERN}" >/dev/null 2>&1 || true
  sleep 1
  pkill -TERM -f "${SERVICE_PATTERN}" >/dev/null 2>&1 || true
  sleep 1
}

cleanup() {
  local ec=$?
  set +e

  for pid in "${PIDS[@]}"; do
    kill -INT "$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  stop_service_processes
  rm -rf "${TMP_DIR}"
  exit "$ec"
}
trap cleanup EXIT INT TERM

# Ensure no leftover service binaries are running from prior runs.
stop_service_processes

start_redpanda() {
  if rpk cluster info --brokers "${BROKER}" >/dev/null 2>&1; then
    echo "Redpanda is already running at ${BROKER}"
    return
  fi

  echo "Starting Redpanda..."
  sudo systemctl start redpanda || sudo rpk redpanda start --mode development --check=false

  for _ in $(seq 1 30); do
    if rpk cluster info --brokers "${BROKER}" >/dev/null 2>&1; then
      echo "Redpanda is ready"
      return
    fi
    sleep 1
  done

  echo "Redpanda did not become ready in time" >&2
  exit 1
}

reset_kafka_state() {
  local max_attempts=10
  local attempt=1

  while [[ ${attempt} -le ${max_attempts} ]]; do
    echo "Resetting topics (attempt ${attempt}/${max_attempts})..."
    rpk topic delete ingest.A ingest.B arb.cert arb.final --brokers "${BROKER}" >/dev/null 2>&1 || true

    local deleted=false
    for _ in $(seq 1 90); do
      if ! rpk topic list --brokers "${BROKER}" | rg -q '^(ingest.A|ingest.B|arb.cert|arb.final)\b'; then
        deleted=true
        break
      fi
      sleep 1
    done

    if [[ "${deleted}" != "true" ]]; then
      echo "Topics were not fully deleted yet; retrying..." >&2
      attempt=$((attempt + 1))
      continue
    fi

    local create_ok=true
    rpk topic create ingest.A --partitions 16 --replicas 1 --brokers "${BROKER}" >/dev/null || create_ok=false
    rpk topic create ingest.B --partitions 16 --replicas 1 --brokers "${BROKER}" >/dev/null || create_ok=false
    rpk topic create arb.cert --partitions 4 --replicas 1 --brokers "${BROKER}" >/dev/null || create_ok=false
    rpk topic create arb.final --partitions 4 --replicas 1 --brokers "${BROKER}" >/dev/null || create_ok=false

    if [[ "${create_ok}" != "true" ]]; then
      echo "Topic creation race detected; retrying reset..." >&2
      sleep 2
      attempt=$((attempt + 1))
      continue
    fi

    local ok=true
    local p_ingest_a p_ingest_b p_arb_cert p_arb_final
    p_ingest_a="$(rpk topic list --brokers "${BROKER}" | awk '$1=="ingest.A"{print $2}')"
    p_ingest_b="$(rpk topic list --brokers "${BROKER}" | awk '$1=="ingest.B"{print $2}')"
    p_arb_cert="$(rpk topic list --brokers "${BROKER}" | awk '$1=="arb.cert"{print $2}')"
    p_arb_final="$(rpk topic list --brokers "${BROKER}" | awk '$1=="arb.final"{print $2}')"

    [[ "${p_ingest_a}" == "16" ]] || ok=false
    [[ "${p_ingest_b}" == "16" ]] || ok=false
    [[ "${p_arb_cert}" == "4" ]] || ok=false
    [[ "${p_arb_final}" == "4" ]] || ok=false

    if [[ "${ok}" == "true" ]]; then
      echo "Topics reset with expected partition counts"
      break
    fi

    echo "Topic partition verification failed; retrying reset..." >&2
    sleep 2
    attempt=$((attempt + 1))
  done

  if [[ ${attempt} -gt ${max_attempts} ]]; then
    echo "Failed to reset topics with expected partition counts" >&2
    exit 1
  fi

  echo "Resetting consumer groups..."
  rpk group delete \
    applier-A-ingest-A applier-A-ingest-B applier-A-arb-final \
    applier-B-ingest-A applier-B-ingest-B applier-B-arb-final \
    finalizer finalizer-arb-final \
    --brokers "${BROKER}" >/dev/null 2>&1 || true
}

start_service() {
  local name="$1"
  shift
  local log_file="${TMP_DIR}/${name}.log"

  echo "Starting ${name}..."
  (
    GOCACHE=/tmp/go-build \
    GOPATH=/tmp/go \
    PATH="${PATH}:${HOME}/go/bin" \
      "$@"
  ) >"${log_file}" 2>&1 &

  local pid=$!
  PIDS+=("${pid}")
  echo "${name} started (pid=${pid}, log=${log_file})"
}

wait_health() {
  local url="$1"
  local name="$2"

  for _ in $(seq 1 60); do
    if curl -sf "${url}" >/dev/null; then
      echo "${name} is healthy"
      return
    fi
    sleep 1
  done

  echo "${name} health check failed: ${url}" >&2
  for file in "${TMP_DIR}"/*.log; do
    echo "----- ${file} -----" >&2
    tail -n 200 "${file}" >&2 || true
  done
  exit 1
}

run_harness() {
  local ingest_a_url="http://127.0.0.1:${INGEST_A_PORT}"
  local ingest_b_url="http://127.0.0.1:${INGEST_B_PORT}"
  local applier_a_url="http://127.0.0.1:${APPLIER_A_PORT}"
  local applier_b_url="http://127.0.0.1:${APPLIER_B_PORT}"

  echo "Running harness..."
  local output
  set +e
  output=$(GOCACHE=/tmp/go-build GOPATH=/tmp/go PATH="${PATH}:${HOME}/go/bin" \
    go run ./cmd/harness \
      --workload="${WORKLOAD}" \
      --baseline="${BASELINE}" \
      --duration="${DURATION}" \
      --ops-per-sec="${OPS_PER_SEC}" \
      --keyspace-size="${KEYSPACE_SIZE}" \
      --zipf-theta="${ZIPF_THETA}" \
      --shared-hot-fraction="${SHARED_HOT_FRACTION}" \
      --disjoint-field-prob="${DISJOINT_FIELD_PROB}" \
      --cross-region-latency-ms="${CROSS_REGION_LATENCY_MS}" \
      --ingest-a-url="${ingest_a_url}" \
      --ingest-b-url="${ingest_b_url}" \
      --applier-a-url="${applier_a_url}" \
      --applier-b-url="${applier_b_url}" 2>&1)
  local rc=$?
  set -e

  echo "${output}"

  if [[ ${rc} -ne 0 ]]; then
    echo "Harness execution failed" >&2
    return ${rc}
  fi

  if ! printf '%s' "${output}" | rg -q '"verify_passed": true'; then
    echo "Harness verification failed" >&2
    return 1
  fi

  echo "E2E run passed"
}

start_redpanda
reset_kafka_state

start_service ingest-A go run ./cmd/ingest --region=A --port="${INGEST_A_PORT}" --broker="${BROKER}"
start_service ingest-B go run ./cmd/ingest --region=B --port="${INGEST_B_PORT}" --broker="${BROKER}"
start_service applier-A go run ./cmd/applier --region=A --port="${APPLIER_A_PORT}" --broker="${BROKER}" --classify-mode="${APPLIER_CLASSIFY_MODE}"
start_service applier-B go run ./cmd/applier --region=B --port="${APPLIER_B_PORT}" --broker="${BROKER}" --classify-mode="${APPLIER_CLASSIFY_MODE}"
start_service finalizer go run ./cmd/finalizer --broker="${BROKER}" --rebase-mode="${FINALIZER_REBASE_MODE}"

wait_health "http://127.0.0.1:${INGEST_A_PORT}/healthz" "ingest-A"
wait_health "http://127.0.0.1:${INGEST_B_PORT}/healthz" "ingest-B"
wait_health "http://127.0.0.1:${APPLIER_A_PORT}/healthz" "applier-A"
wait_health "http://127.0.0.1:${APPLIER_B_PORT}/healthz" "applier-B"

run_harness
