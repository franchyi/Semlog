# CLAUDE.md — Multi-Master Shared Log on Redpanda

## Project: Conflict-Selective Finalization with Semantic Merge and Deterministic Rebase

### One-line goal

Build a research prototype where **both regions accept writes concurrently** (multi-master) by appending to **region-local Redpanda topics**, and only **true conflicts** are escalated to an **arbitration topic** that emits deterministic **Finalize** records, including **semantic merge** (disjoint fields / commutative deltas) and **semantic rebase** (deterministic re-exec) when safe.

### Language & Stack

- **Go** for all components
- `segmentio/kafka-go` for Kafka client
- `google.golang.org/protobuf` for protobuf
- `net/http` for HTTP API (stdlib)
- In-memory maps for MVP stable store (no RocksDB)
- Redpanda installed natively (single broker, no Docker)

---

## 1) Repository Layout

```
redpanda-mm/
├── CLAUDE.md                  # for claude code
├── AGENTS.md                  # for codex
├── baseline.md                # for baseline description
├── scripts/
│   ├── setup-redpanda.sh      # Install + start Redpanda (single broker)
│   ├── create-topics.sh       # Creates all Kafka topics
│   └── run-test.sh            # Runs a workload + verification
├── proto/
│   └── records.proto          # Protobuf definitions
├── gen/                       # Generated protobuf Go code (do not edit)
│   └── proto/
├── cmd/
│   ├── ingest/
│   │   └── main.go            # Region Ingest Service binary
│   ├── applier/
│   │   └── main.go            # Region Applier binary
│   ├── finalizer/
│   │   └── main.go            # Finalizer Service binary
│   └── harness/
│       └── main.go            # Workload harness binary
├── pkg/
│   ├── hlc/
│   │   └── hlc.go             # Hybrid Logical Clock
│   ├── engine/
│   │   ├── apply.go           # Per-op-type Apply function (PURE, shared by applier + finalizer)
│   │   └── diff.go            # Compute net-effect patch between two JSON states
│   ├── ingest/
│   │   └── service.go         # Ingest HTTP handler + Kafka producer
│   ├── applier/
│   │   ├── applier.go         # Main applier loop (consume + classify + merge/escalate)
│   │   ├── store.go           # Stable store (in-memory map)
│   │   ├── pending.go         # Pending window per key
│   │   ├── classify.go        # Conflict classification (DP1)
│   │   ├── merge.go           # Auto-merge logic
│   │   ├── invariants.go      # Invariant checker
│   │   ├── finalize_apply.go  # Apply arb.final records to stable store
│   │   └── api.go             # GET /stable, GET /status handlers
│   ├── finalizer/
│   │   ├── finalizer.go       # Main finalizer loop (consume certs + rebase)
│   │   ├── rebase.go          # Rebase engine (re-execute ops in order)
│   │   └── order.go           # Deterministic ordering comparator
│   ├── kafka/
│   │   ├── producer.go        # Shared Kafka producer wrapper
│   │   ├── consumer.go        # Shared Kafka consumer wrapper
│   │   └── seek.go            # Seek-and-read for fetching ops by offset
│   ├── jsonutil/
│   │   ├── path.go            # JSON path get/set/overlap operations
│   │   └── hash.go            # Deterministic JSON hashing
│   └── workload/
│       ├── harness.go         # Common workload harness
│       ├── keypicker.go       # Zipf + hot-set key picker
│       ├── workload_a.go      # Geo-YCSB++
│       ├── workload_b.go      # Profile Patch
│       ├── workload_c.go      # Task Queue
│       ├── workload_d.go      # Inventory
│       ├── workload_e.go      # Downstream Compute
│       ├── verify.go          # Post-run verification assertions
│       └── metrics.go         # Latency histograms, throughput counters
├── go.mod
└── go.sum
```

---

## 2) System Architecture

### Components

1. **Region Ingest Service** (one per region: A and B)
   - HTTP endpoint `POST /write`
   - Appends Accepted record to `ingest.{region}` with key-based partitioning
   - Returns receipt `{op_id, region, topic, partition, offset}`
   - Does NOT check invariants or detect conflicts

2. **Region Applier** (one per region)
   - Consumes `ingest.A`, `ingest.B`, and `arb.final`
   - Maintains:
     - **Stable store**: finalized-only KV (`map[string]json.RawMessage` with `sync.RWMutex`). **Modified ONLY by arb.final consumer.**
     - **Pending window**: per-key list of recent Accepted ops not yet finalized
     - **Status index**: `map[op_id] → {ACCEPTED | FINALIZED(outcome)}`
     - **Applied conflicts set**: `map[conflict_id]bool` for idempotent finalize
   - On each new Accepted op: classify vs pending ops from other region on same key
     - MERGEABLE → designated applier produces FinalizeRecord (type=MERGE) to `arb.final`
     - CONFLICTING → emit ConflictCertificate to `arb.cert`
   - On each `arb.final` record: apply patches to stable store (idempotent, dedupe by conflict_id)
   - Exposes `GET /stable`, `GET /status`, `GET /hash` for reads

3. **Finalizer Service**
   - Consumes `arb.cert` (consumer group "finalizer")
   - Fetches contender Accepted records from ingest topics by (topic, partition, offset)
   - Decides deterministic order, re-executes ops (rebase), computes net-effect patches
   - Produces FinalizeRecord (type=FINALIZE_REBASE) to `arb.final`

4. **Workload Harness**
   - Two producer goroutines (Region A, Region B)
   - Configurable knobs (keyspace, skew, op mix, conflict rate)
   - Metrics collection and post-run verification assertions

### Data Flow

```
Client A → Ingest A → ingest.A ─┐
                                 ├─→ Applier A ─── classifies conflicts
Client B → Ingest B → ingest.B ─┤                      │
                                 ├─→ Applier B ─── classifies conflicts
                                 │                      │
                                 │            ┌─────────┴─────────┐
                                 │            │                   │
                                 │        MERGEABLE           CONFLICTING
                                 │            │                   │
                                 │   designated applier      arb.cert
                                 │   produces merge           │
                                 │   FinalizeRecord           ▼
                                 │            │           Finalizer
                                 │            │        (deterministic rebase)
                                 │            │               │
                                 │            └───┬───────────┘
                                 │                │
                                 │            arb.final
                                 │          (ALL outcomes logged here)
                                 │                │
                                 │        ┌───────┴───────┐
                                 │        ▼               ▼
                                 │    Applier A       Applier B
                                 │  (apply patches)  (apply patches)
                                 │        ▼               ▼
                                 └── stable store A   stable store B
                                     (must converge)
```

**Key invariant**: Stable store is ONLY modified by consuming `arb.final`. All paths (merge and rebase) converge through this single log.

---

## 3) Topic Layout

| Topic | Partitions | RF | Partition Key | Purpose |
|-------|-----------|-----|--------------|---------|
| `ingest.A` | N=16 | 1 | `key` hash | Region A accepted ops |
| `ingest.B` | N=16 | 1 | `key` hash | Region B accepted ops |
| `arb.cert` | M=4 | 1 | `key` hash | Conflict certificates |
| `arb.final` | M=4 | 1 | `key` hash | Finalization outcomes |

RF=1 because we run a single broker for dev. Raise to 3 with 3 brokers for production/stress testing.

`arb.cert` and `arb.final` MUST use the same partitioning function so all events for a key land in the same partition. All producers use the record's `key` field as the Kafka message key.

---

## 4) Record Formats (Protobuf)

### File: `proto/records.proto`

```protobuf
syntax = "proto3";
package redpanda_mm;
option go_package = "github.com/yourorg/redpanda-mm/gen/proto";

// === Enums ===

enum OpType {
  OP_UNKNOWN = 0;
  OP_PUT = 1;
  OP_PATCH = 2;
  OP_INC = 3;
  OP_PUT_IF_ABSENT = 4;
  OP_CAS = 5;
  OP_RESERVE = 6;
  OP_CANCEL = 7;
  OP_CLAIM = 8;
  OP_COMPLETE = 9;
}

enum OutcomeType {
  OUTCOME_UNKNOWN = 0;
  OUTCOME_COMMIT_PATCH = 1;   // op succeeded; apply patch_json (net-effect delta)
  OUTCOME_NOOP = 2;           // op skipped (already satisfied)
  OUTCOME_FAIL = 3;           // op rejected (precondition/invariant)
  OUTCOME_TRANSFORM = 4;      // op replaced with a different op
}

enum ConflictReason {
  REASON_UNKNOWN = 0;
  REASON_OVERLAP_WRITESET = 1;
  REASON_SAME_FIELD = 2;
  REASON_PRECOND_DEP = 3;
}

// === HLC ===

message HLC {
  int64 physical_ms = 1;
  uint32 logical = 2;
}

// === Accepted Record ===
// Produced by Ingest Service to ingest.A / ingest.B

message AcceptedRecord {
  string op_id = 1;           // UUID v4
  string region = 2;          // "A" or "B"
  string key = 3;
  OpType op_type = 4;
  bytes args_json = 5;        // JSON-encoded type-specific args
  repeated string write_set = 6;  // JSON paths; PUT uses ["*"]
  HLC hlc = 7;
  int64 ingest_ts_ms = 8;
  string client_id = 9;       // optional
  string session_id = 10;     // optional
}

// Type-specific args (JSON-encoded in args_json):
//   PUT:            {"value": <any>}
//   PATCH:          {"patches": [{"path":"/x","value":"y"},...]}
//   INC:            {"path":"/stats/views","delta":1}
//   PUT_IF_ABSENT:  {"value": <any>}
//   CAS:            {"path":"/ver","expected":5,"new_value":6}
//   RESERVE:        {"n": 10}
//   CANCEL:         {"n": 5}
//   CLAIM:          {"worker_id":"w1"}
//   COMPLETE:       {"worker_id":"w1"}

// === Conflict Certificate ===
// Produced by Applier to arb.cert

message OpRef {
  string region = 1;
  string topic = 2;
  int32 partition = 3;
  int64 offset = 4;
  string op_id = 5;
  HLC hlc = 6;
}

message ConflictCertificate {
  string conflict_id = 1;     // hash of (key, sorted contender op_ids)
  string key = 2;
  repeated OpRef contenders = 3;
  ConflictReason reason = 4;
  bytes stable_version_hash = 5;  // optional
}

// === Finalize Record ===
// Produced by Finalizer to arb.final (for CONFLICTING ops)
// Also produced by Applier to arb.final (for MERGEABLE ops — fast-path finalization)
// ALL merge/conflict outcomes are logged here. Appliers never finalize locally.

enum FinalizeType {
  FINALIZE_UNKNOWN = 0;
  FINALIZE_MERGE = 1;      // fast-path: applier detected mergeable ops
  FINALIZE_REBASE = 2;     // slow-path: finalizer resolved via rebase
}

message OpOutcome {
  string op_id = 1;
  OutcomeType outcome = 2;
  string reason = 3;          // for NOOP/FAIL
  bytes patch_json = 4;       // REQUIRED for OUTCOME_PATCH: net-effect delta to apply
  bytes transform_json = 5;   // for OUTCOME_TRANSFORM: replacement op
}

message FinalizeRecord {
  string conflict_id = 1;
  string key = 2;
  repeated string decided_order = 3;  // op_ids in decided order
  repeated OpOutcome outcomes = 4;
  bytes final_state_hash = 5;         // hash of state AFTER applying all outcomes
  bytes verifier_digest = 6;          // hash(base_state, ops, outcomes) for verification
  bytes base_state_hash = 7;          // hash of stable state BEFORE this finalization
  int64 created_ts_ms = 8;
  FinalizeType finalize_type = 9;     // MERGE or REBASE
  string producer_region = 10;        // which region/service produced this record
}
```

---

## 5) Client-Facing API

### Ingest Service: `POST /write`

```
POST /write
Content-Type: application/json

{
  "key": "user:alice",
  "op_type": "PATCH",
  "args": {"patches": [{"path": "/email", "value": "alice@new.com"}]},
  "write_set": ["/email"],
  "client_id": "client-1"
}

Response 200:
{
  "op_id": "uuid-...",
  "region": "A",
  "topic": "ingest.A",
  "partition": 3,
  "offset": 1042,
  "hlc": {"physical_ms": 1700000000000, "logical": 0}
}
```

### Applier Query API

```
GET /stable?key=user:alice      → returns finalized JSON value (or 404)
GET /status?op_id=uuid-...      → returns {"status": "ACCEPTED|FINALIZED", "outcome": "COMMIT_PATCH|NOOP|FAIL"}
GET /hash                       → returns stable store checksum (for verification)
```

---

## 6) Conflict Detection & Auto-Merge (DP1)

### Pending Window

Applier maintains per key:

```go
type PendingEntry struct {
    Op     *AcceptedRecord
    Offset int64
}
// pendingWindow map[string][]PendingEntry  // key → pending ops
```

When a new Accepted op arrives:
1. Add to pending window for this key.
2. Look up pending ops from the *other* region for the same key.
3. For each concurrent pair, classify.

### Classification Rules

```
classify(op1, op2, stableState) → MERGEABLE | CONFLICTING

NOTE: stableState must be the state AFTER all applied FinalizeRecords so far.
Never use tentative/overlay state. The pending window is for tracking op metadata
only — pending ops are NOT reflected in stableState.

1. If either write_set contains "*" → CONFLICTING
   (PUT overwrites everything)

2. If both are INC on the same path → MERGEABLE
   (commutative: sum deltas)

3. If write_sets are disjoint (no path prefix overlap) → MERGEABLE
   (apply both independently)

4. If one is PUT_IF_ABSENT and key exists in stable store → MERGEABLE
   (the one on existing key becomes NOOP)

5. Otherwise → CONFLICTING
```

### Path Overlap Logic

Two JSON paths overlap if one is a **segment prefix** of the other (NOT string prefix):
- `/addr` and `/addr/city` → overlap (`/addr` is a segment prefix of `/addr/city`)
- `/addr/city` and `/addr/zip` → no overlap (neither is a prefix of the other)
- `/email` and `/phone` → no overlap
- `/ad` and `/addr` → **no overlap** (string prefix, but NOT segment prefix)
- `*` overlaps everything

Implementation: split paths by `/` into segments, check if one segment list is a prefix of the other.

```go
// pkg/jsonutil/path.go

// PathsOverlap returns true if one path is a segment-prefix of the other.
// Splits by "/" and checks if one segment sequence is a prefix of the other.
// Special: "*" overlaps everything.
// Note: For MVP, we ignore RFC 6901 escaping (~0, ~1). Keys containing "/" or "~"
// in field names are not supported. This is acceptable for the research prototype.
func PathsOverlap(p1, p2 string) bool

// WriteSetsDisjoint returns true if no path in ws1 overlaps any path in ws2.
func WriteSetsDisjoint(ws1, ws2 []string) bool
```

### After Classification

- **MERGEABLE**: compute the merged state and net-effect patches, run invariant check. If invariants pass → produce a **FinalizeRecord** (type=FINALIZE_MERGE) to `arb.final` with per-op COMMIT_PATCH outcomes containing the net-effect deltas. **Do NOT update stable store locally** — wait for the FinalizeRecord to come back via the `arb.final` consumer, just like arbitrated conflicts. If invariants fail → treat as CONFLICTING.
- **CONFLICTING**: emit ConflictCertificate to `arb.cert`.

**Why merge decisions are logged**: If appliers finalized merges locally without logging, different appliers could reach different states due to processing order differences. By routing all finalization through `arb.final`, every applier applies the same sequence of authoritative records and convergence is guaranteed.

**Who produces merge FinalizeRecords**: Only **one** applier should produce the merge FinalizeRecord for a given key to avoid duplicates. Rule: **the applier whose region matches the first op in the deterministic order (by HLC) produces the record**. The other applier sees the same merge opportunity but waits for the FinalizeRecord from `arb.final` instead of producing one. Both appliers can independently compute the same deterministic order to agree on who produces.

### Auto-Merge Logic

```go
// pkg/applier/merge.go

// ComputeMerge computes the merged state and per-op net-effect patches for MERGEABLE ops.
// Does NOT modify stable store. Returns a FinalizeRecord to be produced to arb.final.
//
// Steps:
// 1. Sort ops by deterministic order (same comparator as finalizer)
// 2. Starting from current stable state, apply each op
// 3. For each op, compute patch_json = diff(state_before_op, state_after_op)
// 4. Run invariant check on final state
// 5. Build FinalizeRecord with FINALIZE_MERGE type and COMMIT_PATCH outcomes
//
// Returns (FinalizeRecord, error). Error if invariants fail.
func ComputeMerge(ops []*AcceptedRecord, stableState json.RawMessage, key string) (*FinalizeRecord, error)
```

---

## 7) Arbitration & Rebase (DP2)

### Deterministic Ordering

```go
// pkg/finalizer/order.go

// CompareOps returns -1, 0, or 1 for deterministic total order.
// Compare by: (hlc.physical_ms, hlc.logical, region, op_id)
// op_id breaks all ties (UUIDs are unique).
func CompareOps(a, b *OpRef) int
```

### Rebase Engine

```
For each ConflictCertificate:
  1. Fetch all contender AcceptedRecords from ingest topics
     (seek to topic/partition/offset and read)

  2. Sort contenders by CompareOps → decided_order

  3. Load stable state S for the key. Record base_state_hash = hash(S).

  4. For each op in decided_order:
     S_before = S
     (S', outcome) = Apply(op, S)
     if outcome == OK and invariants(S'):
       patch = diff(S_before, S')    // net-effect delta
       S = S'
       record COMMIT_PATCH with patch_json = patch
     else if outcome == NOOP:
       record NOOP(reason)
     else:
       record FAIL(reason)
       // S unchanged

  5. Produce FinalizeRecord to arb.final with:
     finalize_type = FINALIZE_REBASE
     decided_order, per-op outcomes (each COMMIT_PATCH carries its patch_json)
     base_state_hash, final_state_hash = hash(S), verifier_digest
```

**Key design: Finalizer emits patches, not "replay instructions".** Every COMMIT_PATCH outcome includes `patch_json` — the net-effect delta that transforms state. Appliers never re-execute original operations from FinalizeRecords. They only apply patches. This eliminates the risk of divergence from re-execution against different base states.

### Apply Function (PURE — shared by Applier and Finalizer)

```go
// pkg/engine/apply.go

// Apply executes a single operation against state.
// This function is PURE — no side effects, deterministic.
// Returns (newState, outcome).
func Apply(op *AcceptedRecord, state json.RawMessage) (json.RawMessage, Outcome)
```

### Diff Function (computes net-effect patches)

```go
// pkg/engine/diff.go

// Diff computes a JSON merge-patch (RFC 7396) that transforms `before` into `after`.
// The result is a JSON object containing only the fields that changed, with their
// new absolute values.
//
// Example:
//   before: {"email":"old@x.com", "phone":"555", "stats":{"views":10}}
//   after:  {"email":"new@x.com", "phone":"555", "stats":{"views":15}}
//   diff:   {"email":"new@x.com", "stats":{"views":15}}
//
// This function is PURE and deterministic.
func Diff(before, after json.RawMessage) json.RawMessage
```

**Diff is used by both ComputeMerge (applier) and Rebase (finalizer)** to generate the `patch_json` in each OpOutcome.

Per op type:

```
PUT:
  S[key] = args.value → OK

PATCH:
  for each patch in args.patches:
    set S[key][patch.path] = patch.value
  if any path invalid → FAIL("invalid path")
  else → OK

INC:
  val = get_path(S[key], args.path)
  if not numeric → FAIL("not numeric")
  S[key][args.path] = val + args.delta → OK

PUT_IF_ABSENT:
  if key exists → NOOP("key already exists")
  else S[key] = args.value → OK

CAS:
  val = get_path(S[key], args.path)
  if val == args.expected:
    S[key][args.path] = args.new_value → OK
  else → FAIL("CAS mismatch")

RESERVE:
  if S[key].stock - S[key].reserved >= args.n:
    S[key].reserved += args.n → OK
  else → FAIL("insufficient stock")

CANCEL:
  if S[key].reserved >= args.n:
    S[key].reserved -= args.n → OK
  else → FAIL("insufficient reserved")

CLAIM:
  if S[key].state == "READY":
    S[key].state = "CLAIMED", owner = args.worker_id, attempt++ → OK
  else → FAIL("not READY")

COMPLETE:
  if S[key].state == "CLAIMED" and S[key].owner == args.worker_id:
    S[key].state = "DONE" → OK
  else → FAIL("not CLAIMED by this worker")
```

### Invariant Checker

```go
// pkg/applier/invariants.go

// CheckInvariants validates stable state for a key based on key prefix.
func CheckInvariants(key string, value json.RawMessage) bool

// Rules:
//   "inventory:*" → stock >= 0, reserved >= 0, reserved <= stock
//   "task:*"      → state in {READY, CLAIMED, DONE}; CLAIMED requires owner != ""
//   default       → true (no invariants)
```

---

## 8) Idempotent Finalize Application

**This is the ONLY path that modifies stable store.** Neither auto-merge nor any other codepath writes to stable store directly. All state changes flow through `arb.final`.

```go
// pkg/applier/finalize_apply.go

// ApplyFinalize applies a FinalizeRecord to stable state.
//
// 1. Check appliedConflicts[conflict_id] — skip if already applied (idempotent)
//
// 2. Optionally verify: hash(current stable state for key) == rec.base_state_hash
//    If mismatch, log a warning (state may have been corrupted or a finalize was missed).
//
// 3. For each outcome in rec.outcomes (in decided_order):
//      COMMIT_PATCH → apply patch_json to stable state (JSON merge-patch or RFC6902)
//      NOOP         → do nothing to state
//      FAIL         → do nothing to state
//      TRANSFORM    → apply transform_json to stable state
//
// 4. Optionally verify: hash(stable state for key) == rec.final_state_hash
//
// 5. Mark conflict_id as applied
// 6. Update status index for each op_id
// 7. Remove ops from pending window
//
func (a *Applier) ApplyFinalize(rec *FinalizeRecord) error
```

**Critical rules:**
- Apply FinalizeRecords in the order received per key (same partition guarantees this).
- Appliers NEVER re-execute original operations from FinalizeRecords. They only apply the pre-computed patches. This ensures all appliers reach identical state regardless of local processing order.
- Both FINALIZE_MERGE and FINALIZE_REBASE records are handled identically — the applier doesn't care who produced them.

### Patch format (`patch_json`)

Each `patch_json` is a **JSON merge-patch** (RFC 7396): a JSON object where each key-value pair represents a field to set. Example:

```json
// Setting /email to "alice@new.com":
{"email": "alice@new.com"}

// Incrementing /stats/views by 5 (net effect):
{"stats": {"views": 42}}  // absolute value after increment

// For INC ops, patch_json contains the absolute post-increment value,
// NOT a delta. This ensures determinism — appliers set, not add.
```

For MVP, use absolute-value patches (set the field to the final value). This is simpler and fully deterministic. Merge-patches with `null` values can delete fields if needed.

---

## 9) HLC Implementation

```go
// pkg/hlc/hlc.go

type HLC struct {
    PhysicalMS int64
    Logical    uint32
    mu         sync.Mutex
}

// Now returns a new timestamp greater than all previous.
func (h *HLC) Now() HLCTimestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    now := time.Now().UnixMilli()
    if now > h.PhysicalMS {
        h.PhysicalMS = now
        h.Logical = 0
    } else {
        h.Logical++
    }
    return HLCTimestamp{PhysicalMS: h.PhysicalMS, Logical: h.Logical}
}

// Update merges a received remote timestamp.
func (h *HLC) Update(remote HLCTimestamp) HLCTimestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    now := time.Now().UnixMilli()
    if now > h.PhysicalMS && now > remote.PhysicalMS {
        h.PhysicalMS = now
        h.Logical = 0
    } else if remote.PhysicalMS > h.PhysicalMS {
        h.PhysicalMS = remote.PhysicalMS
        h.Logical = remote.Logical + 1
    } else if h.PhysicalMS == remote.PhysicalMS {
        if remote.Logical > h.Logical {
            h.Logical = remote.Logical + 1
        } else {
            h.Logical++
        }
    } else {
        h.Logical++
    }
    return HLCTimestamp{PhysicalMS: h.PhysicalMS, Logical: h.Logical}
}

// Compare returns -1, 0, 1 for HLC ordering.
func Compare(a, b HLCTimestamp) int
```

---

## 10) Applier Main Loop

The applier runs three concurrent consumer loops:

```go
// pkg/applier/applier.go

// 1. consumeIngest(topic): reads ingest.A and ingest.B
// 2. consumeArbFinal(): reads arb.final
//
// For ingest consumers:
//   On each AcceptedRecord:
//     a. Lock the key (per-key mutex)
//     b. Add to pending window
//     c. Get pending ops from the OTHER region for the same key
//     d. For each pair: Classify()
//        - All MERGEABLE:
//            Compute deterministic order for the pair.
//            If I am the designated producer (my region == first op's region in order):
//              Call ComputeMerge() → FinalizeRecord
//              Produce FinalizeRecord (type=MERGE) to arb.final
//            Else:
//              Do nothing; wait for the FinalizeRecord from arb.final consumer
//        - Any CONFLICTING → emit ConflictCertificate to arb.cert
//     e. Unlock the key
//
// For arb.final consumer:
//   On each FinalizeRecord:
//     a. Lock the key
//     b. ApplyFinalize() — apply patches to stable store
//     c. Unlock the key
//
// IMPORTANT: The stable store is ONLY modified by the arb.final consumer via ApplyFinalize().
// The ingest consumers NEVER modify stable store directly.
```

### Per-Key Locking

```go
type KeyLocks struct {
    locks sync.Map  // map[string]*sync.Mutex
}

func (kl *KeyLocks) Lock(key string) {
    mu, _ := kl.locks.LoadOrStore(key, &sync.Mutex{})
    mu.(*sync.Mutex).Lock()
}

func (kl *KeyLocks) Unlock(key string) {
    mu, _ := kl.locks.Load(key)
    mu.(*sync.Mutex).Unlock()
}
```

Lock the key before reading/writing stable store, pending window, or status index for that key.

---

## 11) Kafka Patterns

### Producer

```go
// pkg/kafka/producer.go
// Wraps segmentio/kafka-go Writer.
// Always use key-based partitioning: set kafka.Message.Key = []byte(record.Key)
// Serialize protobuf as kafka.Message.Value
```

### Consumer

```go
// pkg/kafka/consumer.go
// Wraps segmentio/kafka-go Reader in consumer group mode.
// Commit offsets after processing each message.
```

### Consumer Group Names

| Consumer | Group Name |
|----------|-----------|
| Applier-A consuming ingest.A | `applier-A-ingest-A` |
| Applier-A consuming ingest.B | `applier-A-ingest-B` |
| Applier-A consuming arb.final | `applier-A-arb-final` |
| Applier-B consuming ingest.A | `applier-B-ingest-A` |
| Applier-B consuming ingest.B | `applier-B-ingest-B` |
| Applier-B consuming arb.final | `applier-B-arb-final` |
| Finalizer consuming arb.cert | `finalizer` |

Each applier has its own consumer groups so both appliers see all messages independently.

### Seek-and-Read (Finalizer fetching contender ops)

```go
// pkg/kafka/seek.go

// FetchRecord reads a specific record by (topic, partition, offset).
// Used by Finalizer to fetch contender ops from ingest topics.
func FetchRecord(brokers []string, topic string, partition int, offset int64) (*kafka.Message, error) {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:   brokers,
        Topic:     topic,
        Partition: partition,
    })
    defer reader.Close()
    reader.SetOffset(offset)
    msg, err := reader.ReadMessage(context.Background())
    return &msg, err
}
```

### Conflict ID Generation

Conflict ID must be unique per conflict *episode*, not just per set of op_ids. Include the base state hash to distinguish episodes on the same key with overlapping op sets.

```go
func ConflictID(key string, opIDs []string, baseStateHash []byte) string {
    sort.Strings(opIDs)
    h := sha256.New()
    h.Write([]byte(key))
    h.Write(baseStateHash)  // epoch discriminator: ties to specific stable state
    for _, id := range opIDs {
        h.Write([]byte(id))
    }
    return hex.EncodeToString(h.Sum(nil))[:32]
}
```

---

## 12) JSON Utilities

```go
// pkg/jsonutil/path.go

// GetPath extracts a value at a JSON pointer path from a document.
// Path format: "/field/subfield" (RFC 6901 JSON Pointer)
func GetPath(doc json.RawMessage, path string) (json.RawMessage, error)

// SetPath sets a value at a JSON pointer path in a document.
// Returns the modified document.
func SetPath(doc json.RawMessage, path string, value json.RawMessage) (json.RawMessage, error)

// PathsOverlap returns true if one path is a prefix of the other.
// "*" overlaps everything.
func PathsOverlap(p1, p2 string) bool

// WriteSetsDisjoint returns true if no path in ws1 overlaps any in ws2.
func WriteSetsDisjoint(ws1, ws2 []string) bool
```

```go
// pkg/jsonutil/hash.go

// CanonicalHash returns a deterministic hash of a JSON value.
// Marshals with sorted keys, no whitespace, then SHA-256.
func CanonicalHash(v json.RawMessage) []byte
```

---

## 13) Infrastructure

### scripts/setup-redpanda.sh

```bash
#!/bin/bash
set -e

# Install Redpanda (Debian/Ubuntu)
if ! command -v rpk &> /dev/null; then
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash
  sudo apt-get install -y redpanda
fi

# Configure for development on single host.
# "dev-container" is a Redpanda config preset (not Docker-related) that relaxes
# production checks (fsync, memory limits) for local development.
sudo rpk redpanda mode dev-container
sudo rpk config set redpanda.advertised_kafka_api '[{address: "127.0.0.1", port: 9092}]'

# Start Redpanda
sudo systemctl start redpanda || sudo rpk redpanda start --detach

# Wait for broker
echo "Waiting for Redpanda..."
for i in $(seq 1 30); do
  rpk cluster info --brokers 127.0.0.1:9092 2>/dev/null && break
  sleep 1
done

echo "Redpanda is running."
rpk cluster info --brokers 127.0.0.1:9092
```

### scripts/create-topics.sh

```bash
#!/bin/bash
set -e

BROKER="127.0.0.1:9092"

rpk topic create ingest.A  --partitions 16 --replicas 1 --brokers $BROKER
rpk topic create ingest.B  --partitions 16 --replicas 1 --brokers $BROKER
rpk topic create arb.cert  --partitions 4  --replicas 1 --brokers $BROKER
rpk topic create arb.final --partitions 4  --replicas 1 --brokers $BROKER

echo "Topics created:"
rpk topic list --brokers $BROKER
```

**Default broker address**: `127.0.0.1:9092` (all services use this as default).

---

## 14) Workloads

### Common Harness

```go
// pkg/workload/harness.go

type WorkloadConfig struct {
    KeyspaceSize      int
    ZipfTheta         float64   // 0=uniform, 0.8-1.2=skew
    SharedHotFraction float64   // fraction of keys both regions write
    OpMix             map[string]float64  // op_type → weight
    DisjointFieldProb float64   // for PATCH: prob fields are disjoint
    OpsPerSecond      int       // per region
    DurationSec       int
    BurstProfile      *BurstProfile  // optional
}

type WorkloadModel interface {
    InitValue(key string) json.RawMessage
    NextOp(region string, key string, rng *rand.Rand) WriteRequest
    CheckInvariants(value json.RawMessage) bool
}
```

```go
// pkg/workload/keypicker.go

// KeyPicker selects keys using Zipf distribution with hot-set overlap.
// Hot set = first SharedHotFraction * KeyspaceSize keys; both regions target it.
// Remaining keys are region-exclusive.
// Uses precomputed Zipf CDF; samples in O(log N) via binary search.
```

### Workload A: Geo-YCSB++ (KV + conditional ops)

- Schema: `{f0..f7: int, stats: {views: int, likes: int}, ver: int}`
- Ops: PUT (write_set=`["*"]`), PATCH 1-3 fields, INC `/stats/views` or `/stats/likes`, PUT_IF_ABSENT, CAS on `/ver`
- Goal: conflict rate curves, auto-merge rate vs `shared_hot_fraction`

### Workload B: Profile Patch (semantic merge showcase) — **implement first**

- Schema: `{email, phone, addr:{city,zip}, prefs:{theme,lang}, stats:{views,likes}}`
- Ops: PATCH single field, INC stats
- Conflict injection: hot keys get paired ops; Region A picks field, Region B picks disjoint or same with prob `disjoint_field_prob`
- Goal: show disjoint patches auto-merge; arb traffic reduction

### Workload C: Task Queue (CAS-heavy rebase)

- Schema: `{state: "READY"|"CLAIMED"|"DONE", owner: "", updated_ms: 0, attempt: 0}`
- Ops: CLAIM, COMPLETE
- Goal: exactly one winner per task; no double-claims in stable view

### Workload D: Inventory (invariant safety)

- Schema: `{stock: 100, reserved: 0}`
- Invariant: `0 <= reserved <= stock`
- Ops: RESERVE(n), CANCEL(n)
- Goal: invariant never violated in stable view

### Workload E: Downstream Compute Pipeline

- Consumer reads `arb.final`, sleeps `compute_ms` per record to simulate compute
- Goal: measure end-to-end latency (Accepted → derived state updated), consumer lag under bursts

---

## 15) Verification (Automated Assertions)

```go
// pkg/workload/verify.go

// RunVerification performs post-run correctness checks.
// Returns list of failures (empty = pass).
func RunVerification(applierAURL, applierBURL string, config WorkloadConfig, submittedOps []OpRecord) []string
```

Checks:

1. **State checksum match**: `GET /hash` from both appliers; must be equal.
2. **Invariant scan**: for all keys in invariant-enabled namespaces, `GET /stable?key=X` and check invariants.
3. **Finalize coverage**: for all submitted op_ids, `GET /status?op_id=X` must return FINALIZED.
4. **No duplicate winners**: for task workload, each task key has at most one CLAIMED owner.

Output per run:
- Config JSON
- Throughput summary (accepted ops/sec, finalized ops/sec)
- Latency histograms (Accepted, Finalized)
- arb.cert/sec, arb.final/sec, auto-merge %
- State checksum + invariant scan pass/fail

---

## 16) Testing

### Unit Tests (run with `go test ./...`)

Priority:

1. `pkg/jsonutil/path_test.go` — PathsOverlap, WriteSetsDisjoint, GetPath, SetPath
2. `pkg/applier/classify_test.go` — all classification rules
3. `pkg/applier/merge_test.go` — auto-merge for each MERGEABLE case
4. `pkg/applier/invariants_test.go` — invariant checks per namespace
5. `pkg/finalizer/order_test.go` — ordering comparator edge cases
6. `pkg/engine/apply_test.go` — apply function for each op type
7. `pkg/finalizer/rebase_test.go` — rebase with 2-3 contenders

### Integration Tests

1. Ensure Redpanda is running (`rpk cluster info`)
2. Submit concurrent ops on same key via two ingest services
3. Verify: disjoint patches produce merge FinalizeRecord in arb.final (no arb.cert); overlapping patches create arb.cert → arb.final; both appliers reach same stable state (checksum match)

---

## 17) Implementation Order

Follow this exact sequence:

```
M0: Infrastructure
  1. scripts/setup-redpanda.sh (install + start single broker)
  2. scripts/create-topics.sh
  3. proto/records.proto + generate Go code
  4. go.mod with dependencies (segmentio/kafka-go, protobuf)

M1: Foundations
  5. pkg/hlc/hlc.go + hlc_test.go
  6. pkg/jsonutil/path.go + path_test.go
  7. pkg/jsonutil/hash.go + hash_test.go

M2: Ingest Service
  8. pkg/kafka/producer.go
  9. pkg/ingest/service.go (HTTP handler + produce)
  10. cmd/ingest/main.go (--region, --port, --broker flags)
  11. Manual test: POST /write, verify message appears in topic via rpk

M3: Apply Engine (shared)
  12. pkg/engine/apply.go (per-op-type Apply — PURE function)
  13. pkg/engine/diff.go (JSON merge-patch diff — PURE function)
  14. pkg/applier/invariants.go
  15. Unit tests for all three

M4: Applier
  16. pkg/kafka/consumer.go
  17. pkg/applier/store.go (in-memory stable store + Hash())
  18. pkg/applier/pending.go (pending window)
  19. pkg/applier/classify.go + classify_test.go
  20. pkg/applier/merge.go + merge_test.go (ComputeMerge → FinalizeRecord)
  21. pkg/applier/finalize_apply.go (apply patches from arb.final — ONLY writer to stable store)
  22. pkg/applier/api.go (GET /stable, /status, /hash)
  23. pkg/applier/applier.go (main loop: 3 consumers + per-key locks + designated merge producer)
  24. cmd/applier/main.go (--region, --port, --broker flags)

M5: Finalizer
  25. pkg/kafka/seek.go (seek-and-read by offset)
  26. pkg/finalizer/order.go + order_test.go
  27. pkg/finalizer/rebase.go + rebase_test.go (rebase emits patches via Diff)
  28. pkg/finalizer/finalizer.go (main loop)
  29. cmd/finalizer/main.go (--broker flag)

M6: End-to-End Sanity
  30. Start all services, submit one PUT to ingest-A, verify stable read on both appliers
  31. Submit concurrent disjoint PATCHes, verify merge FinalizeRecord in arb.final, both stores converge
  32. Submit concurrent overlapping PATCHes, verify arb.cert → arb.final → convergence

M7: Workload Harness + Verification
  33. pkg/workload/keypicker.go (Zipf + hot set)
  34. pkg/workload/metrics.go (latency histograms, throughput)
  35. pkg/workload/harness.go (common harness)
  36. pkg/workload/workload_b.go (Profile Patch — first workload)
  37. pkg/workload/verify.go (checksum, invariants, coverage)
  38. cmd/harness/main.go

M8: Remaining Workloads
  39. workload_a.go (Geo-YCSB++)
  40. workload_c.go (Task Queue)
  41. workload_d.go (Inventory)
  42. workload_e.go (Downstream Compute)
  43. Full test suite: all workloads + verification pass
```

---

## 18) CLI Flags

All binaries:

| Flag | Default | Used by |
|------|---------|---------|
| `--broker` | `127.0.0.1:9092` | all |
| `--region` | (required) `A` or `B` | ingest, applier |
| `--port` | `8080` | ingest, applier |
| `--log-level` | `info` | all |

Harness additional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--workload` | `B` | Workload: A, B, C, D, E |
| `--duration` | `30s` | Test duration |
| `--ops-per-sec` | `100` | Per region |
| `--keyspace-size` | `1000` | Number of keys |
| `--zipf-theta` | `0.8` | Skew (0=uniform) |
| `--shared-hot-fraction` | `0.05` | Keys both regions write |
| `--disjoint-field-prob` | `0.8` | PATCH field disjointness |
| `--ingest-a-url` | `http://localhost:8081` | Ingest A endpoint |
| `--ingest-b-url` | `http://localhost:8082` | Ingest B endpoint |
| `--applier-a-url` | `http://localhost:8091` | Applier A (for verification) |
| `--applier-b-url` | `http://localhost:8092` | Applier B (for verification) |

---

## 19) Build & Run

```bash
# Generate protobuf
protoc --go_out=gen --go_opt=paths=source_relative proto/records.proto

# Install + start Redpanda (first time only)
chmod +x scripts/setup-redpanda.sh scripts/create-topics.sh
./scripts/setup-redpanda.sh

# Create topics (first time only)
./scripts/create-topics.sh

# Start services (each in a separate terminal)
go run ./cmd/ingest    --region=A --port=8081 --broker=127.0.0.1:9092
go run ./cmd/ingest    --region=B --port=8082 --broker=127.0.0.1:9092
go run ./cmd/applier   --region=A --port=8091 --broker=127.0.0.1:9092
go run ./cmd/applier   --region=B --port=8092 --broker=127.0.0.1:9092
go run ./cmd/finalizer --broker=127.0.0.1:9092

# Run workload
go run ./cmd/harness --workload=B --duration=30s --ops-per-sec=100
```

---

## 20) Key Implementation Rules

1. **Stable store is write-only via arb.final.** The ONLY codepath that modifies stable store is `ApplyFinalize()` consuming from `arb.final`. Auto-merge does NOT write to stable store — it produces a FinalizeRecord to `arb.final` and waits for it to come back. This is the fundamental convergence guarantee.

2. **Patches, not replays.** FinalizeRecords carry net-effect patches (`patch_json`) for every committed op. Appliers apply these patches directly. They never re-execute original operations from finalize records. This eliminates divergence from re-execution against different base states.

3. **Determinism is sacred.** The classify, merge, apply, rebase, and ordering functions must produce identical results given the same inputs regardless of which process runs them. No randomness, no wall-clock reads inside these functions.

4. **Idempotency.** Applier dedupes FinalizeRecords by conflict_id. Finalizer dedupes ConflictCertificates by conflict_id.

5. **Partition key consistency.** All producers use the record's `key` field as the Kafka message key. Same key → same partition across all topics.

6. **Per-key locking in Applier.** Three concurrent consumers means potential races on the same key's state. Use per-key mutexes (§10).

7. **Canonical JSON.** For state checksums and verifier digests, marshal with sorted keys, no whitespace.

8. **Apply is shared.** `pkg/engine/apply.go` is used by both the Applier (merge computation) and Finalizer (rebase). Do not duplicate this logic.

9. **Designated merge producer.** When both appliers detect the same MERGEABLE pair, only one produces the FinalizeRecord. The designated producer is the applier whose region matches the first op in the deterministic HLC order. This avoids duplicate merge records.

---

## 21) Terminology

| Term | Meaning |
|------|---------|
| Accepted | Locally durable append to region log; fast ack to client |
| Finalized | Globally stable outcome after arb.final applied |
| Stable store | Contains only finalized state; modified ONLY via arb.final consumer |
| Pending window | Recent accepted ops not yet finalized, per key |
| COMMIT_PATCH | Op succeeded; patch_json contains net-effect delta to apply to stable state |
| NOOP | Op skipped (already satisfied or superseded) |
| FAIL | Op rejected (precondition or invariant violation) |
| MERGEABLE | Concurrent ops that can be auto-merged; still logged to arb.final |
| CONFLICTING | Concurrent ops requiring arbitration and rebase via Finalizer |
| FINALIZE_MERGE | FinalizeRecord produced by applier for mergeable ops (fast path) |
| FINALIZE_REBASE | FinalizeRecord produced by finalizer for conflicting ops (slow path) |

---

## 22) Single-Host Testbed (Stress Testing — after M8)

### WAN Emulation with `tc netem`

Since all services run on the same host communicating via loopback, use `tc netem` on the loopback interface or use per-service artificial delays:

**Option A: Application-level delay (simpler, recommended for MVP)**

Add a `--latency` flag to the applier/ingest that injects `time.Sleep` before producing or after consuming. This simulates cross-region delay without `tc`.

**Option B: `tc netem` on loopback (requires root)**

```bash
# Add 80ms delay to loopback (affects all local traffic)
sudo tc qdisc add dev lo root netem delay 80ms 10ms distribution normal

# Remove
sudo tc qdisc del dev lo root
```

Note: loopback `tc` affects ALL local services including Redpanda itself, so Option A is more precise.

### Stress Tests

- **Stress A**: Add 80ms simulated cross-region latency, re-run all workloads
- **Stress B**: Add jitter + simulated packet loss (application-level random drop)
- **Stress C**: Burst load (60s steady → 30s 2x → steady), verify convergence
- **Stress D**: Pause one applier's consumer for 5s, verify catch-up
- **Stress E**: Kill finalizer during high-conflict workload, restart, verify idempotent recovery
