# llm.md — LLM-Assisted Semantic Repair (DP3)

## 0. Purpose

This document specifies **DP3**, an optional extension to the rebase pipeline that uses an LLM to propose **bounded semantic repairs** when DP2 deterministic re-execution returns **FAIL** outcomes.

DP3 is designed to **enhance success rate and reduce manual reconciliation** while preserving the system's core guarantees:

- **Replica determinism**: all replicas converge to the same stable state.
- **Invariant safety**: stable state never violates declared invariants.
- **Auditability**: every finalized outcome is explainable and reproducible from logs.

> **Key principle**: The LLM is a *proposer*, not a decider. The system accepts only what a **deterministic verifier** validates.

DP3 is **best-effort**: the system always falls back to DP2 outcomes if DP3 fails, times out, or produces invalid proposals.

---

## 1. Context: DP2 vs DP3

### DP2 (baseline)

Given a conflict set on a key and a deterministic decided order:

1. Load the current **stable state** for the key.
2. Re-execute contenders in `decided_order`.
3. For each op, compute the net-effect patch (via `engine.Diff`) and produce outcome:
   - `COMMIT_PATCH(patch_json)` / `NOOP(reason)` / `FAIL(reason)`
4. Emit a `FinalizeRecord` to `arb.final`.
5. Replicas apply patches from `FinalizeRecord` to stable state (idempotent).

DP2 guarantees convergence but produces FAIL when:
- preconditions cannot be satisfied (e.g., CAS mismatch)
- invariants would be violated (e.g., reserved > stock)
- patch is invalid over current state

### DP3 (this document)

For DP2 FAIL operations, DP3 attempts to **repair** by proposing **policy-gated** alternatives:
- adjust parameters within policy bounds (e.g., clamp reservation quantity)
- convert a FAIL into an explicit NOOP with reason
- produce a constrained patch that preserves invariants

DP3 never changes ordering; it only proposes *alternative outcomes* for FAILed ops **after** DP2 has established `decided_order`.

---

## 2. Non-goals

DP3 does **not**:
- override invariants or safety checks
- introduce nondeterministic behavior at replicas
- require replicas to call an LLM
- guarantee that a FAIL can always be repaired
- change the accepted vs finalized semantics of the system

---

## 3. Threat Model and Safety Posture

### Threats

- **Prompt injection / adversarial op content**: operations may contain user-supplied strings embedded in args. The LLM prompt must sanitize or quote these.
- **Nondeterminism**: different replicas or runs could yield different repairs. Contained by having only the Finalizer call the LLM and logging the verified result.
- **Overreach**: LLM proposes changes outside allowed scope (other fields, other keys).
- **Resource abuse**: huge outputs, deep JSON, high CPU parsing/verifying.

### Safety posture

- Only the Finalizer calls the LLM. Replicas never invoke DP3.
- LLM output is treated as **untrusted bytes**.
- Deterministic verifier enforces: schema, scope, policy, invariants, replay determinism.
- If any verification step fails → **fall back to DP2 outcomes** immediately.

---

## 4. Where DP3 Runs

DP3 runs in the **Finalizer Service**, after DP2, before producing the FinalizeRecord.

### Flow per conflict_id

```
ConflictCertificate arrives
  │
  ▼
DP2: deterministic rebase → provisional outcomes
  │
  ├── No FAIL ops → emit FinalizeRecord (done, type=FINALIZE_REBASE)
  │
  └── Some ops FAIL and DP3 enabled for this namespace →
        │
        ▼
      Build prompt from stable state + DP2 context
        │
        ▼
      Call LLM (with timeout)
        │
        ├── Parse succeeds → run deterministic verifier
        │     ├── Verifier passes → merge proposals into outcomes
        │     │     → emit FinalizeRecord (type=FINALIZE_REBASE_LLM)
        │     └── Verifier rejects → fall back to DP2 outcomes
        │
        └── LLM error / timeout / parse failure → fall back to DP2 outcomes
        │
        ▼
      Emit FinalizeRecord
```

---

## 5. Data Model

### 5.1 CandidateResolution (LLM output schema)

The LLM must output **exactly one JSON object** with this schema:

```json
{
  "conflict_id": "string",
  "key": "string",
  "decided_order": ["op_id_1", "op_id_2"],
  "proposals": [
    {
      "op_id": "string",
      "action": "PATCH | NOOP",
      "patch_json": { "field": "value" },
      "reason": "short string"
    }
  ],
  "notes": "optional short string"
}
```

Rules:
- `action=PATCH` → must include `patch_json` (RFC 7396 JSON merge-patch, absolute values)
- `action=NOOP` → no patch, must include `reason`
- MVP: only `PATCH` and `NOOP` actions. `TRANSFORM` deferred to post-MVP.
- `reason` must be non-executable plain text (stored for debugging, not used for logic)
- `decided_order` must exactly match DP2's decided order (byte-for-byte)
- Each proposal's `op_id` must reference a DP2 FAIL op

### 5.2 FinalizeRecord type

Extends the existing `FinalizeType` enum:

```protobuf
enum FinalizeType {
  FINALIZE_UNKNOWN = 0;
  FINALIZE_MERGE = 1;         // DP1 fast-path merge
  FINALIZE_REBASE = 2;        // DP2 deterministic rebase
  FINALIZE_REBASE_LLM = 3;    // DP2 + DP3 LLM-assisted repair
}
```

The FinalizeRecord format is unchanged. DP3 proposals are converted into standard `OpOutcome` entries with `OUTCOME_COMMIT_PATCH` or `OUTCOME_NOOP`. Appliers cannot distinguish DP2-produced patches from DP3-produced patches — both are just `patch_json` in `arb.final`.

---

## 6. Patch Semantics

### 6.1 Format

DP3 uses **JSON Merge Patch (RFC 7396)**.

- Merge patch is an object where each key-value pair sets/replaces a field.
- Values are **absolute** (not deltas). For INC-like repairs, the patch contains the final value.
- Null values delete fields (optional for MVP; can ignore).

### 6.2 Write-set scope rule

Each AcceptedRecord's `write_set` (derived from op_type + args at ingest time) defines which fields the operation is allowed to modify.

Define `ModifiedPaths(patch_json)` as the set of JSON-pointer paths that the merge-patch would change.

Example: `{"addr": {"city": "X"}}` modifies `/addr` and `/addr/city`.

**Scope rule**: A DP3 proposal is valid only if for every `p ∈ ModifiedPaths(patch_json)`:
- `p` is within the op's write_set (exact path or descendant of a write_set path), OR
- write_set contains `"*"`

This prevents the LLM from editing unrelated fields.

---

## 7. Deterministic Verifier

### 7.1 Inputs (all from logged/deterministic sources)

- `stable_state`: stable JSON for the key at finalization time
- `contenders`: AcceptedRecords for the conflict set
- `decided_order`: op_ids in decided order (from DP2)
- `dp2_outcomes`: DP2 results for each op
- `candidate`: parsed CandidateResolution from LLM
- `policy`: per-namespace policy config

### 7.2 Outputs

```go
type VerifierResult struct {
    Accepted        bool
    Failures        []VerifierFailure
    FinalOutcomes   []*OpOutcome        // merged DP2 + accepted DP3 proposals
    FinalState      json.RawMessage     // from deterministic replay
    FinalStateHash  []byte
    VerifierDigest  []byte
}

type VerifierFailure struct {
    Stage  string  // "V0".."V6"
    OpID   string  // which op, if applicable
    Reason string
}
```

### 7.3 Verification stages (all must pass)

#### V0: Parse & Size Caps

- Response must be valid JSON.
- Strict decoder: reject unknown fields (`json.Decoder` with `DisallowUnknownFields`).
- Enforce caps:
  - Max response bytes: 64KB
  - Max proposals: ≤ number of DP2 FAIL ops
  - Max patch depth: 16 levels
  - Max keys per patch: 128

#### V1: Identity & Order

- `conflict_id` must match the conflict being processed.
- `key` must match.
- `decided_order` must exactly match DP2's decided order (same op_ids, same sequence).
- Each proposal's `op_id` must exist in the conflict set AND must have DP2 outcome = FAIL.

#### V2: Scope

- For each `PATCH` proposal: compute `ModifiedPaths(patch_json)` and verify all are within the op's `write_set` (accounting for segment-prefix relationships, not string-prefix).
- Write_set `["*"]` allows any path.

#### V3: Policy Gate

Per-namespace policy defines allowed repairs:
- Inventory: allow clamp quantity? (configurable)
- Task queue: NOOP only (no creative repairs)
- Profile: PATCH within same paths only

If a proposal's action is not allowed by the namespace's policy → reject.

#### V4: Deterministic Replay & Invariants

Starting from `stable_state`, replay ALL ops in `decided_order`:
- For ops with DP2 outcome COMMIT_PATCH: apply DP2's `patch_json`
- For ops with DP2 outcome NOOP/FAIL that have an accepted DP3 proposal: apply DP3's `patch_json` (for PATCH) or skip (for NOOP)
- For ops with DP2 outcome NOOP/FAIL and no DP3 proposal: skip

After each step:
- Validate invariants
- Ensure JSON remains valid

If replay diverges or violates invariants → reject entire candidate.

#### V5: Improvement Requirement

Each accepted DP3 proposal must **strictly improve** over DP2:
- DP2=FAIL → DP3=PATCH or DP3=NOOP is an improvement (op gets a definitive outcome instead of failure)
- DP2=COMMIT_PATCH or DP2=NOOP → DP3 must NOT change these (forbid by default)

This prevents the LLM from making random changes to already-succeeded ops.

#### V6: Deterministic Digest

Compute:
```
verifier_digest = SHA256(
    stable_state_hash ||
    decided_order_serialized ||
    dp2_outcomes_serialized ||
    accepted_proposals_normalized ||
    policy_version
)
```

Embed digest in `FinalizeRecord.verifier_digest`. Replicas may optionally recompute for defense-in-depth (MVP: applier trusts finalizer).

---

## 8. LLM Prompting

### 8.1 Prompt Construction

The prompt must:
- Include stable state for the key (truncate if >4KB; include hash for reference)
- Include each FAILed op (op_id, type, args, write_set, failure reason from DP2)
- Include invariants and allowed policies for this namespace
- Explicitly instruct JSON-only output matching CandidateResolution schema
- Forbid touching fields outside write_set
- Forbid changing decided_order

### 8.2 Prompt Template

```
System: You are a conflict resolution engine. Output ONLY valid JSON matching the
specified schema. No markdown, no explanation outside the JSON.

User:

## Conflict context
conflict_id: {conflict_id}
key: {key}
decided_order: {decided_order_json}

## Current stable state:
{stable_state_json}

## DP2 outcomes (what deterministic re-execution decided):
{for each op: op_id, outcome, reason}

## Failed operations requiring repair:
{for each FAIL op:
  op_id: ...
  op_type: ...
  args: ...
  write_set: [...]
  fail_reason: ...
}

## Invariants (must hold after all patches applied):
{invariant list}

## Allowed actions per policy:
{policy rules: e.g., "PATCH within write_set", "NOOP with reason", "clamp quantity allowed"}

## Output schema:
{
  "conflict_id": "...",
  "key": "...",
  "decided_order": [...],
  "proposals": [
    {
      "op_id": "...",
      "action": "PATCH|NOOP",
      "patch_json": { ... },
      "reason": "..."
    }
  ],
  "notes": "..."
}

## Rules:
- Only propose repairs for FAILED ops. Do not change succeeded or NOOPed ops.
- patch_json may ONLY modify fields within the failed op's write_set.
- Use absolute values in patches, not deltas.
- Final state after all patches must satisfy all invariants.
- decided_order must be copied exactly as shown above.
- Prefer minimal changes. Explain reason briefly.
```

### 8.3 Prompt Safety

- User-supplied strings in `args` (e.g., email addresses, names) must be JSON-encoded within the prompt, not interpolated as raw text. This prevents prompt injection.
- Truncate large stable states (>4KB) to relevant fields only.
- Never include API keys, system internals, or other sensitive data in the prompt.

---

## 9. LLM Client Abstraction

DP3 must not depend on a specific vendor. Use an adapter interface:

```go
// pkg/finalizer/llm.go

// LLMClient is vendor-agnostic. Implementations wrap Anthropic, OpenAI, local models, etc.
type LLMClient interface {
    // CompleteJSON sends a prompt and returns raw text response.
    // Caller must parse JSON and verify deterministically.
    CompleteJSON(ctx context.Context, prompt string, maxTokens int) (string, error)
}
```

### Default implementation (Anthropic)

```go
// pkg/finalizer/llm_anthropic.go

type AnthropicClient struct {
    APIUrl    string
    APIKey    string
    Model     string  // e.g., "claude-sonnet-4-20250514"
    Client    *http.Client
}

func (c *AnthropicClient) CompleteJSON(ctx context.Context, prompt string, maxTokens int) (string, error) {
    reqBody := map[string]interface{}{
        "model":      c.Model,
        "max_tokens": maxTokens,
        "messages": []map[string]string{
            {"role": "user", "content": prompt},
        },
    }
    body, _ := json.Marshal(reqBody)
    httpReq, _ := http.NewRequestWithContext(ctx, "POST", c.APIUrl, bytes.NewReader(body))
    httpReq.Header.Set("Content-Type", "application/json")
    httpReq.Header.Set("x-api-key", c.APIKey)
    httpReq.Header.Set("anthropic-version", "2023-06-01")

    resp, err := c.Client.Do(httpReq)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    var result struct {
        Content []struct {
            Type string `json:"type"`
            Text string `json:"text"`
        } `json:"content"`
    }
    json.NewDecoder(resp.Body).Decode(&result)

    for _, block := range result.Content {
        if block.Type == "text" {
            return block.Text, nil
        }
    }
    return "", fmt.Errorf("no text content in response")
}
```

---

## 10. Integration into Finalizer

### Pseudocode

```go
func (f *Finalizer) ProcessConflict(cert *ConflictCertificate) *FinalizeRecord {
    ops := f.fetchContenders(cert)
    stableState := f.getStableState(cert.Key)

    // DP2: deterministic rebase
    decidedOrder := DecideOrder(ops)
    dp2Result := f.RebaseFull(stableState, ops, cert.Key, decidedOrder)

    // DP3: best-effort LLM repair
    finalOutcomes := dp2Result.Outcomes
    finalizeType := FINALIZE_REBASE

    if f.rebaseMode == "rebase+llm" && dp2Result.HasFails() && f.policyAllows(cert.Key) {
        ctx, cancel := context.WithTimeout(context.Background(), f.dp3Timeout)
        defer cancel()

        prompt := f.buildPrompt(cert, stableState, ops, decidedOrder, dp2Result)
        raw, err := f.llm.CompleteJSON(ctx, prompt, f.dp3MaxTokens)
        if err == nil {
            candidate, parseErr := StrictParse(raw)
            if parseErr == nil {
                vr := f.verifier.Verify(candidate, stableState, ops, decidedOrder, dp2Result.Outcomes, cert.Key)
                if vr.Accepted {
                    finalOutcomes = vr.FinalOutcomes
                    finalizeType = FINALIZE_REBASE_LLM
                }
            }
        }
        // Any failure at any step → keep dp2Result.Outcomes
    }

    return f.emitFinalizeRecord(cert, decidedOrder, finalOutcomes, finalizeType)
}
```

---

## 11. Operational Controls

### 11.1 Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--rebase-mode` | `rebase` | `rebase`, `rebase+llm`, `lww` |
| `--llm-provider` | `anthropic` | `anthropic` (extensible) |
| `--llm-api-url` | `https://api.anthropic.com/v1/messages` | API endpoint |
| `--llm-api-key` | env `ANTHROPIC_API_KEY` | API key |
| `--llm-model` | `claude-sonnet-4-20250514` | Model identifier |
| `--llm-timeout-ms` | `5000` | Timeout per LLM call |
| `--llm-max-tokens` | `1024` | Max response tokens |
| `--llm-max-response-bytes` | `65536` | Max response size |
| `--llm-policy-file` | `""` | YAML policy file (empty = all-enabled defaults) |

### 11.2 Observability (metrics to collect)

- DP2 fail count per conflict
- DP3 attempted (y/n)
- DP3 LLM latency (ms)
- DP3 parse success/failure
- DP3 verifier pass/reject + reject reason histogram (V0-V6)
- DP3 accepted proposals count
- DP3 improvement rate (FAIL → COMMIT_PATCH or NOOP)
- DP3 fallback rate (fell back to DP2)

---

## 12. Workload-Specific Policies

### 12.1 Profile/Document Patch

- Allow: `PATCH` within write_set paths, `NOOP` with reason
- Forbid: `TRANSFORM`, touching other fields
- Common repairs: if patch fails due to missing nested object, propose creating it (within write_set)

### 12.2 Task Queue Claiming

- Allow: `NOOP` with reason only (e.g., "already claimed by another worker")
- Forbid: `PATCH`, `TRANSFORM` — task state machine transitions must be deterministic
- Common repairs: convert FAIL("not READY") into NOOP("task already claimed")

### 12.3 Inventory Reservation

- Allow: `PATCH` with clamped quantity (policy-gated), `NOOP` with reason
- Policy knob: `allow_clamp_quantity: true|false`
- Invariant: `0 <= reserved <= stock`
- Common repairs: clamp reservation to remaining capacity, or NOOP if nothing available

### Example policy YAML (`llm-policy.yaml`)

```yaml
policies:
  - namespace: "inventory:*"
    enabled: true
    allowed_actions: ["PATCH", "NOOP"]
    allow_clamp_quantity: true
    invariants:
      - "0 <= reserved <= stock"
      - "stock and reserved must be non-negative integers"
    hints:
      - "Partial fulfillment is preferred over rejection"
      - "Clamp reservation to remaining capacity when possible"

  - namespace: "user:*"
    enabled: true
    allowed_actions: ["PATCH", "NOOP"]
    invariants:
      - "email must be valid format"
    hints:
      - "User-initiated writes take priority over automated syncs"

  - namespace: "task:*"
    enabled: false
    # Task queue uses pure DP2; no LLM repairs
```

---

## 13. Correctness Argument

DP3 preserves DP2 correctness because:

1. Replicas do not run DP3; only the Finalizer emits `FinalizeRecord`.
2. `FinalizeRecord` is the single authoritative source of truth for stable updates.
3. The verifier ensures every accepted proposal:
   - is within write-set scope (V2)
   - respects policy (V3)
   - preserves invariants (V4)
   - produces deterministic replay from logged inputs (V4, V6)
   - strictly improves over DP2 (V5)
4. DP3 proposals are converted into standard `patch_json` entries. Appliers process them identically to DP2-produced patches.

Thus, DP3 can only refine DP2 outcomes; it cannot violate the system's safety or convergence guarantees.

---

## 14. New Files

```
pkg/finalizer/
├── llm.go              # LLMClient interface + prompt builder + response parser
├── llm_anthropic.go    # Anthropic API implementation of LLMClient
├── verifier.go         # 7-stage deterministic verifier (V0-V6)
├── verifier_test.go    # Unit tests for each verification stage
└── policy.go           # Per-namespace policy config loading
```

---

## 15. Implementation Order

Add after M5 (Finalizer complete), before M6 (end-to-end sanity):

```
M5.5: DP3 — LLM-Assisted Repair
  A. pkg/finalizer/policy.go + policy loading from YAML
  B. pkg/finalizer/llm.go (LLMClient interface, prompt builder, StrictParse)
  C. pkg/finalizer/llm_anthropic.go (Anthropic API implementation)
  D. pkg/finalizer/verifier.go (V0-V6) + verifier_test.go
  E. Integration into finalizer.go (ProcessConflict with DP3 path)
  F. Add FINALIZE_REBASE_LLM to proto enum
  G. Add --rebase-mode=rebase+llm and --llm-* flags to cmd/finalizer/main.go
  H. Integration test: conflicting RESERVEs → DP2 FAIL → DP3 clamps → verifier passes
```

---

## 16. Testing

### Unit tests (`pkg/finalizer/verifier_test.go`)

```
V0 tests:
  1. TestV0_ValidJSON                    — well-formed response passes
  2. TestV0_InvalidJSON                  — malformed JSON rejected
  3. TestV0_UnknownFields               — extra fields rejected (strict decoder)
  4. TestV0_ResponseTooLarge            — exceeds 64KB rejected
  5. TestV0_PatchTooDeep                — >16 levels rejected
  6. TestV0_TooManyPatchKeys            — >128 keys rejected
  7. TestV0_TooManyProposals            — more proposals than FAIL ops rejected

V1 tests:
  8. TestV1_ConflictIDMismatch          — wrong conflict_id rejected
  9. TestV1_DecidedOrderMismatch        — reordered decided_order rejected
  10. TestV1_ProposalForNonFailOp       — proposal targeting COMMIT_PATCH op rejected
  11. TestV1_ProposalForUnknownOp       — op_id not in conflict set rejected

V2 tests:
  12. TestV2_PatchWithinWriteSet        — /email within ["/email"] passes
  13. TestV2_PatchOutsideWriteSet       — /phone outside ["/email"] rejected
  14. TestV2_NestedPathWithinParent     — /addr/city within ["/addr"] passes
  15. TestV2_WildcardAllowsAny          — ["*"] allows any field
  16. TestV2_SegmentPrefixNotString     — /ad not within ["/addr"] rejected

V3 tests:
  17. TestV3_ActionAllowedByPolicy      — PATCH allowed for inventory namespace
  18. TestV3_ActionForbiddenByPolicy    — PATCH rejected for task namespace
  19. TestV3_ClampAllowedWhenEnabled    — policy.allow_clamp_quantity=true allows clamp

V4 tests:
  20. TestV4_ReplayProducesValidState   — invariants hold after replay
  21. TestV4_ReplayViolatesInvariant    — reserved > stock after replay → rejected
  22. TestV4_ReplayWithMixedOutcomes    — DP2 success + DP3 repair combined

V5 tests:
  23. TestV5_FailToPatchIsImprovement   — FAIL → PATCH accepted
  24. TestV5_FailToNoopIsImprovement    — FAIL → NOOP accepted
  25. TestV5_CommitToNoopNotAllowed     — changing COMMIT_PATCH to NOOP rejected

V6 tests:
  26. TestV6_DigestDeterministic        — same inputs produce same digest
  27. TestV6_DigestChangesWithInput     — different inputs produce different digest
```

### Integration test

```
1. Submit two concurrent RESERVE(80) ops on key "inventory:flash-sale" with stock=100.
2. DP2 rebase: first op succeeds (reserved=80), second FAILs (20 < 80).
3. DP3 proposes: PATCH for second op → {"reserved": 100} (clamp to remaining 20).
4. Verifier: V2 passes (/reserved in write_set), V4 passes (100 ≤ 100).
5. FinalizeRecord emitted with type=FINALIZE_REBASE_LLM.
6. Both appliers converge to {stock: 100, reserved: 100}.
7. Verify: invariant holds, checksum match, both ops finalized.
```

---

## 17. Evaluation Plan

### Metrics struct extension

```go
type DP3Metrics struct {
    LLMInvocations      int
    LLMLatencyP50MS     float64
    LLMLatencyP99MS     float64
    ParseSuccessRate    float64
    VerifierPassRate    float64
    VerifierRejectHisto map[string]int  // stage → count (e.g., "V2": 3, "V4": 1)
    RescuedOps          int             // FAIL → COMMIT_PATCH or NOOP via DP3
    ImprovementRate     float64         // RescuedOps / total DP2 FAIL ops
    FallbackRate        float64         // % of DP3 attempts that fell back to DP2
    ScopeViolations     int             // V2 rejects (LLM tried to escape scope)
}
```

### Baseline comparison

| Baseline | DP1 | DP2 | DP3 |
|----------|-----|-----|-----|
| `slx` | ON | rebase | OFF |
| `slx-l` | ON | rebase+llm | ON |
| `b1` | OFF | rebase | OFF |
| `b2` | ON | lww | OFF |
| `b3` | OFF | lww | OFF |
| `b4` | ON | rebase | OFF (single-master) |

Key comparison: `slx` vs `slx-l` isolates DP3 contribution.

### Ablations (within DP3)

| Variant | Allowed actions | Purpose |
|---------|----------------|---------|
| DP3-noop | NOOP only | LLM can only convert FAILs to explicit NOOPs |
| DP3-patch | NOOP + PATCH | LLM can propose bounded repairs |

### Target graphs

**Graph 7: Op Survival With/Without LLM**
- X-axis: Workload (A, B, C, D)
- Y-axis: op_survival_rate
- Bars: `slx` vs `slx-l`

**Graph 8: DP3 Verifier Rejection Breakdown**
- X-axis: Verification stage (V0-V6)
- Y-axis: rejection count
- Shows where LLM proposals fail most → guides prompt improvement

---

## 18. MVP Checklist

For the first implementation:
- [ ] Use DP3 only for `FAIL → NOOP` and `FAIL → PATCH` (no TRANSFORM)
- [ ] Use JSON merge-patch only (no JSON-patch / RFC 6902)
- [ ] Enforce strict schema + `DisallowUnknownFields`
- [ ] Enforce response size (64KB) and patch depth (16) caps
- [ ] `--llm-timeout-ms` default 5000ms, always fall back to DP2
- [ ] Test with Workload D (Inventory) as primary validation
- [ ] Verify 0 invariant violations across all runs

Post-MVP:
- [ ] Add TRANSFORM for whitelisted safe transforms (per workload)
- [ ] Add optional digest verification at appliers
- [ ] Add sampling/budget logic (only invoke DP3 when FAIL rate > threshold)
- [ ] Add retry with structured feedback on verifier rejection
