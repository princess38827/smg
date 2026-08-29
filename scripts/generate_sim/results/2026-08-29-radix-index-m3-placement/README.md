# radix-index M3 — placement-feed revalidation (prompt⊕output chains)

One leg, rerun of the M2 core matrix's `remote-placement-sprayed`
(shared index fed ONLY by gateway placements, bridge off) after the
placement-chain fix: the gateway now publishes the hash chain of
prompt ⊕ output token ids at request completion, instead of the
routing-time prompt-only chain. M2 had located the placement feed's
1.6-point deficit exactly there — the worker's KV blocks after
generation span the generated tail, so follow-up turns under-matched
by the previous output (prediction error p95 −3840 tokens ≈ the
output-length CDF).

Setup identical to M2: `local-small` profile, sprayed ingress, tree
index, no sticky override, non-streaming, 3 seeds, Student-t CIs,
steady-state windows. Scenario: `remote-index-placement` (single leg,
same override dict as the M2 leg).

## Result: the located gap closes

| metric | M2 (prompt-only) | M3 (prompt⊕output) | event-fed reference (M2) |
|---|---|---|---|
| follow-up cached Σ/Σ | 0.9266 ±0.0045 | **0.9398 ±0.0028** | 0.9423 ±0.0028 |
| t2 same-worker | 0.8960 ±0.0059 | **0.9696 ±0.0068** | 0.9786 |
| turn2 hit rate | 0.9816 | 0.9925 | — |
| prediction error mean (tokens) | −640.5 | −12.6 | ≈0 |
| prediction error p95 abs (tokens) | 3840 | **0** | 0 |
| AGG cached Σ/Σ | 0.4646 | 0.4694 | 0.4704 |
| overall fleet CoV | 0.049 | 0.046 | 0.087 |
| e2e p50 (ms) | 7766 | 7765 | — |

- ~85% of the placement-vs-event deficit is gone: 1.6 points → 0.25
  points on follow-up cached tokens, with the same request count and
  latency. The residual ~0.25 points is the feed's intrinsic remainder
  (TTL retirement between turns and capacity-model coarseness), not a
  measurement artifact — prediction error is now exact for 95% of
  requests.
- Same-worker follow-up routing recovered from 0.896 to 0.970 (the
  event feed sits at 0.979): under-claimed overlap was losing the
  affinity comparison against load terms, not just shaving matched
  blocks.
- The eventless-engine thesis strengthens: a fleet with NO KV event
  stream at all now routes within a quarter point of the event-fed
  index on cached tokens.

CIs here are the 95% Student-t half-widths reported by the harness
(the table above quotes them for the headline rows; per-seed values in
`remote-placement-sprayed/seed-rows.json`).

Files: `seed-rows.json` (3 seeds), `meta-seed42.json` (binary sha256s,
registration/readiness), `report-seed42.json` (full per-leg report).
Binary provenance: built at the M3 fixes commit (prompt⊕output publish
+ fast-fail client); the M2 rows were pre-fix by construction (sha in
the M2 meta files differs).
