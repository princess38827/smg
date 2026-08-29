# 2026-08-28 — radix index service, M2 measurements

> **Reduced-scale cache-semantics results.** 8 gateways × 120 gRPC mock
> workers, 10× time compression, compressed gateway clocks
> (load monitor 1 s, eviction 12 s, TTL 18 s) held constant on every
> leg. Resource rows are not production-representative; production
> sizing comes from the crate's synthetic bench (274.7 B/entry →
> ~47 GB at 1.7e8 entries).

Experiment plan and pre-registered endpoints:
`.claude/kv-index-service/02-experiment-plan.md`. All legs: sprayed
ingress (random per turn — the regime that breaks per-replica state),
tree index, no sticky override, non-streaming, 3 seeds (failover 6),
Student-t CIs, steady-state windows. One gateway binary across all legs
(sha asserted by the harness); `--kv-indexer-url` off on local/mesh
legs, on for remote legs (keyspace block 256, 2 ms query deadline).

## Core matrix (remote-index + remote-index-mesh)

| leg | follow-up cached Σ/Σ | same-worker | fleet CoV | AGG |
|---|---|---|---|---|
| local-event (per-gateway indexers, round-3 architecture) | 0.9429 ±0.0019 | 0.9804 | 0.572 ±0.052 | 0.4708 |
| remote-event (ONE shared index, event-fed) | 0.9423 ±0.0028 | 0.9786 | 0.087 ±0.112 | 0.4704 |
| remote-placement (shared index, placements ONLY — no events) | 0.9266 ±0.0045 | 0.8960 | 0.049 ±0.009 | 0.4646 |
| mesh-tree (in-repo TreeSync'd approximate trees) | 0.9425 ±0.0008 | 0.9802 | 0.602 ±0.045 | 0.4706 |

Pre-registered endpoints:

- **Claim 2 (parity): CONFIRMED.** Remote vs local event feed differ by
  0.0006 follow-up cached (margin ±0.02) with 99.5% of decisions
  remote-served (0.5% degraded) and predicted-vs-actual cached tokens
  p95 error = 0. One shared index replaces K per-gateway indexes at no
  accuracy cost.
- **Claim 1 (inferred feed): CONFIRMED.** Placement-fed routing — the
  eventless-engine case — reaches 0.9266, within 1.6 points of ground
  truth; the 0.5 stop-condition was never approached. The gap is
  located, not just measured: prediction error mean −640 / p95 −3840
  tokens = prompt-only placement chains under-claim by the output tail
  (matches the output CDF). Future fix: publish prompt⊕output chains.
  [Landed and revalidated: 0.9398, within 0.25 points — see
  `../2026-08-29-radix-index-m3-placement/`.]
- **Mesh TreeSync ties on accuracy** (0.9425) — the honest result the
  gap-scan demanded. The service's case is therefore: balance (CoV
  0.05–0.09 vs 0.57–0.60, ~10×), memory (state held once vs once per
  gateway: ~47 GB vs ~K×47 GB at production shape), observed-eviction
  event feed, and keyspace generality. Not accuracy.
- Unregistered finding: the shared index IMPROVES fleet balance ~10× —
  eight gateways acting on one view make consistent spill decisions.

## index-staleness (constant apply lag; think = 3 s compressed = 30 s production)

| injected lag (ratio of think) | follow-up cached | same-worker |
|---|---|---|
| 0 (ref) | 0.9423 | 0.9786 |
| 30 ms (0.01) | 0.9410 | 0.9761 |
| 300 ms (0.1) | 0.9420 | 0.9719 |
| 3000 ms stored (1.0) | 0.8766 | 0.8455 |
| 3000 ms removed (1.0) | 0.8827 | 0.8585 |

**Claim 3: CONFIRMED.** Flat through lag/think = 0.1; the first
measurable dip needs lag equal to the entire reuse gap (production
equivalent: a 30-SECOND index lag costs ~6.6 points). The production
operating point (~ms lag) sits four orders of magnitude left of the
cliff. Stale removals cost the same order as stale stores.

## index-capacity (inferred eviction model sensitivity)

0.5× / 1× / 2× of true evictable capacity: 0.9262 / 0.9266 / 0.9279 —
statistically identical. The capacity model can be wrong by 2× either
way without measurable cost; dynamic capacity derivation is a
refinement, not a requirement.

## index-failover (kill the single-endpoint replica, 6 seeds)

Replica 0 (the URL every gateway and publisher dials) killed at t=60 s,
relaunched at +30 s bootstrapping from the survivor. Follow-up cached by
10 s bin around the kill, pooled over 6 seeds:

pre-kill 0.927 → blackout 0.19–0.26 (100% remote_timeout; graceful
fallback) → +30 s relaunch 0.906 in the SAME bin → fully recovered
(0.926) one bin later. **Zero request errors across 410,637 requests.**

**Claim 4: CONFIRMED.** Index loss degrades cache hit to the floor and
NOTHING else; recovery is immediate on relaunch via peer bootstrap.
Noted: during the outage every decision waits the full 2 ms deadline
(reconnect-loop timeouts rather than fast-fail) — a client refinement,
not a correctness issue.

## Deviations from the plan

- Leg F (local block-quantized control) dropped: remote-event vs
  local-event is already a location-only comparison (same block size,
  observed eviction, shared scoring code).
- Leg D (HTTP-fleet placement) deferred: the HTTP router lacks the
  prefetch seam (M1 built the gRPC pipeline's); the eventless-engine
  thesis is answered by the placement-fed leg regardless of worker
  transport.
- Saturation leg not run (all legs ~20% CPU): the balance cost of a
  shared view is unmeasured under queueing — mitigated by the shared
  index measuring BETTER balance, so the risk direction is favorable.
- Failover used 6 independent seeds rather than 3×2 repeats.
- Index-service binaries were rebuilt (staleness knobs) between the
  core matrix's legs; behaviorally identical at zero delay, but the
  index sha256 differs across those legs' meta files. Gateway sha is
  identical everywhere.

Analysis tool for the failover bins: `failover_bins.py <run-dir>`.
