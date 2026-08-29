//! The index engine: keyspaces of holders over a shared
//! [`PositionalIndexer`], with epoch-scoped holder state, per-feed
//! eviction semantics, and overlap queries. Pure and synchronous — the
//! gRPC surface, relay, and bootstrap live in `server.rs`.
//!
//! Correctness model (see `.claude/kv-index-service/01-design.md`):
//! - Event feed: one worker-sequenced stream per (holder, epoch); apply
//!   in order, dedup on `seq <= last_seq`, batch-shaped (one seq may mix
//!   Stored and Removed).
//! - Placement feed: unsequenced (`seq == 0`), idempotent by content —
//!   publishers synthesize identical chains for identical prefixes, so
//!   cross-publisher ordering never matters.
//! - Epochs: a higher epoch clears all lower-epoch state for the holder;
//!   lower-epoch updates are dropped. Restarts and cursor loss are both
//!   epoch bumps, which is what makes relaying `Cleared` safe.
//! - Feed authority: a holder becomes event-fed on its first observed
//!   removal (or an explicit `Added { event_fed: true }`); inferred
//!   Stored updates for an event-fed holder are dropped.
//! - Inferred eviction is tail-first per holder (prefix-closed, which
//!   the jump search assumes), by capacity in blocks with a TTL floor
//!   that retires whole idle holders.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use kv_index::{
    ContentHash, PositionalIndexer, SequenceHash, StoredBlock, WorkerBlockMap, WorkerId,
};

/// One block on the wire: position-chained identity + content identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireBlock {
    pub seq_hash: SequenceHash,
    pub content_hash: ContentHash,
}

/// A cache transition within one update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireEvent {
    Stored {
        parent: Option<SequenceHash>,
        blocks: Vec<WireBlock>,
    },
    Removed {
        seq_hashes: Vec<SequenceHash>,
    },
    Cleared,
}

/// Membership / capacity control payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddedControl {
    pub capacity_blocks: u64,
    pub event_fed: bool,
}

/// One `Publish` message, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateMsg {
    pub keyspace: KeyspaceKey,
    pub holder: String,
    pub epoch: u64,
    /// 0 = unsequenced (placement / control), else the holder's batch seq.
    pub seq: u64,
    pub events: Vec<WireEvent>,
    pub added: Option<AddedControl>,
    pub dropped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    /// Dropped as a duplicate / stale seq or stale epoch.
    Deduped,
    /// Inferred Stored dropped because the holder is event-fed.
    FeedRejected,
    /// Keyspace block_size conflict — publisher misconfigured.
    KeyspaceMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SymbolKind {
    Tokens,
    Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyspaceKey {
    pub model: String,
    pub symbol_kind: SymbolKind,
    pub block_size: u32,
}

/// Per-holder score in a query answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderScore {
    pub holder: String,
    pub matched_blocks: u32,
    pub total_blocks: u64,
    pub event_fed: bool,
}

pub struct EngineConfig {
    /// Jump stride for the positional index query search.
    pub jump_size: usize,
    /// Idle TTL for INFERRED holder state; an inferred holder with no
    /// publish inside the window is cleared entirely (coarse but
    /// prefix-closed). Event-fed holders are never TTL'd — their
    /// eviction is observed.
    pub inferred_ttl: Duration,
    /// Default capacity (blocks) for inferred holders that never sent
    /// `Added`.
    pub default_capacity_blocks: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            jump_size: 64,
            inferred_ttl: Duration::from_secs(180),
            default_capacity_blocks: u64::MAX,
        }
    }
}

struct HolderState {
    worker_id: WorkerId,
    epoch: u64,
    last_seq: u64,
    event_fed: bool,
    capacity_blocks: u64,
    dropped: bool,
    /// Reverse map the indexer's write path requires (caller-owned).
    blocks: WorkerBlockMap,
    /// Position-ordered chain, maintained for INFERRED holders only, so
    /// capacity eviction can pop the tail and keep the prefix closed.
    /// Event-fed holders evict wherever their engine says.
    chain: Vec<SequenceHash>,
    last_publish: Instant,
}

struct KeyspaceState {
    indexer: PositionalIndexer,
    holders: HashMap<String, HolderState>,
    by_worker_id: HashMap<WorkerId, String>,
}

/// The engine: all keyspaces. One lock over everything — apply and query
/// rates in scope (hundreds of k ops/s) are far below contention range
/// for the critical sections involved, and single-lock semantics make
/// the convergence argument checkable.
pub struct Engine {
    cfg: EngineConfig,
    keyspaces: std::sync::Mutex<HashMap<KeyspaceKey, KeyspaceState>>,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        Self {
            cfg,
            keyspaces: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Apply one update. Returns what happened, plus the holder's applied
    /// watermark for the publisher's ack.
    pub fn apply(&self, update: &UpdateMsg) -> (ApplyOutcome, u64) {
        let mut spaces = self.keyspaces.lock().unwrap_or_else(|p| p.into_inner());

        // A keyspace is created on first contact; a later publisher whose
        // key differs only in block_size is a DIFFERENT keyspace by key
        // construction, so mismatch cannot silently merge. Reject only
        // the degenerate block size.
        if update.keyspace.block_size == 0 {
            return (ApplyOutcome::KeyspaceMismatch, 0);
        }
        let KeyspaceState {
            indexer,
            holders,
            by_worker_id,
        } = spaces
            .entry(update.keyspace.clone())
            .or_insert_with(|| KeyspaceState {
                indexer: PositionalIndexer::new(self.cfg.jump_size),
                holders: HashMap::new(),
                by_worker_id: HashMap::new(),
            });

        if !holders.contains_key(&update.holder) {
            let Ok(worker_id) = indexer.intern_worker(&update.holder) else {
                return (ApplyOutcome::KeyspaceMismatch, 0);
            };
            by_worker_id.insert(worker_id, update.holder.clone());
            holders.insert(
                update.holder.clone(),
                HolderState {
                    worker_id,
                    epoch: update.epoch,
                    last_seq: 0,
                    event_fed: false,
                    capacity_blocks: self.cfg.default_capacity_blocks,
                    dropped: false,
                    blocks: WorkerBlockMap::default(),
                    chain: Vec::new(),
                    last_publish: Instant::now(),
                },
            );
        }
        let holder = holders
            .get_mut(&update.holder)
            .expect("holder inserted above");

        // Epoch gate: higher epoch supersedes (implicit clear), lower is
        // dropped, equal proceeds.
        if update.epoch > holder.epoch {
            indexer.apply_cleared(holder.worker_id, &mut holder.blocks);
            holder.chain.clear();
            holder.epoch = update.epoch;
            holder.last_seq = 0;
        } else if update.epoch < holder.epoch {
            return (ApplyOutcome::Deduped, holder.last_seq);
        }
        holder.last_publish = Instant::now();

        if let Some(added) = &update.added {
            holder.capacity_blocks = if added.capacity_blocks == 0 {
                self.cfg.default_capacity_blocks
            } else {
                added.capacity_blocks
            };
            if added.event_fed {
                holder.event_fed = true;
            }
            holder.dropped = false;
        }
        if update.dropped {
            holder.dropped = true;
        }

        // Sequenced = event feed; unsequenced = placement/control.
        let sequenced = update.seq != 0;
        if sequenced {
            if update.seq <= holder.last_seq {
                return (ApplyOutcome::Deduped, holder.last_seq);
            }
            holder.last_seq = update.seq;
        }

        let mut outcome = ApplyOutcome::Applied;
        for event in &update.events {
            match event {
                WireEvent::Stored { parent, blocks } => {
                    if !sequenced && holder.event_fed {
                        // D4: placements never pollute observed holders.
                        outcome = ApplyOutcome::FeedRejected;
                        continue;
                    }
                    if sequenced && !holder.event_fed {
                        // First sequenced traffic pins feed authority too:
                        // a holder with a real event stream is observed.
                        holder.event_fed = true;
                        holder.chain.clear();
                    }
                    let stored: Vec<StoredBlock> = blocks
                        .iter()
                        .map(|b| StoredBlock {
                            seq_hash: b.seq_hash,
                            content_hash: b.content_hash,
                        })
                        .collect();
                    let applied = indexer
                        .apply_stored(holder.worker_id, &stored, *parent, &mut holder.blocks)
                        .or_else(|_| {
                            // Unresolvable parent re-anchors at position 0,
                            // mirroring the gateway monitor's recovery.
                            indexer.apply_stored(
                                holder.worker_id,
                                &stored,
                                None,
                                &mut holder.blocks,
                            )
                        });
                    if applied.is_ok() && !holder.event_fed {
                        Self::extend_chain(holder, blocks);
                        Self::evict_tail(indexer, holder);
                    }
                }
                WireEvent::Removed { seq_hashes } => {
                    if !holder.event_fed {
                        holder.event_fed = true;
                        holder.chain.clear();
                    }
                    indexer.apply_removed(holder.worker_id, seq_hashes, &mut holder.blocks);
                }
                WireEvent::Cleared => {
                    indexer.apply_cleared(holder.worker_id, &mut holder.blocks);
                    holder.chain.clear();
                }
            }
        }
        (outcome, holder.last_seq)
    }

    /// Position-ordered chain bookkeeping for inferred holders. The
    /// stored blocks landed at parent_pos+1.. (or 0): reflect that in the
    /// tail vector so capacity eviction stays prefix-closed.
    fn extend_chain(holder: &mut HolderState, blocks: &[WireBlock]) {
        let Some(first) = blocks.first() else { return };
        let Some(&(start_pos, _, _)) = holder.blocks.get(&first.seq_hash) else {
            return;
        };
        if holder.chain.len() < start_pos {
            // A gap should be impossible for deterministic placement
            // chains; anchor defensively at the current tail.
            return;
        }
        holder.chain.truncate(start_pos);
        holder.chain.extend(blocks.iter().map(|b| b.seq_hash));
    }

    fn evict_tail(indexer: &PositionalIndexer, holder: &mut HolderState) {
        let capacity = usize::try_from(holder.capacity_blocks).unwrap_or(usize::MAX);
        while holder.chain.len() > capacity {
            let Some(tail) = holder.chain.pop() else {
                break;
            };
            indexer.apply_removed(holder.worker_id, &[tail], &mut holder.blocks);
        }
    }

    /// TTL sweep: clear inferred holders idle beyond the window. Cheap
    /// (per-holder timestamps only); run from a timer.
    pub fn sweep_idle(&self) {
        let ttl = self.cfg.inferred_ttl;
        let mut spaces = self.keyspaces.lock().unwrap_or_else(|p| p.into_inner());
        for space in spaces.values_mut() {
            let KeyspaceState {
                indexer, holders, ..
            } = space;
            for holder in holders.values_mut() {
                if !holder.event_fed && holder.last_publish.elapsed() > ttl {
                    indexer.apply_cleared(holder.worker_id, &mut holder.blocks);
                    holder.chain.clear();
                }
            }
        }
    }

    /// Overlap query: per-holder matched prefix depth, dropped holders
    /// excluded. Missing keyspace = empty answer (advisory semantics).
    pub fn find_matches(&self, keyspace: &KeyspaceKey, hashes: &[ContentHash]) -> Vec<HolderScore> {
        let spaces = self.keyspaces.lock().unwrap_or_else(|p| p.into_inner());
        let Some(space) = spaces.get(keyspace) else {
            return Vec::new();
        };
        let overlap = space.indexer.find_matches(hashes, false);
        let mut scores = Vec::with_capacity(overlap.scores.len());
        for (worker_id, matched) in overlap.scores {
            let Some(holder_key) = space.by_worker_id.get(&worker_id) else {
                continue;
            };
            let Some(holder) = space.holders.get(holder_key) else {
                continue;
            };
            if holder.dropped || matched == 0 {
                continue;
            }
            scores.push(HolderScore {
                holder: holder_key.clone(),
                matched_blocks: matched,
                total_blocks: overlap
                    .tree_sizes
                    .get(&worker_id)
                    .copied()
                    .unwrap_or_default() as u64,
                event_fed: holder.event_fed,
            });
        }
        scores.sort_by_key(|s| std::cmp::Reverse(s.matched_blocks));
        scores
    }

    /// Serialize current state as synthetic Updates (for `Pull`): one
    /// Stored per holder carrying its full chain in position order, under
    /// the holder's current epoch, unsequenced-for-inferred /
    /// watermark-seq-for-observed so the puller lands with the same
    /// dedup posture.
    pub fn snapshot(&self) -> Vec<UpdateMsg> {
        let spaces = self.keyspaces.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = Vec::new();
        for (key, space) in spaces.iter() {
            for (holder_key, holder) in &space.holders {
                // Reconstruct position order from the reverse map.
                let mut ordered: Vec<(usize, SequenceHash, ContentHash)> = holder
                    .blocks
                    .iter()
                    .map(|(&seq, &(pos, content, _))| (pos, seq, content))
                    .collect();
                ordered.sort_by_key(|&(pos, _, _)| pos);
                let blocks: Vec<WireBlock> = ordered
                    .iter()
                    .map(|&(_, seq_hash, content_hash)| WireBlock {
                        seq_hash,
                        content_hash,
                    })
                    .collect();
                out.push(UpdateMsg {
                    keyspace: key.clone(),
                    holder: holder_key.clone(),
                    epoch: holder.epoch,
                    seq: if holder.event_fed { holder.last_seq } else { 0 },
                    events: if blocks.is_empty() {
                        Vec::new()
                    } else {
                        vec![WireEvent::Stored {
                            parent: None,
                            blocks,
                        }]
                    },
                    added: Some(AddedControl {
                        capacity_blocks: holder.capacity_blocks,
                        event_fed: holder.event_fed,
                    }),
                    dropped: holder.dropped,
                });
            }
        }
        out
    }

    /// Total indexed blocks across keyspaces (stats/tests).
    pub fn entry_count(&self) -> usize {
        let spaces = self.keyspaces.lock().unwrap_or_else(|p| p.into_inner());
        spaces.values().map(|s| s.indexer.entry_count()).sum()
    }

    /// Point-in-time gauges for the metrics endpoint. One pass under the
    /// engine lock; cheap relative to apply/query traffic.
    pub fn stats(&self) -> EngineStats {
        let spaces = self.keyspaces.lock().unwrap_or_else(|p| p.into_inner());
        let mut stats = EngineStats {
            keyspaces: spaces.len(),
            ..EngineStats::default()
        };
        for space in spaces.values() {
            stats.blocks += space.indexer.entry_count();
            for holder in space.holders.values() {
                stats.holders += 1;
                if holder.event_fed {
                    stats.event_fed_holders += 1;
                }
                if holder.dropped {
                    stats.dropped_holders += 1;
                }
            }
        }
        stats
    }
}

/// Point-in-time engine gauges (see [`Engine::stats`]).
#[derive(Debug, Clone, Copy, Default)]
pub struct EngineStats {
    pub keyspaces: usize,
    pub holders: usize,
    pub event_fed_holders: usize,
    pub dropped_holders: usize,
    pub blocks: usize,
}

/// Deterministic placement chain: content hashes -> position-chained
/// wire blocks, using the indexer's own rolling prefix hash so every
/// publisher synthesizes byte-identical chains for identical prefixes.
pub fn placement_chain(content_hashes: &[ContentHash]) -> Vec<WireBlock> {
    let mut out = Vec::with_capacity(content_hashes.len());
    let mut prev = SequenceHash(0);
    for (i, &content_hash) in content_hashes.iter().enumerate() {
        let seq_hash = if i == 0 {
            SequenceHash(content_hash.0)
        } else {
            kv_index::chain_prefix_hash(prev, content_hash)
        };
        out.push(WireBlock {
            seq_hash,
            content_hash,
        });
        prev = seq_hash;
    }
    out
}

#[cfg(test)]
mod tests {
    use kv_index::compute_content_hash;
    use rand::{rngs::StdRng, seq::SliceRandom, RngExt, SeedableRng};

    use super::*;

    fn keyspace() -> KeyspaceKey {
        KeyspaceKey {
            model: "m".into(),
            symbol_kind: SymbolKind::Tokens,
            block_size: 4,
        }
    }

    /// Content hashes for a synthetic prefix: block i hashes tokens
    /// [seed, i] so shared prefixes share leading hashes.
    fn prefix_hashes(seed: u32, blocks: usize) -> Vec<ContentHash> {
        (0..blocks as u32)
            .map(|i| compute_content_hash(&[seed, i]))
            .collect()
    }

    fn placement(holder: &str, seed: u32, blocks: usize) -> UpdateMsg {
        UpdateMsg {
            keyspace: keyspace(),
            holder: holder.into(),
            epoch: 1,
            seq: 0,
            events: vec![WireEvent::Stored {
                parent: None,
                blocks: placement_chain(&prefix_hashes(seed, blocks)),
            }],
            added: None,
            dropped: false,
        }
    }

    fn event_batch(holder: &str, seq: u64, events: Vec<WireEvent>) -> UpdateMsg {
        UpdateMsg {
            keyspace: keyspace(),
            holder: holder.into(),
            epoch: 1,
            seq,
            events,
            added: None,
            dropped: false,
        }
    }

    fn scores(engine: &Engine, seed: u32, blocks: usize) -> Vec<(String, u32)> {
        engine
            .find_matches(&keyspace(), &prefix_hashes(seed, blocks))
            .into_iter()
            .map(|s| (s.holder, s.matched_blocks))
            .collect()
    }

    #[test]
    fn placement_chain_is_deterministic_and_positions_match() {
        let hashes = prefix_hashes(7, 6);
        assert_eq!(placement_chain(&hashes), placement_chain(&hashes));
        let engine = Engine::new(EngineConfig::default());
        engine.apply(&placement("w1", 7, 6));
        assert_eq!(scores(&engine, 7, 6), vec![("w1".into(), 6)]);
        // A shorter shared prefix matches its depth only.
        assert_eq!(scores(&engine, 7, 3), vec![("w1".into(), 3)]);
    }

    #[test]
    fn placements_are_idempotent_across_publishers() {
        let a = Engine::new(EngineConfig::default());
        let b = Engine::new(EngineConfig::default());
        // "Gateways" 1..4 place the same prefix repeatedly and extensions
        // of it, in different orders per replica.
        let mut updates = Vec::new();
        for _publisher in 0..4 {
            updates.push(placement("w1", 9, 4));
            updates.push(placement("w1", 9, 8)); // extension
            updates.push(placement("w2", 9, 2)); // shorter copy elsewhere
        }
        let mut rng = StdRng::seed_from_u64(1);
        let mut for_a = updates.clone();
        let mut for_b = updates.clone();
        for_a.shuffle(&mut rng);
        for_b.shuffle(&mut rng);
        for u in &for_a {
            a.apply(u);
        }
        for u in &for_b {
            b.apply(u);
        }
        assert_eq!(scores(&a, 9, 8), scores(&b, 9, 8));
        assert_eq!(a.entry_count(), b.entry_count());
        // And identical to the once-only application.
        let once = Engine::new(EngineConfig::default());
        once.apply(&placement("w1", 9, 8));
        once.apply(&placement("w2", 9, 2));
        assert_eq!(scores(&once, 9, 8), scores(&a, 9, 8));
    }

    #[test]
    fn replicas_converge_under_interleaving_and_duplication() {
        // Event holders: per-holder order preserved (the stream contract);
        // cross-holder interleaving and duplicates are free game.
        // Placement holders: any order, any duplication.
        let mut rng = StdRng::seed_from_u64(42);
        let mut per_holder: Vec<Vec<UpdateMsg>> = Vec::new();
        for h in 0..4u32 {
            let holder = format!("ev{h}");
            let mut seqs = Vec::new();
            let mut chain = placement_chain(&prefix_hashes(100 + h, 12));
            for (i, window) in chain.chunks(3).enumerate() {
                let parent = if i == 0 {
                    None
                } else {
                    Some(chain[i * 3 - 1].seq_hash)
                };
                let mut events = vec![WireEvent::Stored {
                    parent,
                    blocks: window.to_vec(),
                }];
                // Mixed batch: occasionally remove an old tail block in the
                // same seq as a store.
                if i == 3 {
                    events.push(WireEvent::Removed {
                        seq_hashes: vec![chain[11].seq_hash],
                    });
                }
                seqs.push(event_batch(&holder, (i + 1) as u64, events));
            }
            chain.clear();
            per_holder.push(seqs);
        }
        let placements: Vec<UpdateMsg> = (0..6u32)
            .map(|p| placement(&format!("pl{}", p % 2), 200 + (p % 3), 4 + (p % 5) as usize))
            .collect();

        let deliver = |engine: &Engine, rng: &mut StdRng| {
            // Merge per-holder event queues preserving each holder's order.
            let mut cursors = vec![0usize; per_holder.len()];
            let mut pending_placements = placements.clone();
            loop {
                let live: Vec<usize> = cursors
                    .iter()
                    .enumerate()
                    .filter(|(h, &c)| c < per_holder[*h].len())
                    .map(|(h, _)| h)
                    .collect();
                if live.is_empty() && pending_placements.is_empty() {
                    break;
                }
                if !pending_placements.is_empty() && (live.is_empty() || rng.random_bool(0.4)) {
                    let i = rng.random_range(0..pending_placements.len());
                    let u = pending_placements.swap_remove(i);
                    engine.apply(&u);
                    if rng.random_bool(0.3) {
                        engine.apply(&u); // duplicate
                    }
                } else {
                    let h = live[rng.random_range(0..live.len())];
                    let u = &per_holder[h][cursors[h]];
                    engine.apply(u);
                    if rng.random_bool(0.3) {
                        engine.apply(u); // duplicate (deduped by seq)
                    }
                    cursors[h] += 1;
                }
            }
        };

        let a = Engine::new(EngineConfig::default());
        let b = Engine::new(EngineConfig::default());
        deliver(&a, &mut rng);
        deliver(&b, &mut rng);
        for h in 0..4u32 {
            assert_eq!(
                scores(&a, 100 + h, 12),
                scores(&b, 100 + h, 12),
                "event holder {h} diverged"
            );
        }
        for p in 0..3u32 {
            assert_eq!(scores(&a, 200 + p, 8), scores(&b, 200 + p, 8));
        }
        assert_eq!(a.entry_count(), b.entry_count());
    }

    #[test]
    fn higher_epoch_clears_lower_and_stale_epochs_drop() {
        let engine = Engine::new(EngineConfig::default());
        engine.apply(&placement("w1", 5, 6));
        assert_eq!(scores(&engine, 5, 6), vec![("w1".into(), 6)]);

        // Restarted holder announces epoch 2 with a fresh (shorter) cache.
        let mut restarted = placement("w1", 5, 2);
        restarted.epoch = 2;
        let (outcome, _) = engine.apply(&restarted);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(scores(&engine, 5, 6), vec![("w1".into(), 2)]);

        // A late epoch-1 update (relay stragglers) is dropped.
        let (outcome, _) = engine.apply(&placement("w1", 5, 6));
        assert_eq!(outcome, ApplyOutcome::Deduped);
        assert_eq!(scores(&engine, 5, 6), vec![("w1".into(), 2)]);
    }

    #[test]
    fn tail_eviction_keeps_prefixes_closed() {
        let engine = Engine::new(EngineConfig {
            default_capacity_blocks: 4,
            ..EngineConfig::default()
        });
        engine.apply(&placement("w1", 3, 8));
        // Head survives, tail evicted: prefix-closed at depth 4.
        assert_eq!(scores(&engine, 3, 8), vec![("w1".into(), 4)]);
        assert_eq!(engine.entry_count(), 4);
    }

    #[test]
    fn event_fed_holders_reject_placements_and_split_batches_do_not_lose_events() {
        let engine = Engine::new(EngineConfig::default());
        let chain = placement_chain(&prefix_hashes(11, 6));
        // One seq carrying Stored + Removed together (engine batch shape).
        engine.apply(&event_batch(
            "w1",
            1,
            vec![
                WireEvent::Stored {
                    parent: None,
                    blocks: chain.clone(),
                },
                WireEvent::Removed {
                    seq_hashes: vec![chain[5].seq_hash],
                },
            ],
        ));
        assert_eq!(scores(&engine, 11, 6), vec![("w1".into(), 5)]);

        // A placement for the now event-fed holder is rejected.
        let (outcome, _) = engine.apply(&placement("w1", 12, 3));
        assert_eq!(outcome, ApplyOutcome::FeedRejected);
        assert!(scores(&engine, 12, 3).is_empty());

        // Duplicate seq is deduped even with different content.
        engine.apply(&event_batch(
            "w1",
            1,
            vec![WireEvent::Removed {
                seq_hashes: vec![chain[4].seq_hash],
            }],
        ));
        assert_eq!(scores(&engine, 11, 6), vec![("w1".into(), 5)]);
    }

    #[test]
    fn snapshot_bootstrap_reproduces_answers() {
        let a = Engine::new(EngineConfig::default());
        a.apply(&placement("w1", 21, 5));
        let chain = placement_chain(&prefix_hashes(22, 4));
        a.apply(&event_batch(
            "w2",
            3,
            vec![WireEvent::Stored {
                parent: None,
                blocks: chain,
            }],
        ));

        let b = Engine::new(EngineConfig::default());
        for update in a.snapshot() {
            b.apply(&update);
        }
        assert_eq!(scores(&a, 21, 5), scores(&b, 21, 5));
        assert_eq!(scores(&a, 22, 4), scores(&b, 22, 4));
        assert_eq!(a.entry_count(), b.entry_count());

        // The bootstrapped replica keeps the event holder's dedup posture:
        // the watermark seq travels, so a replayed old batch is dropped.
        let (outcome, _) = b.apply(&event_batch("w2", 2, vec![WireEvent::Cleared]));
        assert_eq!(outcome, ApplyOutcome::Deduped);
    }
}
