//! Remote radix index access (`--kv-indexer-url`), experiment-scoped:
//! process-global handle set once at startup; M3 plumbs it through
//! AppContext properly. With the flag unset nothing here runs and every
//! caller's fast path is a `None` check.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use radix_index::client::{QueryOutcome, RemoteIndex};

static REMOTE: OnceLock<Arc<RemoteIndex>> = OnceLock::new();
static BLOCK: OnceLock<usize> = OnceLock::new();

/// Hard deadline for the routing-time overlap query; a miss falls back
/// to expected-wait for that one decision.
pub(crate) const QUERY_DEADLINE: Duration = Duration::from_millis(2);

/// Idempotent: the first URL wins (one client per process). `block_size`
/// is the KEYSPACE block size — the engine-side page size the index was
/// fed at (worker events / bridge `--block-size`), not the routing block.
pub(crate) fn init(url: &str, block_size: usize) {
    let _ = BLOCK.set(block_size.max(1));
    let _ = REMOTE.set(RemoteIndex::connect(url.to_string()));
}

pub(crate) fn block_size() -> usize {
    BLOCK.get().copied().unwrap_or(128)
}

pub(crate) fn get() -> Option<&'static Arc<RemoteIndex>> {
    REMOTE.get()
}

/// What the selection-stage prefetch resolved, kept on the request
/// context for the placement publish and the response echo headers.
#[derive(Debug, Clone)]
pub(crate) struct IndexPrediction {
    /// remote_hit | remote_empty | remote_timeout | remote_disconnected
    pub source: &'static str,
    /// Per-holder (url, matched blocks) as answered (empty on non-hit).
    pub scores: Vec<(String, u32)>,
    pub block_size: usize,
    /// The request prefix's content hashes (block-aligned from 0),
    /// republished as the placement chain after successful dispatch.
    pub content_hashes: Vec<u64>,
    pub model: String,
}

pub(crate) fn outcome_label(outcome: &QueryOutcome) -> &'static str {
    match outcome {
        QueryOutcome::Scores(_) => "remote_hit",
        QueryOutcome::Empty => "remote_empty",
        QueryOutcome::Timeout => "remote_timeout",
        QueryOutcome::Disconnected => "remote_disconnected",
    }
}
