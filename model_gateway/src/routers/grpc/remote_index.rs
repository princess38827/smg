//! Remote radix index access (`--kv-indexer-url`): a per-process client
//! handle carried on `AppContext` and threaded into the gRPC pipelines.
//! With the flag unset the handle is `None` and every caller's fast path
//! is a `None` check.

use std::{sync::Arc, time::Duration};

use radix_index::client::{QueryOutcome, RemoteIndex};

/// Hard deadline for the routing-time overlap query; a miss falls back
/// to expected-wait for that one decision.
pub(crate) const QUERY_DEADLINE: Duration = Duration::from_millis(2);

/// The connected remote-index client plus the keyspace block size it was
/// configured for. One per process, owned by `AppContext`.
pub struct RemoteIndexHandle {
    client: Arc<RemoteIndex>,
    block_size: usize,
}

impl RemoteIndexHandle {
    /// `block_size` is the KEYSPACE block size — the engine-side page
    /// size the index was fed at (worker events / bridge `--block-size`),
    /// not the routing block.
    pub fn connect(url: &str, block_size: usize) -> Arc<Self> {
        Arc::new(Self {
            client: RemoteIndex::connect(url.to_string()),
            block_size: block_size.max(1),
        })
    }

    pub(crate) fn client(&self) -> &RemoteIndex {
        &self.client
    }

    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }
}

impl std::fmt::Debug for RemoteIndexHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteIndexHandle")
            .field("block_size", &self.block_size)
            .finish_non_exhaustive()
    }
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
