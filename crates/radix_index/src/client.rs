//! Gateway-side client: one persistent Subscribe stream for queries
//! (query_id-correlated, caller-enforced deadline) and one Publish
//! stream for fire-and-forget placements. Both reconnect forever in
//! background drivers; a query during an outage resolves Disconnected
//! and the caller falls through to its local fallback.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::StreamExt;
use kv_index::ContentHash;
use tokio::sync::{mpsc, oneshot};

use crate::{
    bridge,
    engine::placement_chain,
    proto::{self, radix_index_client::RadixIndexClient},
};

/// What a routing-time query resolved to; the caller maps this onto its
/// fallback ladder and metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOutcome {
    /// Per-holder (url, matched_blocks), descending.
    Scores(Vec<(String, u32)>),
    /// The index answered with no overlap.
    Empty,
    /// Deadline elapsed; the late answer is dropped by id.
    Timeout,
    /// No live stream (index down / reconnecting).
    Disconnected,
}

struct PendingQuery {
    query: proto::Query,
    reply: oneshot::Sender<proto::Match>,
}

pub struct RemoteIndex {
    queries: mpsc::Sender<PendingQuery>,
    placements: mpsc::Sender<proto::Update>,
    next_id: AtomicU64,
}

impl RemoteIndex {
    /// Lazy client: drivers connect (and reconnect) in the background.
    #[expect(
        clippy::disallowed_methods,
        reason = "client-lifetime driver tasks; the owner holds the Arc for the process lifetime"
    )]
    pub fn connect(url: String) -> Arc<Self> {
        let (query_tx, query_rx) = mpsc::channel::<PendingQuery>(4096);
        let (placement_tx, placement_rx) = mpsc::channel::<proto::Update>(65_536);
        tokio::spawn(subscribe_driver(url.clone(), query_rx));
        tokio::spawn(bridge::run_publisher(placement_rx, url));
        Arc::new(Self {
            queries: query_tx,
            placements: placement_tx,
            next_id: AtomicU64::new(1),
        })
    }

    /// Overlap query with a hard deadline. Never blocks longer than
    /// `deadline`; every non-Scores outcome is a signal to fall back.
    pub async fn query(
        &self,
        model: &str,
        block_size: u32,
        content_hashes: Vec<u64>,
        deadline: Duration,
    ) -> QueryOutcome {
        let query_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        let pending = PendingQuery {
            query: proto::Query {
                query_id,
                keyspace: Some(bridge::keyspace(model, block_size)),
                content_hashes,
            },
            reply: reply_tx,
        };
        if self.queries.try_send(pending).is_err() {
            return QueryOutcome::Disconnected;
        }
        match tokio::time::timeout(deadline, reply_rx).await {
            Ok(Ok(answer)) => {
                let scores: Vec<(String, u32)> = answer
                    .scores
                    .into_iter()
                    .map(|s| (s.holder, s.matched_blocks))
                    .collect();
                if scores.is_empty() {
                    QueryOutcome::Empty
                } else {
                    QueryOutcome::Scores(scores)
                }
            }
            Ok(Err(_)) => QueryOutcome::Disconnected,
            Err(_) => QueryOutcome::Timeout,
        }
    }

    /// Fire-and-forget placement: the request's block-hash chain now
    /// (probably) resides on `holder`. Never blocks; dropped on overflow
    /// (the next turn re-places it).
    pub fn publish_placement(
        &self,
        model: &str,
        block_size: u32,
        holder: &str,
        content_hashes: &[u64],
    ) {
        let hashes: Vec<ContentHash> = content_hashes.iter().copied().map(ContentHash).collect();
        let blocks = placement_chain(&hashes)
            .into_iter()
            .map(|b| proto::Block {
                seq_hash: b.seq_hash.0,
                content_hash: b.content_hash.0,
            })
            .collect();
        let update = proto::Update {
            keyspace: Some(bridge::keyspace(model, block_size)),
            holder: holder.to_string(),
            // Placements are unsequenced (seq 0) and epoch-constant: an
            // event-fed holder rejects them regardless, and inferred-only
            // holders never bump epochs.
            epoch: 1,
            seq: 0,
            events: vec![proto::Event {
                kind: Some(proto::event::Kind::Stored(proto::Stored {
                    parent_seq_hash: None,
                    blocks,
                })),
            }],
            added: None,
            dropped: false,
        };
        let _ = self.placements.try_send(update);
    }
}

/// Owns the Subscribe bidi stream and the pending-answer map; reconnects
/// forever. On disconnect all pending replies drop (callers resolve
/// Disconnected); late answers for abandoned ids are discarded by the
/// map lookup.
async fn subscribe_driver(url: String, mut queries: mpsc::Receiver<PendingQuery>) {
    loop {
        let Ok(mut client) = RadixIndexClient::connect(url.clone()).await else {
            drain_disconnected(&mut queries);
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let (fwd_tx, fwd_rx) = mpsc::channel::<proto::Query>(1024);
        let outbound = tokio_stream::wrappers::ReceiverStream::new(fwd_rx);
        let mut answers = match client.subscribe(tonic::Request::new(outbound)).await {
            Ok(response) => response.into_inner(),
            Err(_) => {
                drain_disconnected(&mut queries);
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let mut pending: HashMap<u64, oneshot::Sender<proto::Match>> = HashMap::new();
        loop {
            tokio::select! {
                item = queries.recv() => match item {
                    Some(PendingQuery { query, reply }) => {
                        let id = query.query_id;
                        if fwd_tx.send(query).await.is_err() {
                            break; // stream gone; reply drops -> Disconnected
                        }
                        pending.insert(id, reply);
                    }
                    None => return, // client dropped
                },
                answer = answers.next() => match answer {
                    Some(Ok(answer)) => {
                        if let Some(reply) = pending.remove(&answer.query_id) {
                            let _ = reply.send(answer);
                        }
                    }
                    _ => break, // stream error/closed; reconnect
                },
            }
        }
        // Pending replies drop here -> callers resolve Disconnected.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// While there is no live stream, immediately fail queued queries so
/// callers hit their fallback instead of their deadline.
fn drain_disconnected(queries: &mut mpsc::Receiver<PendingQuery>) {
    while let Ok(PendingQuery { reply, .. }) = queries.try_recv() {
        drop(reply);
    }
}
