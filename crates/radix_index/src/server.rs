//! The gRPC surface: Publish (apply + best-effort peer relay),
//! Subscribe (query stream), Pull (state as synthetic Updates for
//! replica bootstrap). Relay and bootstrap speak the same Update
//! vocabulary as publishers, so replicas copy — they never agree.

use std::{pin::Pin, sync::Arc, time::Duration};

use futures::{Stream, StreamExt};
use kv_index::ContentHash;
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use crate::{
    engine::{Engine, KeyspaceKey, SymbolKind},
    proto::{
        self,
        radix_index_client::RadixIndexClient,
        radix_index_server::{RadixIndex, RadixIndexServer},
    },
    UpdateMsg,
};

type AckStream = Pin<Box<dyn Stream<Item = Result<proto::PublishAck, Status>> + Send>>;
type MatchStream = Pin<Box<dyn Stream<Item = Result<proto::Match, Status>> + Send>>;
type PullStream = Pin<Box<dyn Stream<Item = Result<proto::Update, Status>> + Send>>;

/// Bound on the per-peer relay queue; overflowing drops the oldest
/// (divergence is bounded by TTL + re-placement + digest heal, and a
/// wedged peer must not wedge ingest).
const RELAY_QUEUE: usize = 65_536;

pub struct IndexService {
    engine: Arc<Engine>,
    relay: Vec<mpsc::Sender<proto::Update>>,
}

impl IndexService {
    /// `peers`: sibling replica endpoints to relay Publishes to (empty
    /// for single-replica runs or when publishers fan out themselves).
    /// Relay is async and best-effort: a wedged peer drops updates, and
    /// epoch/seq dedup plus TTL/re-placement bound the divergence.
    pub fn new(engine: Arc<Engine>, peers: Vec<String>) -> Self {
        let relay = peers.into_iter().map(spawn_relay).collect();
        Self { engine, relay }
    }
}

/// One background relay: queue -> (re)connected Publish stream to `peer`.
#[expect(
    clippy::disallowed_methods,
    reason = "service-lifetime task; the index process is its own supervisor"
)]
fn spawn_relay(peer: String) -> mpsc::Sender<proto::Update> {
    let (tx, mut rx) = mpsc::channel::<proto::Update>(RELAY_QUEUE);
    tokio::spawn(async move {
        loop {
            match RadixIndexClient::connect(peer.clone()).await {
                Ok(mut client) => {
                    let (fwd_tx, fwd_rx) = mpsc::channel::<proto::Update>(1024);
                    let outbound = tokio_stream::wrappers::ReceiverStream::new(fwd_rx);
                    let mut acks = match client.publish(Request::new(outbound)).await {
                        Ok(response) => response.into_inner(),
                        Err(error) => {
                            tracing::warn!(%peer, %error, "relay publish failed; retrying");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    };
                    loop {
                        tokio::select! {
                            item = rx.recv() => match item {
                                Some(update) => {
                                    if fwd_tx.send(update).await.is_err() {
                                        break; // stream torn down; reconnect
                                    }
                                }
                                None => return, // service dropped
                            },
                            ack = acks.next() => {
                                if ack.is_none() {
                                    break; // peer closed; reconnect
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%peer, %error, "relay connect failed; retrying");
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    tx
}

#[tonic::async_trait]
impl RadixIndex for IndexService {
    type PublishStream = AckStream;
    type SubscribeStream = MatchStream;
    type PullStream = PullStream;

    #[expect(
        clippy::disallowed_methods,
        reason = "per-stream task, bounded by the stream's lifetime"
    )]
    async fn publish(
        &self,
        request: Request<Streaming<proto::Update>>,
    ) -> Result<Response<Self::PublishStream>, Status> {
        let mut inbound = request.into_inner();
        let engine = Arc::clone(&self.engine);
        let relay = self.relay.clone();
        let (tx, rx) = mpsc::channel::<Result<proto::PublishAck, Status>>(1024);
        tokio::spawn(async move {
            while let Some(update) = inbound.next().await {
                let Ok(update) = update else { break };
                let msg = UpdateMsg::from(&update);
                let (_outcome, applied_seq) = engine.apply(&msg);
                for peer in &relay {
                    let _ = peer.try_send(update.clone());
                }
                let ack = proto::PublishAck {
                    holder: msg.holder,
                    epoch: msg.epoch,
                    applied_seq,
                };
                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "per-stream task, bounded by the stream's lifetime"
    )]
    async fn subscribe(
        &self,
        request: Request<Streaming<proto::Query>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut inbound = request.into_inner();
        let engine = Arc::clone(&self.engine);
        let (tx, rx) = mpsc::channel::<Result<proto::Match, Status>>(1024);
        tokio::spawn(async move {
            while let Some(query) = inbound.next().await {
                let Ok(query) = query else { break };
                let keyspace = query.keyspace.as_ref();
                let key = KeyspaceKey {
                    model: keyspace.map(|k| k.model.clone()).unwrap_or_default(),
                    symbol_kind: match keyspace.map(|k| k.symbol_kind) {
                        Some(k) if k == proto::SymbolKind::Bytes as i32 => SymbolKind::Bytes,
                        _ => SymbolKind::Tokens,
                    },
                    block_size: keyspace.map(|k| k.block_size).unwrap_or_default(),
                };
                let hashes: Vec<ContentHash> = query
                    .content_hashes
                    .iter()
                    .copied()
                    .map(ContentHash)
                    .collect();
                let scores = engine.find_matches(&key, &hashes);
                let answer = proto::Match {
                    query_id: query.query_id,
                    scores: scores
                        .into_iter()
                        .map(|s| proto::HolderScore {
                            holder: s.holder,
                            matched_blocks: s.matched_blocks,
                            total_blocks: s.total_blocks,
                            event_fed: s.event_fed,
                        })
                        .collect(),
                };
                if tx.send(Ok(answer)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn pull(
        &self,
        _request: Request<proto::PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let snapshot = self.engine.snapshot();
        let updates = snapshot
            .iter()
            .map(|u| Ok(proto::Update::from(u)))
            .collect::<Vec<_>>();
        Ok(Response::new(Box::pin(futures::stream::iter(updates))))
    }
}

/// Bootstrap: pull the full state from `peer` and apply it before
/// serving. Returns Ok(applied_count); a connect failure is Ok(0) so a
/// lone first replica can boot cold.
pub async fn bootstrap_from(engine: &Engine, peer: &str) -> Result<usize, tonic::Status> {
    let Ok(mut client) = RadixIndexClient::connect(peer.to_string()).await else {
        return Ok(0);
    };
    let mut stream = client
        .pull(Request::new(proto::PullRequest {}))
        .await?
        .into_inner();
    let mut applied = 0usize;
    while let Some(update) = stream.next().await {
        let update = update?;
        engine.apply(&UpdateMsg::from(&update));
        applied += 1;
    }
    Ok(applied)
}

/// Serve the index on `addr` until the process exits.
#[expect(
    clippy::disallowed_methods,
    reason = "service-lifetime sweeper; the index process is its own supervisor"
)]
pub async fn serve(
    engine: Arc<Engine>,
    addr: std::net::SocketAddr,
    peers: Vec<String>,
    sweep_interval: Duration,
) -> Result<(), tonic::transport::Error> {
    let sweeper = Arc::clone(&engine);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(sweep_interval);
        loop {
            tick.tick().await;
            sweeper.sweep_idle();
        }
    });
    Server::builder()
        .add_service(RadixIndexServer::new(IndexService::new(engine, peers)))
        .serve(addr)
        .await
}
