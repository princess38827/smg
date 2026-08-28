//! Event-bridge core: worker `SubscribeKvEvents` streams -> hash-only
//! index Updates -> one Publish stream to the index. The binary in
//! `bin/bridge.rs` is a thin flag-parsing shell over these; tests drive
//! them in-process.
//!
//! Reconnect semantics mirror the gateway monitor's: resume from the
//! last applied seq; a gap or a backend loss signal (DataLoss /
//! OutOfRange) bumps the holder's EPOCH and restarts from zero — the
//! epoch bump is what makes the restart safe to relay.

use std::time::Duration;

use futures::StreamExt;
use kv_index::compute_content_hash;
use smg_grpc_client::{common_proto, tokenspeed_scheduler::TokenSpeedSchedulerClient};
use tokio::sync::mpsc;

use crate::proto::{self, radix_index_client::RadixIndexClient};

pub fn keyspace(model: &str, block_size: u32) -> proto::Keyspace {
    proto::Keyspace {
        model: model.to_string(),
        symbol_kind: proto::SymbolKind::Tokens as i32,
        block_size,
    }
}

pub fn convert_batch(
    batch: &common_proto::KvEventBatch,
    model: &str,
    block_size: u32,
    holder: &str,
    epoch: u64,
) -> proto::Update {
    let events = batch
        .events
        .iter()
        .filter_map(|event| event.data.as_ref())
        .map(|data| {
            let kind = match data {
                common_proto::kv_cache_event::Data::Stored(stored) => {
                    proto::event::Kind::Stored(proto::Stored {
                        parent_seq_hash: stored.parent_block_hash.map(|p| p as u64),
                        blocks: stored
                            .blocks
                            .iter()
                            .map(|b| proto::Block {
                                seq_hash: b.block_hash as u64,
                                content_hash: compute_content_hash(&b.token_ids).0,
                            })
                            .collect(),
                    })
                }
                common_proto::kv_cache_event::Data::Removed(removed) => {
                    proto::event::Kind::Removed(proto::Removed {
                        seq_hashes: removed.block_hashes.iter().map(|&h| h as u64).collect(),
                    })
                }
                common_proto::kv_cache_event::Data::Cleared(_) => proto::event::Kind::Cleared(true),
            };
            proto::Event { kind: Some(kind) }
        })
        .collect();
    proto::Update {
        keyspace: Some(keyspace(model, block_size)),
        holder: holder.to_string(),
        epoch,
        seq: batch.sequence_number,
        events,
        added: None,
        dropped: false,
    }
}

/// One worker's subscription loop: resume on plain failures, epoch-bump
/// on loss signals or sequence gaps. Runs until the publish channel
/// closes or the worker reports Unimplemented.
pub async fn worker_loop(
    worker: String,
    model: String,
    block_size: u32,
    out: mpsc::Sender<proto::Update>,
) {
    let mut epoch: u64 = 1;
    let mut last_seq: u64 = 0;
    loop {
        let Ok(client) = TokenSpeedSchedulerClient::connect(&worker).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let mut stream = match client.subscribe_kv_events(last_seq).await {
            Ok(stream) => stream,
            Err(status) => {
                match status.code() {
                    // Terminal per the monitor contract.
                    tonic::Code::Unimplemented => {
                        tracing::warn!(%worker, "KV events unimplemented; bridge exits for this worker");
                        return;
                    }
                    // Cursor lost: new generation, replay from zero.
                    tonic::Code::OutOfRange | tonic::Code::DataLoss => {
                        epoch += 1;
                        last_seq = 0;
                    }
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        while let Some(batch) = stream.next().await {
            let Ok(batch) = batch else { break };
            if last_seq > 0 && batch.sequence_number <= last_seq {
                continue; // duplicate replay
            }
            if last_seq > 0 && batch.sequence_number > last_seq + 1 {
                // Gap: the ring may have wrapped; new generation.
                epoch += 1;
                last_seq = 0;
                break;
            }
            last_seq = batch.sequence_number;
            let update = convert_batch(&batch, &model, block_size, &worker, epoch);
            if out.send(update).await.is_err() {
                return; // publisher gone; process exiting
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The publish pump: drain `rx` into one (re)connected Publish stream to
/// `index`. The receiver persists across reconnects, so no update is
/// lost inside the bridge. Returns when all worker loops have ended.
pub async fn run_publisher(mut rx: mpsc::Receiver<proto::Update>, index: String) {
    loop {
        let Ok(mut client) = RadixIndexClient::connect(index.clone()).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let (fwd_tx, fwd_rx) = mpsc::channel::<proto::Update>(1024);
        let outbound = tokio_stream::wrappers::ReceiverStream::new(fwd_rx);
        let mut acks = match client.publish(tonic::Request::new(outbound)).await {
            Ok(response) => response.into_inner(),
            Err(error) => {
                tracing::warn!(%error, "publish stream failed; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        loop {
            tokio::select! {
                item = rx.recv() => match item {
                    Some(update) => {
                        if fwd_tx.send(update).await.is_err() {
                            break;
                        }
                    }
                    // All worker loops ended (fleet torn down).
                    None => return,
                },
                ack = acks.next() => {
                    if ack.is_none() {
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
