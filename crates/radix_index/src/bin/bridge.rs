//! Event bridge: subscribes to workers' `SubscribeKvEvents` streams
//! (TokenSpeed dialect — the sim fleet's), converts batches to index
//! Updates with publisher-computed content hashes (hash-only wire), and
//! publishes them to one index endpoint over a single Publish stream.
//!
//! Reconnect semantics mirror the gateway monitor's: resume from the
//! last applied seq; a gap or a backend loss signal (DataLoss /
//! OutOfRange) bumps the holder's EPOCH and restarts from zero — the
//! epoch bump is what makes the restart safe to relay.
//!
//! Usage:
//!   radix-index-bridge --workers grpc://127.0.0.1:9000,... \
//!     --index http://127.0.0.1:40000 --model mock-model --block-size 256

use std::time::Duration;

use futures::StreamExt;
use kv_index::compute_content_hash;
use radix_index::proto::{self, radix_index_client::RadixIndexClient};
use smg_grpc_client::{common_proto, tokenspeed_scheduler::TokenSpeedSchedulerClient};
use tokio::sync::mpsc;

fn parse_flag<T: std::str::FromStr>(args: &[String], flag: &str) -> Option<T> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

fn keyspace(model: &str, block_size: u32) -> proto::Keyspace {
    proto::Keyspace {
        model: model.to_string(),
        symbol_kind: proto::SymbolKind::Tokens as i32,
        block_size,
    }
}

fn convert_batch(
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
/// on loss signals or sequence gaps.
async fn worker_loop(
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

#[expect(
    clippy::disallowed_methods,
    reason = "worker subscription tasks live for the process lifetime"
)]
#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    let workers: Vec<String> = parse_flag::<String>(&args, "--workers")
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let index: String =
        parse_flag(&args, "--index").unwrap_or_else(|| "http://127.0.0.1:40000".to_string());
    let model: String = parse_flag(&args, "--model").unwrap_or_else(|| "mock-model".to_string());
    let block_size: u32 = parse_flag(&args, "--block-size").unwrap_or(256);
    if workers.is_empty() {
        eprintln!("--workers is required");
        return std::process::ExitCode::from(2);
    }

    let (tx, rx) = mpsc::channel::<proto::Update>(65_536);
    for worker in &workers {
        tokio::spawn(worker_loop(
            worker.clone(),
            model.clone(),
            block_size,
            tx.clone(),
        ));
    }
    drop(tx);
    tracing::info!(workers = workers.len(), %index, "bridge running");

    // One Publish stream to the index; reconnect forever, the receiver
    // half persists across reconnects so no update is lost in the bridge.
    let mut rx = rx;
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
                    None => return std::process::ExitCode::SUCCESS,
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
