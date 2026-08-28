//! The radix index service binary.
//!
//! Usage:
//!   radix-index-service --port 40000 [--peers http://127.0.0.1:40001,..]
//!     [--bootstrap-from http://127.0.0.1:40001]
//!     [--inferred-ttl-secs 18] [--default-capacity-blocks 4688]
//!     [--sweep-interval-secs 5] [--apply-delay-ms 0]

use std::{sync::Arc, time::Duration};

use radix_index::{server, Engine, EngineConfig};

fn parse_flag<T: std::str::FromStr>(args: &[String], flag: &str) -> Option<T> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = parse_flag(&args, "--port").unwrap_or(40000);
    let peers: Vec<String> = parse_flag::<String>(&args, "--peers")
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let bootstrap: Option<String> = parse_flag(&args, "--bootstrap-from");
    let cfg = EngineConfig {
        jump_size: 64,
        inferred_ttl: Duration::from_secs(parse_flag(&args, "--inferred-ttl-secs").unwrap_or(180)),
        default_capacity_blocks: parse_flag(&args, "--default-capacity-blocks").unwrap_or(u64::MAX),
    };
    let sweep = Duration::from_secs(parse_flag(&args, "--sweep-interval-secs").unwrap_or(5));

    let engine = Arc::new(Engine::new(cfg));
    if let Some(peer) = bootstrap {
        match server::bootstrap_from(&engine, &peer).await {
            Ok(applied) => tracing::info!(peer, applied, "bootstrap pull complete"),
            Err(error) => tracing::warn!(peer, %error, "bootstrap pull failed; starting cold"),
        }
    }

    let addr = format!("127.0.0.1:{port}").parse().expect("bind addr");
    tracing::info!(%addr, peers = peers.len(), "radix index serving");
    if let Err(error) = server::serve(engine, addr, peers, sweep).await {
        tracing::error!(%error, "server exited");
    }
}
