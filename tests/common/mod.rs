#![allow(dead_code)]

pub mod fake_peer;
pub mod fake_tracker;

use std::path::Path;
use std::time::Duration;

use oxtorrent::metainfo::Torrent;
use oxtorrent::peer::connection::Timeouts;
use oxtorrent::peer::generate_peer_id;
use oxtorrent::session::{self, Config, Outcome, Progress};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub const PIECE: u64 = 64 * 1024;

/// Deterministic pseudo-random test data.
pub fn make_data(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..len)
        .map(|_| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (x >> 33) as u8
        })
        .collect()
}

pub fn fast_timeouts() -> Timeouts {
    Timeouts {
        connect: Duration::from_secs(2),
        handshake: Duration::from_secs(2),
        idle: Duration::from_secs(20),
        keepalive: Duration::from_secs(10),
        block: Duration::from_secs(5),
        choked: Duration::from_secs(60),
    }
}

/// Runs a client on localhost until it finishes or `token` is cancelled.
pub async fn run_client(
    torrent: Torrent,
    out: &Path,
    seed: bool,
    timeouts: Timeouts,
    token: CancellationToken,
) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = Config::new(out.to_path_buf(), generate_peer_id());
    cfg.port = listener.local_addr().unwrap().port();
    cfg.seed = seed;
    cfg.timeouts = timeouts;
    let (tx, _rx) = watch::channel(Progress::default());
    session::run_download(torrent, cfg, Some(listener), tx, token)
        .await
        .unwrap()
}

/// Same, failing the test if it takes longer than `limit`.
pub async fn run_client_within(
    limit: Duration,
    torrent: Torrent,
    out: &Path,
    timeouts: Timeouts,
) -> Outcome {
    tokio::time::timeout(
        limit,
        run_client(torrent, out, false, timeouts, CancellationToken::new()),
    )
    .await
    .expect("download did not finish in time")
}
