//! Download and upload limits on real localhost transfers.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::fake_peer::{self, Behaviour};
use common::{fast_timeouts, make_data, PIECE};
use oxtorrent::metainfo::{build_single_file, Torrent};
use oxtorrent::peer::generate_peer_id;
use oxtorrent::ratelimit::Limits;
use oxtorrent::session::{self, Config, Progress};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const SIZE: usize = 1024 * 1024;
const RATE: u64 = 512 * 1024;

fn config(dir: &std::path::Path, port: u16) -> Config {
    let mut cfg = Config::new(dir.to_path_buf(), generate_peer_id());
    cfg.port = port;
    cfg.timeouts = fast_timeouts();
    cfg
}

/// 1 MiB at 512 KiB/s takes about 2 s (less the 128 KiB burst), where the
/// same download unlimited takes a fraction of a second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_limit_holds_the_rate() {
    let data = Arc::new(make_data(SIZE, 31));
    let torrent = Torrent::from_bytes(&build_single_file("d.bin", &data, PIECE, None)).unwrap();
    let peers = [
        fake_peer::spawn(data.clone(), PIECE, torrent.info_hash, Behaviour::Honest).await,
        fake_peer::spawn(data.clone(), PIECE, torrent.info_hash, Behaviour::Honest).await,
    ];

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), 0);
    cfg.extra_peers = peers.iter().map(|p| p.addr).collect();
    cfg.limits = Limits::new(RATE, 0);
    let (tx, _rx) = watch::channel(Progress::default());
    let start = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        session::run_download(torrent, cfg, None, tx, CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    let secs = start.elapsed().as_secs_f64();
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dir.path().join("d.bin")).unwrap(), *data);
    assert!((1.5..6.0).contains(&secs), "took {secs:.2} s");
}

/// A seeder limited to 512 KiB/s upload serves 1 MiB in about 2 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_limit_holds_the_rate() {
    let data = make_data(SIZE, 32);
    let torrent = Torrent::from_bytes(&build_single_file("u.bin", &data, PIECE, None)).unwrap();
    let seed_dir = tempfile::tempdir().unwrap();
    std::fs::write(seed_dir.path().join("u.bin"), &data).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = listener.local_addr().unwrap();
    let mut seed_cfg = config(seed_dir.path(), seed_addr.port());
    seed_cfg.seed = true;
    seed_cfg.limits = Limits::new(0, RATE);
    let seed_token = CancellationToken::new();
    let (tx, _rx) = watch::channel(Progress::default());
    tokio::spawn(session::run_download(
        torrent.clone(),
        seed_cfg,
        Some(listener),
        tx,
        seed_token.clone(),
    ));

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), 0);
    cfg.extra_peers = vec![seed_addr];
    let (tx, _rx) = watch::channel(Progress::default());
    let start = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        session::run_download(torrent, cfg, None, tx, CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    let secs = start.elapsed().as_secs_f64();
    seed_token.cancel();
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dir.path().join("u.bin")).unwrap(), data);
    assert!((1.5..6.0).contains(&secs), "took {secs:.2} s");
}
