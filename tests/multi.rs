//! Several torrents in one run, sharing one listening port.

mod common;

use std::time::Duration;

use common::{fast_timeouts, make_data, PIECE};
use oxtorrent::metainfo::{build_single_file, Torrent};
use oxtorrent::peer::generate_peer_id;
use oxtorrent::router::Router;
use oxtorrent::session::{self, Config, Inbound, Progress};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

fn config(dir: &std::path::Path, port: u16, seed: bool) -> Config {
    let mut cfg = Config::new(dir.to_path_buf(), generate_peer_id());
    cfg.port = port;
    cfg.seed = seed;
    cfg.timeouts = fast_timeouts();
    cfg
}

/// A seeder serving two torrents on one port, and a leecher fetching both
/// at once. Every connection the leecher opens reaches the seeder's single
/// port, so both downloads complete only if the router sends each one to
/// the session for its info hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_torrents_share_one_port() {
    let files = [
        ("a.bin", make_data(700 * 1024, 21)),
        ("b.bin", make_data(450 * 1024, 22)),
    ];
    let torrents: Vec<Torrent> = files
        .iter()
        .map(|(name, data)| {
            Torrent::from_bytes(&build_single_file(name, data, PIECE, None)).unwrap()
        })
        .collect();
    assert_ne!(torrents[0].info_hash, torrents[1].info_hash);

    let seed_dir = tempfile::tempdir().unwrap();
    for (name, data) in &files {
        std::fs::write(seed_dir.path().join(name), data).unwrap();
    }
    let seed_token = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_port = listener.local_addr().unwrap().port();
    let router = Router::spawn(listener, seed_token.clone());
    for t in &torrents {
        let inbound = Inbound::Routed(router.register(t.info_hash));
        let (tx, _rx) = watch::channel(Progress::default());
        tokio::spawn(session::run_download(
            t.clone(),
            config(seed_dir.path(), seed_port, true),
            inbound,
            tx,
            seed_token.clone(),
        ));
    }

    let dir = tempfile::tempdir().unwrap();
    let token = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = Router::spawn(listener, token.clone());
    let mut runs = Vec::new();
    for t in &torrents {
        let mut cfg = config(dir.path(), port, false);
        cfg.extra_peers = vec![format!("127.0.0.1:{seed_port}").parse().unwrap()];
        let inbound = Inbound::Routed(router.register(t.info_hash));
        let (tx, _rx) = watch::channel(Progress::default());
        runs.push(tokio::spawn(session::run_download(
            t.clone(),
            cfg,
            inbound,
            tx,
            token.clone(),
        )));
    }
    for run in runs {
        let outcome = tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("downloads did not finish in time")
            .unwrap()
            .unwrap();
        assert!(outcome.complete);
        assert_eq!(outcome.stats.failed_pieces, 0);
    }
    for (name, data) in &files {
        assert_eq!(
            std::fs::read(dir.path().join(name)).unwrap(),
            *data,
            "{name}"
        );
    }
    token.cancel();
    seed_token.cancel();
}
