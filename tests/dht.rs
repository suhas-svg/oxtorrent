//! A trackerless download: the seeder announces itself on a small DHT on
//! localhost, and a magnet link with no trackers finds it there.

mod common;

use std::time::Duration;

use common::{fast_timeouts, make_data, PIECE};
use oxtorrent::dht::{Dht, DhtConfig};
use oxtorrent::magnet::{fetch_metadata, FetchConfig, Magnet};
use oxtorrent::metainfo::{build_single_file, hex, Torrent};
use oxtorrent::peer::generate_peer_id;
use oxtorrent::session::{self, Config, Progress};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

fn dht_config(bootstrap: Vec<String>) -> DhtConfig {
    DhtConfig {
        port: 0,
        bootstrap,
        query_timeout: Duration::from_millis(500),
        maintenance: Duration::from_millis(300),
    }
}

async fn start_node(seed: &str, token: &CancellationToken) -> Dht {
    let d = Dht::start(dht_config(vec![seed.to_string()]), token.clone())
        .await
        .unwrap();
    d.bootstrap().await;
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trackerless_magnet_download_via_dht() {
    let token = CancellationToken::new();
    let root = Dht::start(dht_config(Vec::new()), token.clone())
        .await
        .unwrap();
    let seed = format!("127.0.0.1:{}", root.local_addr().unwrap().port());
    for _ in 0..5 {
        start_node(&seed, &token).await;
    }

    // A seeder with no tracker at all.
    let data = make_data(400 * 1024 + 9, 40);
    let torrent = Torrent::from_bytes(&build_single_file("d.bin", &data, PIECE, None)).unwrap();
    assert!(torrent.trackers().is_empty());
    let seed_dir = tempfile::tempdir().unwrap();
    std::fs::write(seed_dir.path().join("d.bin"), &data).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = Config::new(seed_dir.path().to_path_buf(), generate_peer_id());
    cfg.port = listener.local_addr().unwrap().port();
    cfg.seed = true;
    cfg.timeouts = fast_timeouts();
    cfg.dht = Some(start_node(&seed, &token).await);
    let (tx, _rx) = watch::channel(Progress::default());
    let (t, tok) = (torrent.clone(), token.clone());
    tokio::spawn(async move { session::run_download(t, cfg, Some(listener), tx, tok).await });

    // Wait until the seeder's announce has reached the DHT.
    let probe = start_node(&seed, &token).await;
    let start = tokio::time::Instant::now();
    while probe.get_peers(torrent.info_hash, None).await.is_empty() {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "seeder never appeared on the DHT"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The downloader has only the info hash.
    let magnet =
        Magnet::parse(&format!("magnet:?xt=urn:btih:{}", hex(&torrent.info_hash))).unwrap();
    assert!(magnet.trackers.is_empty() && magnet.peers.is_empty());
    let dht = start_node(&seed, &token).await;
    let mut fetch = FetchConfig::new(generate_peer_id(), 6881);
    fetch.timeouts = fast_timeouts();
    fetch.dht = Some(dht.clone());
    let fetched = tokio::time::timeout(
        Duration::from_secs(30),
        fetch_metadata(&magnet, &fetch, &[], &CancellationToken::new()),
    )
    .await
    .expect("metadata fetch timed out")
    .unwrap();
    assert_eq!(fetched.info, torrent.info_bytes);

    let got = Torrent::from_info(&fetched.info, &[]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = Config::new(dir.path().to_path_buf(), generate_peer_id());
    cfg.port = listener.local_addr().unwrap().port();
    cfg.timeouts = fast_timeouts();
    cfg.extra_peers = fetched.peers;
    cfg.dht = Some(dht);
    let (tx, _rx) = watch::channel(Progress::default());
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        session::run_download(got, cfg, Some(listener), tx, CancellationToken::new()),
    )
    .await
    .expect("download did not finish in time")
    .unwrap();
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dir.path().join("d.bin")).unwrap(), data);
    token.cancel();
}
