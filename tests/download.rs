mod common;

use std::sync::Arc;
use std::time::Duration;

use common::fake_peer::{self, Behaviour};
use common::{fake_tracker, fast_timeouts, make_data, run_client_within, PIECE};
use oxtorrent::metainfo::{build_multi_file, build_single_file, Torrent};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn honest_seeder_download_matches_byte_for_byte() {
    let data = Arc::new(make_data(1024 * 1024, 1));
    let probe = Torrent::from_bytes(&build_single_file("file.bin", &data, PIECE, None)).unwrap();
    let peer = fake_peer::spawn(data.clone(), PIECE, probe.info_hash, Behaviour::Honest).await;
    let tracker = fake_tracker::spawn(vec![peer.addr]).await;
    let torrent = Torrent::from_bytes(&build_single_file(
        "file.bin",
        &data,
        PIECE,
        Some(&tracker.url),
    ))
    .unwrap();
    assert_eq!(torrent.info_hash, probe.info_hash);

    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent.clone(),
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dir.path().join("file.bin")).unwrap(), *data);
    assert_eq!(outcome.stats.failed_pieces, 0);
    // The tracker heard "started" first (and "stopped" on shutdown).
    let events = tracker.announces.lock().unwrap().clone();
    assert_eq!(events.first().map(String::as_str), Some("started"));

    // Running again resumes: everything verifies from disk, nothing is fetched.
    let before = peer
        .stats
        .blocks_sent
        .load(std::sync::atomic::Ordering::SeqCst);
    let again = run_client_within(
        Duration::from_secs(30),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(again.complete);
    assert_eq!(again.stats.resumed_pieces, 16);
    assert_eq!(
        peer.stats
            .blocks_sent
            .load(std::sync::atomic::Ordering::SeqCst),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_with_peers_from_a_udp_tracker() {
    let data = Arc::new(make_data(300 * 1024, 6));
    let probe = Torrent::from_bytes(&build_single_file("u.bin", &data, PIECE, None)).unwrap();
    let peer = fake_peer::spawn(data.clone(), PIECE, probe.info_hash, Behaviour::Honest).await;
    let tracker = fake_tracker::spawn_udp(vec![peer.addr]).await;
    let torrent = Torrent::from_bytes(&build_single_file(
        "u.bin",
        &data,
        PIECE,
        Some(&tracker.url),
    ))
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dir.path().join("u.bin")).unwrap(), *data);
    let events = tracker.announces.lock().unwrap().clone();
    assert_eq!(events.first().map(String::as_str), Some("started"));
    assert!(events.iter().any(|e| e == "stopped"), "events: {events:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_file_resumes_and_fetches_only_missing_pieces() {
    let data = Arc::new(make_data(512 * 1024 + 123, 2));
    let probe = Torrent::from_bytes(&build_single_file("p.bin", &data, PIECE, None)).unwrap();
    let peer = fake_peer::spawn(data.clone(), PIECE, probe.info_hash, Behaviour::Honest).await;
    let tracker = fake_tracker::spawn(vec![peer.addr]).await;
    let torrent = Torrent::from_bytes(&build_single_file(
        "p.bin",
        &data,
        PIECE,
        Some(&tracker.url),
    ))
    .unwrap();

    // First half of the file is already on disk; the rest is garbage.
    let dir = tempfile::tempdir().unwrap();
    let mut partial = data[..256 * 1024].to_vec();
    partial.resize(data.len(), 0xAA);
    std::fs::write(dir.path().join("p.bin"), &partial).unwrap();

    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(outcome.stats.resumed_pieces, 4);
    assert_eq!(std::fs::read(dir.path().join("p.bin")).unwrap(), *data);
    // 9 pieces total, 5 fetched: 4 x 4 blocks + the short last piece (1 block).
    assert_eq!(
        peer.stats
            .blocks_sent
            .load(std::sync::atomic::Ordering::SeqCst),
        17
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_file_torrent_downloads() {
    let a = make_data(100_000, 3);
    let b = make_data(250_001, 4);
    let c = make_data(7, 5);
    let files = vec![
        (vec!["a.bin"], a.clone()),
        (vec!["sub", "b.bin"], b.clone()),
        (vec!["sub", "c.txt"], c.clone()),
    ];
    let stream: Arc<Vec<u8>> = Arc::new(a.iter().chain(&b).chain(&c).copied().collect());
    let probe = Torrent::from_bytes(&build_multi_file("set", &files, PIECE, None)).unwrap();
    let peer = fake_peer::spawn(stream, PIECE, probe.info_hash, Behaviour::Honest).await;
    let tracker = fake_tracker::spawn(vec![peer.addr]).await;
    let torrent =
        Torrent::from_bytes(&build_multi_file("set", &files, PIECE, Some(&tracker.url))).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    let root = dir.path().join("set");
    assert_eq!(std::fs::read(root.join("a.bin")).unwrap(), a);
    assert_eq!(std::fs::read(root.join("sub").join("b.bin")).unwrap(), b);
    assert_eq!(std::fs::read(root.join("sub").join("c.txt")).unwrap(), c);
}
