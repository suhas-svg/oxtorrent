mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common::fake_peer::{self, Behaviour, FakePeer};
use common::{fake_tracker, fast_timeouts, make_data, run_client, run_client_within, PIECE};
use oxtorrent::metainfo::{build_single_file, Torrent};
use tokio_util::sync::CancellationToken;

/// Starts fake peers with the given behaviours and returns them with a
/// torrent whose tracker lists all of them.
async fn swarm(data: &Arc<Vec<u8>>, behaviours: &[Behaviour]) -> (Vec<FakePeer>, Torrent) {
    let probe = Torrent::from_bytes(&build_single_file("f.bin", data, PIECE, None)).unwrap();
    let mut peers = Vec::new();
    for b in behaviours {
        peers.push(fake_peer::spawn(data.clone(), PIECE, probe.info_hash, *b).await);
    }
    let tracker = fake_tracker::spawn(peers.iter().map(|p| p.addr).collect()).await;
    let torrent =
        Torrent::from_bytes(&build_single_file("f.bin", data, PIECE, Some(&tracker.url))).unwrap();
    (peers, torrent)
}

async fn wait_for(limit: Duration, cond: impl Fn() -> bool) -> bool {
    let start = tokio::time::Instant::now();
    while start.elapsed() < limit {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_corrupt_block_is_redownloaded() {
    let data = Arc::new(make_data(1024 * 1024, 10));
    let (_peers, torrent) = swarm(&data, &[Behaviour::CorruptOnce]).await;
    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(outcome.stats.failed_pieces, 1);
    assert!(outcome.stats.banned.is_empty());
    assert_eq!(std::fs::read(dir.path().join("f.bin")).unwrap(), *data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn always_corrupt_peer_is_banned() {
    let data = Arc::new(make_data(1024 * 1024, 11));
    let (peers, torrent) = swarm(&data, &[Behaviour::AlwaysCorrupt, Behaviour::Honest]).await;
    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(
        outcome.stats.banned,
        vec![peers[0].addr],
        "only the corrupt peer is banned"
    );
    assert!(outcome.stats.failed_pieces >= 3);
    assert_eq!(std::fs::read(dir.path().join("f.bin")).unwrap(), *data);
    // Banned peers are not dialled again.
    assert_eq!(peers[0].stats.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocks_of_a_disconnecting_peer_are_reassigned() {
    let data = Arc::new(make_data(1024 * 1024, 12));
    let (peers, torrent) = swarm(&data, &[Behaviour::DisconnectAfter(6), Behaviour::Honest]).await;
    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert!(peers[0].stats.closed.load(Ordering::SeqCst) >= 1);
    assert_eq!(std::fs::read(dir.path().join("f.bin")).unwrap(), *data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_that_never_unchokes_times_out_and_client_does_not_hang() {
    let data = Arc::new(make_data(256 * 1024, 13));
    let (peers, torrent) = swarm(&data, &[Behaviour::NeverUnchoke]).await;
    let dir = tempfile::tempdir().unwrap();
    let mut timeouts = fast_timeouts();
    timeouts.choked = Duration::from_secs(1);
    let token = CancellationToken::new();
    let client = tokio::spawn({
        let (token, path) = (token.clone(), dir.path().to_path_buf());
        async move { run_client(torrent, &path, false, timeouts, token).await }
    });
    let stats = peers[0].stats.clone();
    assert!(
        wait_for(Duration::from_secs(10), || stats
            .closed
            .load(Ordering::SeqCst)
            >= 1)
        .await,
        "client never dropped the choking peer"
    );
    token.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(3), client)
        .await
        .expect("client did not shut down within 3 s")
        .unwrap();
    assert!(!outcome.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_length_prefix_closes_connection() {
    let data = Arc::new(make_data(256 * 1024, 14));
    let (peers, torrent) = swarm(&data, &[Behaviour::OversizedLength]).await;
    let dir = tempfile::tempdir().unwrap();
    let token = CancellationToken::new();
    let client = tokio::spawn({
        let (token, path) = (token.clone(), dir.path().to_path_buf());
        async move { run_client(torrent, &path, false, fast_timeouts(), token).await }
    });
    let stats = peers[0].stats.clone();
    assert!(
        wait_for(Duration::from_secs(10), || stats
            .closed
            .load(Ordering::SeqCst)
            >= 1)
        .await,
        "client kept a peer that claimed a 2 GB message"
    );
    token.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(3), client)
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.complete);
    assert_eq!(outcome.stats.downloaded, 0);
}
