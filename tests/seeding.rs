//! Two copies of the client: one seeds, the other downloads from it. This
//! exercises the upload path (listener, choking, block reads).

mod common;

use std::time::Duration;

use common::{fake_tracker, fast_timeouts, make_data, run_client, run_client_within, PIECE};
use oxtorrent::metainfo::{build_single_file, Torrent};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_seeds_to_another_client() {
    let data = make_data(2 * 1024 * 1024 + 5, 20);
    let tracker = fake_tracker::spawn(Vec::new()).await;
    let torrent = Torrent::from_bytes(&build_single_file(
        "s.bin",
        &data,
        PIECE,
        Some(&tracker.url),
    ))
    .unwrap();

    let seed_dir = tempfile::tempdir().unwrap();
    std::fs::write(seed_dir.path().join("s.bin"), &data).unwrap();
    let token = CancellationToken::new();
    let seeder = tokio::spawn({
        let (t, token, path) = (
            torrent.clone(),
            token.clone(),
            seed_dir.path().to_path_buf(),
        );
        async move { run_client(t, &path, true, fast_timeouts(), token).await }
    });

    // Wait until the seeder has announced itself.
    let start = tokio::time::Instant::now();
    while tracker.announced.lock().unwrap().is_empty() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "seeder never announced"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let dl_dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dl_dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dl_dir.path().join("s.bin")).unwrap(), data);

    token.cancel();
    let seeded = tokio::time::timeout(Duration::from_secs(3), seeder)
        .await
        .unwrap()
        .unwrap();
    assert!(seeded.complete);
    assert_eq!(seeded.stats.uploaded, data.len() as u64);
}
