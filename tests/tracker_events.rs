//! Which events the tracker task sends, driven by progress updates.

mod common;

use std::time::Duration;

use common::fake_tracker;
use oxtorrent::session::Progress;
use oxtorrent::tracker::{self, TrackerConfig};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const TOTAL: u64 = 1024 * 1024;

/// Runs the tracker task from `start`, then publishes `later` once the
/// first announce has arrived, and returns the event names announced.
async fn events_for(start: Progress, later: Progress) -> Vec<String> {
    let tracker = fake_tracker::spawn(Vec::new()).await;
    let (tx, rx) = watch::channel(start);
    let (events_tx, mut events_rx) = mpsc::channel(16);
    tokio::spawn(async move { while events_rx.recv().await.is_some() {} });
    let token = CancellationToken::new();
    let cfg = TrackerConfig {
        urls: vec![tracker.url.clone()],
        info_hash: [7; 20],
        peer_id: [8; 20],
        port: 6881,
        udp_retransmit: Default::default(),
    };
    let task = tokio::spawn(tracker::run_tracker(cfg, rx, events_tx, token.clone()));

    let wait_for = |n: usize| {
        let log = tracker.announces.clone();
        async move {
            for _ in 0..100 {
                if log.lock().unwrap().len() >= n {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("tracker heard fewer than {n} announces");
        }
    };
    wait_for(1).await;
    tx.send_replace(later);
    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let events = tracker.announces.lock().unwrap().clone();
    events
}

fn progress(verified: u64, complete: bool) -> Progress {
    Progress {
        total: TOTAL,
        verified_bytes: verified,
        complete,
        ..Progress::default()
    }
}

#[tokio::test]
async fn finishing_during_the_run_sends_completed() {
    let events = events_for(progress(0, false), progress(TOTAL, true)).await;
    assert_eq!(events, ["started", "completed", "stopped"]);
}

/// A resumed run starts with every piece verified but `complete` not yet
/// set: the session marks itself complete only once its loop starts. That
/// is not a completed download.
#[tokio::test]
async fn starting_complete_never_sends_completed() {
    let events = events_for(progress(TOTAL, false), progress(TOTAL, true)).await;
    assert_eq!(events, ["started", "stopped"]);
}
