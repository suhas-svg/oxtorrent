//! Joins the real Mainline DHT and looks up peers for the Debian 13.7.0
//! netinst image. Needs the internet, so it is ignored by default:
//!
//! ```text
//! cargo test --test live_dht -- --ignored --nocapture
//! ```

use std::time::Instant;

use oxtorrent::dht::{Dht, DhtConfig};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the internet"]
async fn find_debian_peers_on_the_dht() {
    let token = CancellationToken::new();
    let dht = Dht::start(DhtConfig::default(), token.clone())
        .await
        .unwrap();
    let start = Instant::now();
    let nodes = dht.bootstrap().await;
    println!("bootstrap: {nodes} nodes in {:.1?}", start.elapsed());
    assert!(nodes > 0, "could not join the DHT");

    let hash = [
        0x7a, 0xcf, 0x8f, 0xb5, 0x90, 0xb2, 0x06, 0x0d, 0xd9, 0xc3, 0x14, 0x6e, 0xf7, 0x70, 0x16,
        0x9d, 0x59, 0x34, 0x33, 0xb0,
    ];
    let start = Instant::now();
    let peers = dht.get_peers(hash, None).await;
    println!(
        "get_peers: {} peers in {:.1?}",
        peers.len(),
        start.elapsed()
    );
    for p in peers.iter().take(5) {
        println!("  {p}");
    }
    assert!(!peers.is_empty(), "no peers found");
    token.cancel();
}
