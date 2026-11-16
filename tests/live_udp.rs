//! Announces to a real public UDP tracker. Needs the internet, so it is
//! ignored by default:
//!
//! ```text
//! cargo test --test live_udp -- --ignored --nocapture
//! ```
//!
//! Set `OXTORRENT_UDP_TRACKER` to try another tracker.

use oxtorrent::tracker::udp::{Retransmit, UdpClient};
use oxtorrent::tracker::{AnnounceRequest, Event};

#[tokio::test]
#[ignore = "needs the internet"]
async fn announce_to_public_udp_tracker() {
    let url = std::env::var("OXTORRENT_UDP_TRACKER")
        .unwrap_or_else(|_| "udp://tracker.opentrackr.org:1337/announce".to_string());
    // debian-13.7.0-amd64-netinst.iso
    let info_hash = [
        0x7a, 0xcf, 0x8f, 0xb5, 0x90, 0xb2, 0x06, 0x0d, 0xd9, 0xc3, 0x14, 0x6e, 0xf7, 0x70,
        0x16, 0x9d, 0x59, 0x34, 0x33, 0xb0,
    ];
    let mut req = AnnounceRequest {
        info_hash,
        peer_id: oxtorrent::peer::generate_peer_id(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 1,
        event: Some(Event::Started),
    };
    let mut client = UdpClient::new(Retransmit::default());
    let resp = client.announce(&url, &req).await.expect("announce failed");
    println!("{url}: interval {:?}, {} peers", resp.interval, resp.peers.len());
    for p in resp.peers.iter().take(5) {
        println!("  {p}");
    }
    req.event = Some(Event::Stopped);
    let _ = client.announce(&url, &req).await;
}
