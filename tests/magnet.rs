//! Magnet links: fetching the info dictionary from peers (BEP 9) and then
//! downloading with it.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use common::{fake_tracker, fast_timeouts, make_data, run_client, run_client_within, PIECE};
use futures::{SinkExt, StreamExt};
use oxtorrent::magnet::{fetch_metadata, FetchConfig, Magnet, MagnetError};
use oxtorrent::metainfo::{build_single_file, hex, Torrent};
use oxtorrent::peer::extension::{self, MetadataMsg};
use oxtorrent::peer::generate_peer_id;
use oxtorrent::peer::handshake::{Handshake, HANDSHAKE_LEN};
use oxtorrent::peer::message::{Message, MessageCodec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

fn fetch_config() -> FetchConfig {
    let mut c = FetchConfig::new(generate_peer_id(), 6881);
    c.timeouts = fast_timeouts();
    c.per_peer = Duration::from_secs(5);
    c
}

/// Starts an Oxtorrent seeder for `data` and waits until it has announced.
async fn spawn_seeder(
    data: &[u8],
    name: &str,
    tracker: &fake_tracker::FakeTracker,
    token: &CancellationToken,
) -> (Torrent, tempfile::TempDir) {
    let torrent =
        Torrent::from_bytes(&build_single_file(name, data, PIECE, Some(&tracker.url))).unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(name), data).unwrap();
    let (t, tok, path) = (torrent.clone(), token.clone(), dir.path().to_path_buf());
    tokio::spawn(async move { run_client(t, &path, true, fast_timeouts(), tok).await });
    let start = tokio::time::Instant::now();
    while tracker.announced.lock().unwrap().is_empty() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "seeder never announced"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (torrent, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_link_downloads_from_an_oxtorrent_seeder() {
    // Big enough that the info dictionary spans two metadata pieces:
    // 1,000 pieces x 20 bytes of hashes.
    let data = make_data(1000 * 1024 + 77, 30);
    let tracker = fake_tracker::spawn_udp(Vec::new()).await;
    let token = CancellationToken::new();
    let (seeded, _seed_dir) = {
        let torrent =
            Torrent::from_bytes(&build_single_file("m.bin", &data, 1024, Some(&tracker.url)))
                .unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("m.bin"), &data).unwrap();
        let (t, tok, path) = (torrent.clone(), token.clone(), dir.path().to_path_buf());
        tokio::spawn(async move { run_client(t, &path, true, fast_timeouts(), tok).await });
        let start = tokio::time::Instant::now();
        while tracker.announced.lock().unwrap().is_empty() {
            assert!(start.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        (torrent, dir)
    };
    assert!(seeded.info_bytes.len() > extension::METADATA_PIECE);

    let link = format!(
        "magnet:?xt=urn:btih:{}&dn=m.bin&tr={}",
        hex(&seeded.info_hash),
        percent_encoding::utf8_percent_encode(&tracker.url, percent_encoding::NON_ALPHANUMERIC)
    );
    let magnet = Magnet::parse(&link).unwrap();
    let fetched = tokio::time::timeout(
        Duration::from_secs(30),
        fetch_metadata(&magnet, &fetch_config(), &[], &CancellationToken::new()),
    )
    .await
    .expect("metadata fetch timed out")
    .unwrap();
    assert_eq!(fetched.info, seeded.info_bytes);
    assert!(!fetched.peers.is_empty());

    let torrent = Torrent::from_info(&fetched.info, &magnet.trackers).unwrap();
    assert_eq!(torrent.info_hash, seeded.info_hash);
    let dir = tempfile::tempdir().unwrap();
    let outcome = run_client_within(
        Duration::from_secs(60),
        torrent,
        dir.path(),
        fast_timeouts(),
    )
    .await;
    assert!(outcome.complete);
    assert_eq!(std::fs::read(dir.path().join("m.bin")).unwrap(), data);
    token.cancel();
}

#[derive(Clone, Copy)]
enum Bad {
    /// Sends metadata of the right size whose hash does not match.
    WrongData,
    /// Rejects every metadata request.
    Reject,
    /// Does not set the extension bit.
    NoExtensions,
    /// Claims a 1 GiB info dictionary.
    HugeSize,
}

/// A peer that misbehaves during the metadata exchange.
async fn spawn_bad_peer(info_hash: [u8; 20], size: usize, how: Bad) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut hs = [0u8; HANDSHAKE_LEN];
                if s.read_exact(&mut hs).await.is_err() {
                    return;
                }
                let mut ours = Handshake::new(info_hash, generate_peer_id());
                if !matches!(how, Bad::NoExtensions) {
                    extension::set_extension_bit(&mut ours.reserved);
                }
                if s.write_all(&ours.to_bytes()).await.is_err() {
                    return;
                }
                let mut f = Framed::new(s, MessageCodec::new(0));
                let claimed = if matches!(how, Bad::HugeSize) {
                    1 << 30
                } else {
                    size
                };
                let hs = Bytes::from(format!(
                    "d1:md11:ut_metadatai2ee13:metadata_sizei{claimed}ee"
                ));
                if f.send(Message::Extended { id: 0, payload: hs })
                    .await
                    .is_err()
                {
                    return;
                }
                while let Some(Ok(m)) = f.next().await {
                    let Message::Extended { id: 2, payload } = m else {
                        continue;
                    };
                    let Ok(MetadataMsg::Request { piece }) = MetadataMsg::parse(&payload) else {
                        continue;
                    };
                    let reply = match how {
                        Bad::Reject => MetadataMsg::Reject { piece },
                        _ => {
                            let start = piece as usize * extension::METADATA_PIECE;
                            let len = (size - start).min(extension::METADATA_PIECE);
                            MetadataMsg::Data {
                                piece,
                                total_size: Some(size),
                                data: Bytes::from(vec![0x55; len]),
                            }
                        }
                    };
                    let payload = reply.encode();
                    if f.send(Message::Extended {
                        id: extension::UT_METADATA_ID,
                        payload,
                    })
                    .await
                    .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_metadata_peers_are_skipped() {
    let data = make_data(200 * 1024, 31);
    let tracker = fake_tracker::spawn(Vec::new()).await;
    let token = CancellationToken::new();
    let (seeded, _dir) = spawn_seeder(&data, "b.bin", &tracker, &token).await;
    let seeder = tracker.announced.lock().unwrap()[0];
    let (hash, size) = (seeded.info_hash, seeded.info_bytes.len());

    let mut peers = Vec::new();
    for how in [
        Bad::WrongData,
        Bad::Reject,
        Bad::NoExtensions,
        Bad::HugeSize,
    ] {
        peers.push(spawn_bad_peer(hash, size, how).await);
    }
    // Only one peer at a time, bad ones first, so each is tried and refused.
    let mut cfg = fetch_config();
    cfg.parallel = 1;
    peers.push(seeder);
    let magnet = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", hex(&hash))).unwrap();
    let fetched = tokio::time::timeout(
        Duration::from_secs(30),
        fetch_metadata(&magnet, &cfg, &peers, &CancellationToken::new()),
    )
    .await
    .expect("metadata fetch timed out")
    .unwrap();
    assert_eq!(fetched.info, seeded.info_bytes);
    token.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_bad_peers_never_yield_metadata() {
    let hash = [9u8; 20];
    let mut peers = Vec::new();
    for how in [Bad::WrongData, Bad::Reject] {
        peers.push(spawn_bad_peer(hash, 30_000, how).await);
    }
    let magnet = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", hex(&hash))).unwrap();
    let token = CancellationToken::new();
    let cancel = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        cancel.cancel();
    });
    let r = fetch_metadata(&magnet, &fetch_config(), &peers, &token).await;
    assert_eq!(r.unwrap_err(), MagnetError::Cancelled);

    let empty = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", hex(&hash))).unwrap();
    assert_eq!(
        fetch_metadata(&empty, &fetch_config(), &[], &token)
            .await
            .unwrap_err(),
        MagnetError::NoSources
    );
}
