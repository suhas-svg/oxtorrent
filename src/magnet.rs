//! Magnet links, and fetching a torrent's info dictionary from peers
//! (BEP 9) so a download can start without a `.torrent` file.
//!
//! Fetching works like a small download of its own: the tracker task and
//! the DHT find peers, several peers are tried at once, and each one that supports the
//! extension protocol is asked for every metadata piece. The first complete
//! copy whose SHA-1 matches the magnet link's info hash wins. The metadata
//! is small (tens of KiB), so asking one peer for all of it is simpler than
//! splitting the pieces across peers, and costs little.

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use percent_encoding::percent_decode_str;
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::dht::Dht;
use crate::peer::connection::Timeouts;
use crate::peer::extension::{self, MetadataMsg};
use crate::peer::handshake::{self, Handshake};
use crate::peer::message::{Message, MessageCodec};
use crate::session::{Progress, SessionEvent};
use crate::tracker::{self, TrackerConfig};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MagnetError {
    #[error("not a magnet link")]
    NotMagnet,
    #[error("magnet link has no BitTorrent info hash (xt=urn:btih:...)")]
    NoInfoHash,
    #[error("invalid info hash {0:?}: expected 40 hex or 32 base32 characters")]
    BadInfoHash(String),
    #[error("magnet link has no trackers or peers, and the DHT is off")]
    NoSources,
    #[error("cancelled before the metadata arrived")]
    Cancelled,
}

/// A parsed magnet link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Magnet {
    pub info_hash: [u8; 20],
    /// Display name (`dn`), only for messages until the metadata arrives.
    pub name: Option<String>,
    /// Tracker URLs (`tr`), in the order given.
    pub trackers: Vec<String>,
    /// Peer addresses (`x.pe`).
    pub peers: Vec<SocketAddr>,
}

impl Magnet {
    pub fn parse(uri: &str) -> Result<Magnet, MagnetError> {
        let query = uri.strip_prefix("magnet:?").ok_or(MagnetError::NotMagnet)?;
        let mut info_hash = None;
        let mut name = None;
        let mut trackers = Vec::new();
        let mut peers = Vec::new();
        for pair in query.split('&') {
            let Some((key, raw)) = pair.split_once('=') else {
                continue;
            };
            let value = percent_decode_str(raw).decode_utf8_lossy().into_owned();
            match key {
                "xt" => {
                    if let Some(h) = value.strip_prefix("urn:btih:") {
                        info_hash = Some(parse_btih(h)?);
                    }
                }
                "dn" => {
                    let plus_as_space = percent_decode_str(&raw.replace('+', " "))
                        .decode_utf8_lossy()
                        .into_owned();
                    name = Some(plus_as_space);
                }
                "tr" => {
                    if !trackers.contains(&value) {
                        trackers.push(value);
                    }
                }
                "x.pe" => {
                    if let Ok(a) = value.parse() {
                        peers.push(a);
                    }
                }
                _ => {}
            }
        }
        Ok(Magnet {
            info_hash: info_hash.ok_or(MagnetError::NoInfoHash)?,
            name,
            trackers,
            peers,
        })
    }
}

/// Decodes a BitTorrent v1 info hash: 40 hex digits, or 32 base32
/// characters (RFC 4648 alphabet, as older magnet links use).
fn parse_btih(s: &str) -> Result<[u8; 20], MagnetError> {
    let bad = || MagnetError::BadInfoHash(s.to_string());
    let mut out = [0u8; 20];
    match s.len() {
        40 => {
            for (i, o) in out.iter_mut().enumerate() {
                *o = u8::from_str_radix(s.get(2 * i..2 * i + 2).ok_or_else(bad)?, 16)
                    .map_err(|_| bad())?;
            }
        }
        32 => {
            let mut acc: u64 = 0;
            let mut bits = 0;
            let mut n = 0;
            for c in s.bytes() {
                let v = match c.to_ascii_uppercase() {
                    c @ b'A'..=b'Z' => c - b'A',
                    c @ b'2'..=b'7' => c - b'2' + 26,
                    _ => return Err(bad()),
                };
                acc = (acc << 5) | v as u64;
                bits += 5;
                if bits >= 8 {
                    bits -= 8;
                    out[n] = (acc >> bits) as u8;
                    n += 1;
                }
            }
        }
        _ => return Err(bad()),
    }
    Ok(out)
}

/// Settings for [`fetch_metadata`].
#[derive(Debug, Clone)]
pub struct FetchConfig {
    pub peer_id: [u8; 20],
    /// Our listening port, reported to trackers.
    pub port: u16,
    pub timeouts: Timeouts,
    /// Peers asked at the same time.
    pub parallel: usize,
    /// How long one peer gets to deliver the whole info dictionary.
    pub per_peer: Duration,
    /// DHT node to search for peers on, alongside the trackers.
    pub dht: Option<Dht>,
}

impl FetchConfig {
    pub fn new(peer_id: [u8; 20], port: u16) -> Self {
        FetchConfig {
            peer_id,
            port,
            timeouts: Timeouts::default(),
            parallel: 8,
            per_peer: Duration::from_secs(30),
            dht: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Fetched {
    /// The info dictionary; its SHA-1 is the magnet link's info hash.
    pub info: Bytes,
    /// Every peer address seen, to give the download a head start.
    pub peers: Vec<SocketAddr>,
}

/// A peer address is tried this many times at most.
const MAX_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(20);

/// Finds peers through the magnet link's trackers (and its `x.pe` peers
/// plus `extra_peers`) and fetches the info dictionary from them. Runs until
/// it succeeds or `token` is cancelled.
pub async fn fetch_metadata(
    magnet: &Magnet,
    cfg: &FetchConfig,
    extra_peers: &[SocketAddr],
    token: &CancellationToken,
) -> Result<Fetched, MagnetError> {
    if magnet.trackers.is_empty()
        && magnet.peers.is_empty()
        && extra_peers.is_empty()
        && cfg.dht.is_none()
    {
        return Err(MagnetError::NoSources);
    }
    let inner = token.child_token();
    let (events_tx, mut events_rx) = mpsc::channel(64);
    // The size is unknown until the metadata arrives; report one byte left
    // so trackers treat us as a downloader.
    let (_progress_tx, progress_rx) = watch::channel(Progress {
        total: 1,
        ..Progress::default()
    });
    let tracker_task = (!magnet.trackers.is_empty()).then(|| {
        tokio::spawn(tracker::run_tracker(
            TrackerConfig {
                urls: magnet.trackers.clone(),
                info_hash: magnet.info_hash,
                peer_id: cfg.peer_id,
                port: cfg.port,
                udp_retransmit: Default::default(),
            },
            progress_rx,
            events_tx.clone(),
            inner.clone(),
        ))
    });
    if let Some(dht) = cfg.dht.clone() {
        tokio::spawn(crate::dht::run_search(
            dht,
            magnet.info_hash,
            None,
            events_tx.clone(),
            inner.clone(),
            Duration::from_secs(20),
            Duration::from_secs(60),
        ));
    }
    let mut sources_open = tracker_task.is_some() || cfg.dht.is_some();
    drop(events_tx);

    let mut queue: VecDeque<SocketAddr> = VecDeque::new();
    let mut seen: HashSet<SocketAddr> = HashSet::new();
    let mut order: Vec<SocketAddr> = Vec::new();
    let mut attempts: std::collections::HashMap<SocketAddr, u32> = Default::default();
    let mut retry: Vec<(Instant, SocketAddr)> = Vec::new();
    let mut add = |addrs: &[SocketAddr], queue: &mut VecDeque<SocketAddr>| {
        for &a in addrs {
            if seen.insert(a) {
                order.push(a);
                queue.push_back(a);
            }
        }
    };
    add(&magnet.peers, &mut queue);
    add(extra_peers, &mut queue);

    let mut tasks: JoinSet<(SocketAddr, Result<Bytes, String>)> = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    let result = loop {
        let now = Instant::now();
        retry.retain(|&(at, a)| {
            if at <= now {
                queue.push_back(a);
                false
            } else {
                true
            }
        });
        while tasks.len() < cfg.parallel.max(1) {
            let Some(addr) = queue.pop_front() else {
                break;
            };
            *attempts.entry(addr).or_insert(0) += 1;
            let (hash, id, t, limit) = (magnet.info_hash, cfg.peer_id, cfg.timeouts, cfg.per_peer);
            tasks.spawn(async move {
                let r = timeout(limit, fetch_from_peer(addr, hash, id, t))
                    .await
                    .unwrap_or_else(|_| Err("timed out".to_string()));
                (addr, r)
            });
        }

        tokio::select! {
            _ = token.cancelled() => break Err(MagnetError::Cancelled),
            ev = events_rx.recv(), if sources_open => match ev {
                Some(SessionEvent::PeersFound(addrs)) => {
                    debug!("metadata: {} peers found", addrs.len());
                    add(&addrs, &mut queue);
                }
                Some(_) => {}
                None => sources_open = false,
            },
            Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                let Ok((addr, r)) = done else { continue };
                match r {
                    Ok(info) => {
                        info!("metadata ({} bytes) received from {addr}", info.len());
                        break Ok(info);
                    }
                    Err(e) => {
                        debug!("metadata from {addr} failed: {e}");
                        if attempts.get(&addr).copied().unwrap_or(0) < MAX_ATTEMPTS {
                            retry.push((Instant::now() + RETRY_DELAY, addr));
                        }
                    }
                }
            }
            _ = tick.tick() => {}
        }
    };

    tasks.abort_all();
    inner.cancel();
    if let Some(t) = tracker_task {
        // Lets the tracker task send `stopped`; the download announces again.
        let _ = timeout(Duration::from_secs(2), t).await;
    }
    result.map(|info| Fetched { info, peers: order })
}

/// Connects to one peer and asks it for the whole info dictionary.
/// Returns it only if its SHA-1 matches `info_hash`.
pub async fn fetch_from_peer(
    addr: SocketAddr,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    timeouts: Timeouts,
) -> Result<Bytes, String> {
    let mut stream = timeout(timeouts.connect, TcpStream::connect(addr))
        .await
        .map_err(|_| "connect timed out".to_string())?
        .map_err(|e| format!("connect failed: {e}"))?;
    let _ = stream.set_nodelay(true);
    let mut ours = Handshake::new(info_hash, peer_id);
    extension::set_extension_bit(&mut ours.reserved);
    let theirs = timeout(
        timeouts.handshake,
        handshake::exchange(&mut stream, &ours, true),
    )
    .await
    .map_err(|_| "handshake timed out".to_string())?
    .map_err(|e| e.to_string())?;
    if !extension::supports_extensions(&theirs.reserved) {
        return Err("peer does not support the extension protocol".to_string());
    }

    // The piece count is unknown, so allow bitfields for large torrents.
    let mut framed = Framed::new(stream, MessageCodec::with_max_len(1 << 20));
    framed
        .send(Message::Extended {
            id: extension::HANDSHAKE_ID,
            payload: extension::encode_handshake(None),
        })
        .await
        .map_err(|e| e.to_string())?;

    let idle = timeouts.handshake.max(Duration::from_secs(5));
    let mut buf: Vec<u8> = Vec::new();
    let mut got: Vec<bool> = Vec::new();
    let mut size = 0usize;
    loop {
        let msg = match timeout_at(Instant::now() + idle, framed.next()).await {
            Err(_) => return Err("peer went quiet".to_string()),
            Ok(None) => return Err("connection closed".to_string()),
            Ok(Some(Err(e))) => return Err(e.to_string()),
            Ok(Some(Ok(m))) => m,
        };
        let Message::Extended { id, payload } = msg else {
            continue; // bitfield, have, unchoke...: not needed here
        };
        if id == extension::HANDSHAKE_ID && got.is_empty() {
            let h = extension::parse_handshake(&payload).map_err(|e| e.to_string())?;
            let their_id = h
                .ut_metadata
                .ok_or_else(|| "peer does not support ut_metadata".to_string())?;
            size = h
                .metadata_size
                .ok_or_else(|| "peer does not have the metadata".to_string())?;
            if size == 0 || size > extension::MAX_METADATA_SIZE {
                return Err(format!("implausible metadata size {size}"));
            }
            let n = extension::metadata_pieces(size);
            buf = vec![0; size];
            got = vec![false; n];
            for piece in 0..n as u32 {
                framed
                    .feed(Message::Extended {
                        id: their_id,
                        payload: MetadataMsg::Request { piece }.encode(),
                    })
                    .await
                    .map_err(|e| e.to_string())?;
            }
            framed.flush().await.map_err(|e| e.to_string())?;
        } else if id == extension::UT_METADATA_ID && !got.is_empty() {
            match MetadataMsg::parse(&payload).map_err(|e| e.to_string())? {
                MetadataMsg::Data {
                    piece,
                    total_size,
                    data,
                } => {
                    let i = piece as usize;
                    if i >= got.len() {
                        return Err(format!("metadata piece {piece} out of range"));
                    }
                    if total_size.is_some_and(|t| t != size) {
                        return Err("metadata total_size changed".to_string());
                    }
                    let start = i * extension::METADATA_PIECE;
                    let end = (start + extension::METADATA_PIECE).min(size);
                    if data.len() != end - start {
                        return Err(format!(
                            "metadata piece {piece} has {} bytes, expected {}",
                            data.len(),
                            end - start
                        ));
                    }
                    buf[start..end].copy_from_slice(&data);
                    got[i] = true;
                    if got.iter().all(|&g| g) {
                        let hash: [u8; 20] = Sha1::digest(&buf).into();
                        if hash != info_hash {
                            return Err("metadata does not match the info hash".to_string());
                        }
                        return Ok(Bytes::from(buf));
                    }
                }
                MetadataMsg::Reject { piece } => {
                    return Err(format!("peer rejected metadata piece {piece}"));
                }
                MetadataMsg::Request { .. } => {} // we have nothing to serve
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "7acf8fb590b2060dd9c3146ef770169d593433b0";

    #[test]
    fn parses_hex_magnet() {
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{HEX}&dn=debian-13.7.0-amd64-netinst.iso\
             &tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce\
             &tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce\
             &tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce\
             &x.pe=10.0.0.1:6881&x.pe=bad"
        ))
        .unwrap();
        assert_eq!(crate::metainfo::hex(&m.info_hash), HEX);
        assert_eq!(m.name.as_deref(), Some("debian-13.7.0-amd64-netinst.iso"));
        assert_eq!(
            m.trackers,
            vec![
                "http://bttracker.debian.org:6969/announce",
                "udp://tracker.opentrackr.org:1337/announce"
            ]
        );
        assert_eq!(m.peers, vec!["10.0.0.1:6881".parse().unwrap()]);
    }

    #[test]
    fn parses_base32_and_uppercase_hex() {
        // Same hash in base32 (RFC 4648).
        let m = Magnet::parse("magnet:?xt=urn:btih:PLHY7NMQWIDA3WODCRXPO4AWTVMTIM5Q").unwrap();
        assert_eq!(crate::metainfo::hex(&m.info_hash), HEX);
        let up = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", HEX.to_uppercase())).unwrap();
        assert_eq!(up.info_hash, m.info_hash);
        let named = Magnet::parse(&format!("magnet:?dn=a+b%20c&xt=urn:btih:{HEX}")).unwrap();
        assert_eq!(named.name.as_deref(), Some("a b c"));
    }

    #[test]
    fn rejects_bad_links() {
        assert_eq!(Magnet::parse("http://x"), Err(MagnetError::NotMagnet));
        assert_eq!(Magnet::parse("magnet:?dn=x"), Err(MagnetError::NoInfoHash));
        assert!(matches!(
            Magnet::parse("magnet:?xt=urn:btih:abcd"),
            Err(MagnetError::BadInfoHash(_))
        ));
        assert!(matches!(
            Magnet::parse(&format!("magnet:?xt=urn:btih:{}", "zz".repeat(20))),
            Err(MagnetError::BadInfoHash(_))
        ));
        assert!(matches!(
            Magnet::parse("magnet:?xt=urn:btih:11111111111111111111111111111111"),
            Err(MagnetError::BadInfoHash(_))
        ));
    }
}
