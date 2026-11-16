//! Tracker announces over HTTP (BEP 3, BEP 23) and UDP (BEP 15), and the
//! tracker task that announces on a timer.

pub mod udp;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep_until, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::bencode::{self, BencodeError, Value};
use crate::session::{Progress, SessionEvent};

/// Characters left unescaped in query values (RFC 3986 "unreserved").
const QUERY_SAFE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

const MIN_INTERVAL: Duration = Duration::from_secs(30);
const MAX_INTERVAL: Duration = Duration::from_secs(2 * 60 * 60);
const FIRST_RETRY: Duration = Duration::from_secs(15);
const MAX_RETRY: Duration = Duration::from_secs(30 * 60);
const STOP_TIMEOUT: Duration = Duration::from_millis(1500);

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("tracker answered with HTTP status {0}")]
    Status(u16),
    #[error("tracker reply is not valid bencode: {0}")]
    Bencode(#[from] BencodeError),
    #[error("tracker rejected the request: {0}")]
    Failure(String),
    #[error("tracker reply has an invalid `{0}` field")]
    Invalid(&'static str),
    #[error("unsupported tracker URL scheme: {0}")]
    UnsupportedScheme(String),
    #[error("no usable tracker")]
    NoTracker,
    #[error("network error: {0}")]
    Io(std::io::Error),
    #[error("tracker did not answer")]
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Started,
    Completed,
    Stopped,
}

impl Event {
    fn as_str(self) -> &'static str {
        match self {
            Event::Started => "started",
            Event::Completed => "completed",
            Event::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: Option<Event>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: Duration,
    pub peers: Vec<SocketAddr>,
}

/// Appends the announce query to `base`, which may already have a query.
pub fn build_url(base: &str, req: &AnnounceRequest) -> String {
    let sep = if base.contains('?') { '&' } else { '?' };
    let mut url = format!(
        "{base}{sep}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1",
        percent_encode(&req.info_hash, QUERY_SAFE),
        percent_encode(&req.peer_id, QUERY_SAFE),
        req.port,
        req.uploaded,
        req.downloaded,
        req.left,
    );
    if let Some(e) = req.event {
        url.push_str("&event=");
        url.push_str(e.as_str());
    }
    url
}

/// Parses a tracker reply. Accepts the compact peer list (BEP 23), the
/// older list-of-dictionaries form, and compact IPv6 peers (`peers6`).
pub fn parse_response(body: &[u8]) -> Result<AnnounceResponse, TrackerError> {
    let root = bencode::decode(body)?;
    if root.as_dict().is_none() {
        return Err(TrackerError::Invalid("root"));
    }
    if let Some(reason) = root.get("failure reason") {
        let text = reason
            .as_bytes()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default();
        return Err(TrackerError::Failure(text));
    }
    let interval = root
        .get("interval")
        .and_then(Value::as_int)
        .filter(|&i| i >= 0)
        .ok_or(TrackerError::Invalid("interval"))?;

    let mut peers = Vec::new();
    match root.get("peers") {
        Some(Value::Bytes(b)) => {
            if b.len() % 6 != 0 {
                return Err(TrackerError::Invalid("peers"));
            }
            for c in b.as_chunks::<6>().0 {
                let ip = Ipv4Addr::new(c[0], c[1], c[2], c[3]);
                let port = u16::from_be_bytes([c[4], c[5]]);
                peers.push(SocketAddr::new(IpAddr::V4(ip), port));
            }
        }
        Some(Value::List(list)) => {
            for p in list {
                let ip = p
                    .get("ip")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<IpAddr>().ok());
                let port = p
                    .get("port")
                    .and_then(Value::as_int)
                    .and_then(|n| u16::try_from(n).ok());
                if let (Some(ip), Some(port)) = (ip, port) {
                    peers.push(SocketAddr::new(ip, port));
                }
            }
        }
        Some(_) => return Err(TrackerError::Invalid("peers")),
        None => {}
    }
    if let Some(Value::Bytes(b)) = root.get("peers6") {
        for c in b.as_chunks::<18>().0 {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&c[..16]);
            let port = u16::from_be_bytes([c[16], c[17]]);
            peers.push(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port));
        }
    }
    peers.retain(|p| p.port() != 0);
    Ok(AnnounceResponse {
        interval: Duration::from_secs(interval as u64),
        peers,
    })
}

/// Announces over HTTP(S) or UDP depending on the URL scheme.
pub struct Announcer {
    http: reqwest::Client,
    udp: udp::UdpClient,
}

impl Announcer {
    pub fn new(http: reqwest::Client, retransmit: udp::Retransmit) -> Self {
        Announcer {
            http,
            udp: udp::UdpClient::new(retransmit),
        }
    }

    pub async fn announce(
        &mut self,
        base: &str,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TrackerError> {
        if base.starts_with("udp://") {
            return self.udp.announce(base, req).await;
        }
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            return Err(TrackerError::UnsupportedScheme(base.to_string()));
        }
        let resp = self.http.get(build_url(base, req)).send().await?;
        if !resp.status().is_success() {
            return Err(TrackerError::Status(resp.status().as_u16()));
        }
        let body = resp.bytes().await?;
        parse_response(&body)
    }
}

pub struct TrackerConfig {
    pub urls: Vec<String>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    /// UDP retransmission policy; tests shorten it.
    pub udp_retransmit: udp::Retransmit,
}

/// Tries each URL in order; a URL that works is moved to the front (BEP 12).
async fn announce_any(
    client: &mut Announcer,
    urls: &mut Vec<String>,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse, TrackerError> {
    let mut last = TrackerError::NoTracker;
    for i in 0..urls.len() {
        match client.announce(&urls[i], req).await {
            Ok(resp) => {
                let url = urls.remove(i);
                urls.insert(0, url);
                return Ok(resp);
            }
            Err(TrackerError::UnsupportedScheme(u)) => {
                debug!("skipping tracker {u}: unsupported scheme");
            }
            Err(e) => {
                debug!("tracker {} failed: {e}", urls[i]);
                last = e;
            }
        }
    }
    Err(last)
}

/// The tracker task. Announces `started`, then again every interval, sends
/// `completed` when the download finishes and `stopped` on shutdown. New
/// peer addresses go to the session as [`SessionEvent::PeersFound`].
pub async fn run_tracker(
    mut cfg: TrackerConfig,
    mut progress: watch::Receiver<Progress>,
    events: mpsc::Sender<SessionEvent>,
    token: CancellationToken,
) {
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent(concat!("Oxtorrent/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("cannot create HTTP client: {e}");
            return;
        }
    };
    let mut client = Announcer::new(http, cfg.udp_retransmit);

    let make_req = |p: &Progress, event| AnnounceRequest {
        info_hash: cfg.info_hash,
        peer_id: cfg.peer_id,
        port: cfg.port,
        uploaded: p.uploaded,
        downloaded: p.downloaded,
        left: p.total.saturating_sub(p.verified_bytes),
        event,
    };

    let mut event = Some(Event::Started);
    let mut completed_sent = progress.borrow().complete;
    let mut announced = false;
    let mut retry = FIRST_RETRY;

    'outer: loop {
        let req = make_req(&progress.borrow(), event);
        let result = tokio::select! {
            r = announce_any(&mut client, &mut cfg.urls, &req) => r,
            _ = token.cancelled() => break 'outer,
        };
        let wait = match result {
            Ok(resp) => {
                info!("tracker returned {} peers", resp.peers.len());
                announced = true;
                event = None;
                retry = FIRST_RETRY;
                if events
                    .send(SessionEvent::PeersFound(resp.peers))
                    .await
                    .is_err()
                {
                    break 'outer;
                }
                resp.interval.clamp(MIN_INTERVAL, MAX_INTERVAL)
            }
            Err(e) => {
                warn!(
                    "tracker announce failed: {e}; retrying in {}s",
                    retry.as_secs()
                );
                let w = retry;
                retry = (retry * 2).min(MAX_RETRY);
                w
            }
        };

        let deadline = Instant::now() + wait;
        loop {
            tokio::select! {
                _ = sleep_until(deadline) => break,
                _ = token.cancelled() => break 'outer,
                changed = progress.changed() => {
                    if changed.is_err() {
                        break 'outer;
                    }
                    if progress.borrow().complete && !completed_sent {
                        completed_sent = true;
                        event = Some(Event::Completed);
                        break;
                    }
                }
            }
        }
    }

    if announced {
        let req = make_req(&progress.borrow(), Some(Event::Stopped));
        if timeout(STOP_TIMEOUT, announce_any(&mut client, &mut cfg.urls, &req))
            .await
            .is_err()
        {
            debug!("tracker did not answer the stopped announce in time");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x9a,
            ],
            peer_id: *b"-OX0001-abcdefghijkl",
            port: 6881,
            uploaded: 0,
            downloaded: 10,
            left: 20,
            event: Some(Event::Started),
        }
    }

    #[test]
    fn url_is_percent_encoded() {
        let url = build_url("http://t.example/announce", &req());
        assert!(url.starts_with(
            "http://t.example/announce?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A&peer_id=-OX0001-abcdefghijkl&port=6881"
        ));
        assert!(url.ends_with("&left=20&compact=1&event=started"));
        let url2 = build_url("http://t.example/a?key=1", &req());
        assert!(url2.starts_with("http://t.example/a?key=1&info_hash="));
    }

    #[test]
    fn parses_compact_reply() {
        let body = b"d8:intervali1800e5:peers12:\x7f\x00\x00\x01\x1a\xe1\x0a\x00\x00\x02\x00\x50e";
        let r = parse_response(body).unwrap();
        assert_eq!(r.interval, Duration::from_secs(1800));
        assert_eq!(
            r.peers,
            vec![
                "127.0.0.1:6881".parse().unwrap(),
                "10.0.0.2:80".parse().unwrap()
            ]
        );
    }

    #[test]
    fn parses_dictionary_peers_and_failure() {
        let body = b"d8:intervali60e5:peersld2:ip9:127.0.0.14:porti7000eeee";
        let r = parse_response(body).unwrap();
        assert_eq!(r.peers, vec!["127.0.0.1:7000".parse().unwrap()]);

        let fail = b"d14:failure reason9:not founde";
        assert!(matches!(parse_response(fail), Err(TrackerError::Failure(m)) if m == "not found"));

        let bad = b"d8:intervali60e5:peers5:abcdee";
        assert!(matches!(
            parse_response(bad),
            Err(TrackerError::Invalid("peers"))
        ));
    }
}
