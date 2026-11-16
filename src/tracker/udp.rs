//! UDP tracker protocol (BEP 15).
//!
//! An announce is two round trips: `connect` gets a connection ID, which is
//! then used for `announce`. Connection IDs are cached for a minute, so later
//! announces take one round trip. Lost packets are retransmitted with a
//! doubling timeout.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::{timeout_at, Instant};

use super::{AnnounceRequest, AnnounceResponse, Event, TrackerError};

const PROTOCOL_ID: u64 = 0x0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;
/// BEP 15 says a connection ID may be used for one minute after it is issued.
const CONNECTION_TTL: Duration = Duration::from_secs(60);
/// Largest datagram we accept. Enough for about 1,400 IPv6 peers.
const MAX_PACKET: usize = 1 << 15;

/// Retransmission policy. BEP 15 suggests 15 s doubling up to 8 times (about
/// an hour); the tracker task already backs off between announces, so a
/// single announce gives up much sooner.
#[derive(Debug, Clone, Copy)]
pub struct Retransmit {
    pub first: Duration,
    pub attempts: u32,
}

impl Default for Retransmit {
    fn default() -> Self {
        // 3 + 6 + 12 = 21 s at most per request.
        Retransmit {
            first: Duration::from_secs(3),
            attempts: 3,
        }
    }
}

/// Holds cached connection IDs, keyed by tracker address.
#[derive(Debug, Default)]
pub struct UdpClient {
    connections: HashMap<SocketAddr, (u64, Instant)>,
    pub retransmit: Retransmit,
}

/// Splits `udp://host:port[/path]` into `host:port`.
pub fn parse_url(url: &str) -> Result<&str, TrackerError> {
    let rest = url
        .strip_prefix("udp://")
        .ok_or_else(|| TrackerError::UnsupportedScheme(url.to_string()))?;
    let authority = rest.split(['/', '?']).next().unwrap_or("");
    // A port is required; IPv6 literals look like "[::1]:6969".
    let has_port = authority.rsplit_once(':').is_some_and(|(h, p)| {
        !h.is_empty() && !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())
    });
    if !has_port {
        return Err(TrackerError::Invalid("udp url"));
    }
    Ok(authority)
}

pub fn encode_connect(tid: u32) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    b[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    b[12..].copy_from_slice(&tid.to_be_bytes());
    b
}

pub fn encode_announce(conn: u64, tid: u32, key: u32, req: &AnnounceRequest) -> [u8; 98] {
    let event: u32 = match req.event {
        None => 0,
        Some(Event::Completed) => 1,
        Some(Event::Started) => 2,
        Some(Event::Stopped) => 3,
    };
    let mut b = [0u8; 98];
    b[0..8].copy_from_slice(&conn.to_be_bytes());
    b[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    b[12..16].copy_from_slice(&tid.to_be_bytes());
    b[16..36].copy_from_slice(&req.info_hash);
    b[36..56].copy_from_slice(&req.peer_id);
    b[56..64].copy_from_slice(&req.downloaded.to_be_bytes());
    b[64..72].copy_from_slice(&req.left.to_be_bytes());
    b[72..80].copy_from_slice(&req.uploaded.to_be_bytes());
    b[80..84].copy_from_slice(&event.to_be_bytes());
    // 84..88: IP address, 0 = use the sender's.
    b[88..92].copy_from_slice(&key.to_be_bytes());
    b[92..96].copy_from_slice(&(-1i32).to_be_bytes()); // num_want: default
    b[96..98].copy_from_slice(&req.port.to_be_bytes());
    b
}

fn be_u32(b: &[u8], at: usize) -> u32 {
    let mut w = [0u8; 4];
    w.copy_from_slice(&b[at..at + 4]);
    u32::from_be_bytes(w)
}

/// What a datagram means for the request with transaction ID `tid`.
#[derive(Debug)]
enum Reply<T> {
    /// Not for us (wrong transaction ID or too short): keep waiting.
    Ignore,
    Ok(T),
    Err(TrackerError),
}

fn check_header(
    b: &[u8],
    tid: u32,
    action: u32,
    min_len: usize,
) -> Option<Result<(), TrackerError>> {
    if b.len() < 8 || be_u32(b, 4) != tid {
        return None;
    }
    let got = be_u32(b, 0);
    if got == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&b[8..]).into_owned();
        return Some(Err(TrackerError::Failure(msg)));
    }
    if got != action || b.len() < min_len {
        return Some(Err(TrackerError::Invalid("udp reply")));
    }
    Some(Ok(()))
}

fn parse_connect(b: &[u8], tid: u32) -> Reply<u64> {
    match check_header(b, tid, ACTION_CONNECT, 16) {
        None => Reply::Ignore,
        Some(Err(e)) => Reply::Err(e),
        Some(Ok(())) => {
            let mut w = [0u8; 8];
            w.copy_from_slice(&b[8..16]);
            Reply::Ok(u64::from_be_bytes(w))
        }
    }
}

/// Parses an announce reply. The peer list holds IPv4 entries (6 bytes) when
/// the request went over IPv4 and IPv6 entries (18 bytes) over IPv6.
fn parse_announce(b: &[u8], tid: u32, ipv6: bool) -> Reply<AnnounceResponse> {
    match check_header(b, tid, ACTION_ANNOUNCE, 20) {
        None => Reply::Ignore,
        Some(Err(e)) => Reply::Err(e),
        Some(Ok(())) => {
            let interval = Duration::from_secs(be_u32(b, 8) as u64);
            let list = &b[20..];
            let mut peers = Vec::new();
            if ipv6 {
                for c in list.as_chunks::<18>().0 {
                    let mut o = [0u8; 16];
                    o.copy_from_slice(&c[..16]);
                    let port = u16::from_be_bytes([c[16], c[17]]);
                    peers.push(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(o)), port));
                }
            } else {
                for c in list.as_chunks::<6>().0 {
                    let ip = Ipv4Addr::new(c[0], c[1], c[2], c[3]);
                    let port = u16::from_be_bytes([c[4], c[5]]);
                    peers.push(SocketAddr::new(IpAddr::V4(ip), port));
                }
            }
            peers.retain(|p| p.port() != 0);
            Reply::Ok(AnnounceResponse { interval, peers })
        }
    }
}

/// Sends `packet` and waits for a matching reply, retransmitting on timeout.
async fn exchange<T>(
    sock: &UdpSocket,
    retransmit: Retransmit,
    packet: &[u8],
    parse: impl Fn(&[u8]) -> Reply<T>,
) -> Result<T, TrackerError> {
    let mut buf = vec![0u8; MAX_PACKET];
    let mut wait = retransmit.first;
    for _ in 0..retransmit.attempts.max(1) {
        sock.send(packet).await.map_err(TrackerError::Io)?;
        let deadline = Instant::now() + wait;
        loop {
            match timeout_at(deadline, sock.recv(&mut buf)).await {
                Err(_) => break, // timed out: retransmit
                // ICMP "port unreachable" shows up as a receive error on a
                // connected socket; treat it like a lost packet.
                Ok(Err(_)) => break,
                Ok(Ok(n)) => match parse(&buf[..n]) {
                    Reply::Ignore => continue,
                    Reply::Ok(v) => return Ok(v),
                    Reply::Err(e) => return Err(e),
                },
            }
        }
        // Wait out the rest of the timeout if the receive failed early, so a
        // refused port is not hammered.
        tokio::time::sleep_until(deadline).await;
        wait *= 2;
    }
    Err(TrackerError::Timeout)
}

impl UdpClient {
    pub fn new(retransmit: Retransmit) -> Self {
        UdpClient {
            connections: HashMap::new(),
            retransmit,
        }
    }

    pub async fn announce(
        &mut self,
        url: &str,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TrackerError> {
        let authority = parse_url(url)?;
        let addr = tokio::net::lookup_host(authority)
            .await
            .map_err(TrackerError::Io)?
            .next()
            .ok_or(TrackerError::Invalid("udp host"))?;
        let bind: SocketAddr = if addr.is_ipv6() {
            (Ipv6Addr::UNSPECIFIED, 0).into()
        } else {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        };
        let sock = UdpSocket::bind(bind).await.map_err(TrackerError::Io)?;
        sock.connect(addr).await.map_err(TrackerError::Io)?;

        let result = self.announce_on(&sock, addr, req).await;
        if result.is_err() {
            // The ID may have expired or the tracker restarted; reconnect next time.
            self.connections.remove(&addr);
        }
        result
    }

    async fn announce_on(
        &mut self,
        sock: &UdpSocket,
        addr: SocketAddr,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TrackerError> {
        let conn = match self.connections.get(&addr) {
            Some(&(id, at)) if at.elapsed() < CONNECTION_TTL => id,
            _ => {
                let tid: u32 = rand::random();
                let at = Instant::now();
                let id = exchange(sock, self.retransmit, &encode_connect(tid), |b| {
                    parse_connect(b, tid)
                })
                .await?;
                self.connections.insert(addr, (id, at));
                id
            }
        };
        let tid: u32 = rand::random();
        let key: u32 = rand::random();
        let packet = encode_announce(conn, tid, key, req);
        let ipv6 = addr.is_ipv6();
        exchange(sock, self.retransmit, &packet, |b| {
            parse_announce(b, tid, ipv6)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [7; 20],
            peer_id: *b"-OX0001-abcdefghijkl",
            port: 6881,
            uploaded: 3,
            downloaded: 2,
            left: 1,
            event: Some(Event::Started),
        }
    }

    #[test]
    fn urls() {
        assert_eq!(
            parse_url("udp://tracker.example:6969/announce").unwrap(),
            "tracker.example:6969"
        );
        assert_eq!(parse_url("udp://[::1]:80").unwrap(), "[::1]:80");
        assert!(parse_url("udp://tracker.example/announce").is_err());
        assert!(parse_url("http://tracker.example:80/").is_err());
    }

    #[test]
    fn packets_match_bep15_layout() {
        let c = encode_connect(0xdead_beef);
        assert_eq!(&c[..8], &[0, 0, 0x04, 0x17, 0x27, 0x10, 0x19, 0x80]);
        assert_eq!(&c[8..], &[0, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef]);

        let a = encode_announce(0x0102_0304_0506_0708, 9, 10, &req());
        assert_eq!(&a[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(be_u32(&a, 8), 1);
        assert_eq!(be_u32(&a, 12), 9);
        assert_eq!(&a[16..36], &[7; 20]);
        assert_eq!(&a[36..56], b"-OX0001-abcdefghijkl");
        assert_eq!(a[63], 2); // downloaded
        assert_eq!(a[71], 1); // left
        assert_eq!(a[79], 3); // uploaded
        assert_eq!(be_u32(&a, 80), 2); // started
        assert_eq!(be_u32(&a, 88), 10);
        assert_eq!(&a[92..96], &[0xff; 4]);
        assert_eq!(&a[96..], &6881u16.to_be_bytes());
    }

    #[test]
    fn replies_are_matched_by_transaction() {
        let mut r = vec![0, 0, 0, 0, 0, 0, 0, 5];
        r.extend_from_slice(&42u64.to_be_bytes());
        assert!(matches!(parse_connect(&r, 5), Reply::Ok(42)));
        assert!(matches!(parse_connect(&r, 6), Reply::Ignore));
        assert!(matches!(parse_connect(&r[..4], 5), Reply::Ignore));

        let mut err = vec![0, 0, 0, 3, 0, 0, 0, 5];
        err.extend_from_slice(b"no such torrent");
        assert!(
            matches!(parse_connect(&err, 5), Reply::Err(TrackerError::Failure(m)) if m == "no such torrent")
        );

        let mut a = vec![0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 7, 8, 0, 0, 0, 1, 0, 0, 0, 2];
        a.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1, 10, 0, 0, 2, 0, 0]);
        let Reply::Ok(resp) = parse_announce(&a, 5, false) else {
            panic!("announce reply not parsed");
        };
        assert_eq!(resp.interval, Duration::from_secs(1800));
        // The port-0 entry is dropped.
        assert_eq!(resp.peers, vec!["127.0.0.1:6881".parse().unwrap()]);
        assert!(matches!(parse_announce(&a[..19], 5, false), Reply::Err(_)));
    }

    /// A tracker on localhost that drops the first connect packet, so the
    /// client has to retransmit, then answers with one peer.
    #[tokio::test]
    async fn announces_to_a_udp_tracker_with_one_lost_packet() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("udp://{}/announce", server.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let mut connects = 0;
            let mut announces = Vec::new();
            while announces.len() < 2 {
                let (n, from) = server.recv_from(&mut buf).await.unwrap();
                let p = &buf[..n];
                match be_u32(p, 8) {
                    ACTION_CONNECT => {
                        connects += 1;
                        if connects == 1 {
                            continue; // lost
                        }
                        let mut r = vec![0, 0, 0, 0];
                        r.extend_from_slice(&p[12..16]);
                        r.extend_from_slice(&0xabcd_u64.to_be_bytes());
                        server.send_to(&r, from).await.unwrap();
                    }
                    ACTION_ANNOUNCE => {
                        announces.push(p.to_vec());
                        let mut r = vec![0, 0, 0, 1];
                        r.extend_from_slice(&p[12..16]);
                        r.extend_from_slice(&[0, 0, 0, 60, 0, 0, 0, 0, 0, 0, 0, 1]);
                        r.extend_from_slice(&[10, 1, 2, 3, 0x1f, 0x90]);
                        server.send_to(&r, from).await.unwrap();
                    }
                    _ => panic!("unexpected action"),
                }
            }
            (connects, announces)
        });

        let mut client = UdpClient::new(Retransmit {
            first: Duration::from_millis(200),
            attempts: 3,
        });
        let r = client.announce(&url, &req()).await.unwrap();
        assert_eq!(r.interval, Duration::from_secs(60));
        assert_eq!(r.peers, vec!["10.1.2.3:8080".parse().unwrap()]);
        // The second announce reuses the cached connection ID.
        client.announce(&url, &req()).await.unwrap();

        let (connects, announces) = task.await.unwrap();
        assert_eq!(connects, 2);
        assert_eq!(&announces[1][..8], &0xabcd_u64.to_be_bytes());
    }

    #[tokio::test]
    async fn silent_tracker_times_out() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("udp://{}", server.local_addr().unwrap());
        let mut client = UdpClient::new(Retransmit {
            first: Duration::from_millis(50),
            attempts: 2,
        });
        let err = client.announce(&url, &req()).await.unwrap_err();
        assert!(matches!(err, TrackerError::Timeout));
        drop(server);
    }
}
