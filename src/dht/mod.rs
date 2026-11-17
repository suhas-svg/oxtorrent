//! A Mainline DHT node (BEP 5): finds peers for an info hash without a
//! tracker, and lets other nodes find us.
//!
//! The node is one UDP socket shared by two tasks: a receive loop that
//! answers queries and hands responses to whoever is waiting for them, and
//! a maintenance loop that keeps the routing table filled. Lookups run in
//! the caller's task: they query the closest known nodes three at a time,
//! moving toward the target until the eight closest nodes have all
//! answered (Kademlia's iterative lookup).
//!
//! IPv4 only. Node IDs are random per run.

pub mod krpc;
pub mod routing;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

use crate::session::SessionEvent;
use krpc::{Body, Msg, Query, Response};
use routing::{RoutingTable, K};

/// Well-known routers used to join the network.
pub const BOOTSTRAP_NODES: &[&str] = &[
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.utorrent.com:6881",
    "dht.libtorrent.org:25401",
];

/// Queries in flight per lookup. Kademlia's paper uses 3; many DHT nodes
/// never answer (they are behind NAT or gone), so more in flight hides the
/// timeouts.
const ALPHA: usize = 8;
/// Candidates remembered per lookup.
const SHORTLIST: usize = 64;
/// Peers returned for one info hash in a `get_peers` answer.
const MAX_VALUES: usize = 50;
/// Limits on what other nodes can make us store.
const MAX_STORED_HASHES: usize = 2000;
const MAX_STORED_PEERS: usize = 200;
const PEER_TTL: Duration = Duration::from_secs(30 * 60);
const SECRET_ROTATION: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct NodeId(pub [u8; 20]);

/// XOR distance between two IDs, compared as a 160-bit number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Distance(pub [u8; 20]);

impl NodeId {
    pub fn random() -> NodeId {
        NodeId(rand::random())
    }

    pub fn distance(&self, other: &NodeId) -> Distance {
        let mut d = [0u8; 20];
        for (i, b) in d.iter_mut().enumerate() {
            *b = self.0[i] ^ other.0[i];
        }
        Distance(d)
    }
}

impl Distance {
    pub fn leading_zeros(&self) -> u32 {
        let mut n = 0;
        for b in self.0 {
            if b == 0 {
                n += 8;
            } else {
                return n + b.leading_zeros();
            }
        }
        n
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DhtError {
    #[error("no answer")]
    Timeout,
    #[error("node answered with error {0}: {1}")]
    Remote(i64, String),
    #[error("network error: {0}")]
    Io(String),
}

#[derive(Debug, Clone)]
pub struct DhtConfig {
    /// UDP port to listen on; 0 picks any. If taken, any free port is used.
    pub port: u16,
    /// `host:port` entries used to join the network.
    pub bootstrap: Vec<String>,
    pub query_timeout: Duration,
    /// How often the maintenance loop checks the routing table.
    pub maintenance: Duration,
}

impl Default for DhtConfig {
    fn default() -> Self {
        DhtConfig {
            port: 0,
            bootstrap: BOOTSTRAP_NODES.iter().map(|s| s.to_string()).collect(),
            query_timeout: Duration::from_millis(1500),
            maintenance: Duration::from_secs(60),
        }
    }
}

/// Peers other nodes announced to us, by info hash, with when.
type PeerStore = HashMap<[u8; 20], Vec<(SocketAddrV4, Instant)>>;

type Pending = HashMap<Vec<u8>, (SocketAddrV4, oneshot::Sender<Result<Response, DhtError>>)>;

struct Secrets {
    current: [u8; 16],
    previous: [u8; 16],
    rotated: Instant,
}

struct Inner {
    socket: UdpSocket,
    own: NodeId,
    cfg: DhtConfig,
    table: Mutex<RoutingTable>,
    pending: Mutex<Pending>,
    next_tid: AtomicU16,
    stored: Mutex<PeerStore>,
    secrets: Mutex<Secrets>,
}

/// A handle to a running DHT node. Cheap to clone.
#[derive(Clone)]
pub struct Dht {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Dht {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dht")
            .field("addr", &self.local_addr().ok())
            .field("nodes", &self.node_count())
            .finish()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a lookup found.
#[derive(Debug, Default)]
struct LookupResult {
    peers: Vec<SocketAddrV4>,
    /// The closest nodes that answered, with the token each gave.
    closest: Vec<(NodeId, SocketAddrV4, Option<Vec<u8>>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    New,
    InFlight,
    Answered(Option<Vec<u8>>),
    Failed,
}

impl Dht {
    /// Binds the socket and starts the receive and maintenance tasks, which
    /// run until `token` is cancelled. Joining the network happens in the
    /// background.
    pub async fn start(cfg: DhtConfig, token: CancellationToken) -> std::io::Result<Dht> {
        let socket = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, cfg.port)).await {
            Ok(s) => s,
            Err(_) if cfg.port != 0 => UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?,
            Err(e) => return Err(e),
        };
        let own = NodeId::random();
        let dht = Dht {
            inner: Arc::new(Inner {
                socket,
                own,
                cfg,
                table: Mutex::new(RoutingTable::new(own)),
                pending: Mutex::new(HashMap::new()),
                next_tid: AtomicU16::new(rand::random()),
                stored: Mutex::new(HashMap::new()),
                secrets: Mutex::new(Secrets {
                    current: rand::random(),
                    previous: rand::random(),
                    rotated: Instant::now(),
                }),
            }),
        };
        tokio::spawn(dht.clone().receive_loop(token.clone()));
        tokio::spawn(dht.clone().maintenance_loop(token));
        Ok(dht)
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    pub fn id(&self) -> NodeId {
        self.inner.own
    }

    pub fn node_count(&self) -> usize {
        lock(&self.inner.table).len()
    }

    /// Joins the network: asks the bootstrap nodes for nodes near our own
    /// ID, then looks our ID up. Returns the routing table size afterwards.
    pub async fn bootstrap(&self) -> usize {
        let mut addrs = Vec::new();
        for host in &self.inner.cfg.bootstrap {
            match tokio::net::lookup_host(host.as_str()).await {
                Ok(it) => addrs.extend(it.filter_map(krpc::v4)),
                Err(e) => debug!("dht: cannot resolve {host}: {e}"),
            }
        }
        let own = self.inner.own;
        let mut asks: FuturesUnordered<_> = addrs
            .into_iter()
            .map(|a| self.query(a, Query::FindNode { target: own }))
            .collect();
        let mut seeds = Vec::new();
        while let Some(r) = asks.next().await {
            if let Ok(r) = r {
                seeds.extend(r.nodes);
            }
        }
        self.lookup(own, None, seeds).await;
        let n = self.node_count();
        info!("dht: routing table has {n} nodes");
        n
    }

    /// Finds peers for `info_hash`. With `announce_port`, also tells the
    /// closest nodes that we are a peer on that TCP port.
    pub async fn get_peers(
        &self,
        info_hash: [u8; 20],
        announce_port: Option<u16>,
    ) -> Vec<SocketAddr> {
        if self.node_count() == 0 {
            self.bootstrap().await;
        }
        let r = self
            .lookup(NodeId(info_hash), Some(info_hash), Vec::new())
            .await;
        if let Some(port) = announce_port {
            let mut acks: FuturesUnordered<_> = r
                .closest
                .iter()
                .filter_map(|(_, addr, tok)| {
                    tok.clone().map(|token| {
                        self.query(
                            *addr,
                            Query::AnnouncePeer {
                                info_hash,
                                port,
                                implied_port: false,
                                token,
                            },
                        )
                    })
                })
                .collect();
            let mut ok = 0;
            while let Some(a) = acks.next().await {
                ok += a.is_ok() as usize;
            }
            debug!("dht: announced to {ok} nodes");
        }
        let mut seen = HashSet::new();
        r.peers
            .into_iter()
            .filter(|p| seen.insert(*p))
            .map(SocketAddr::V4)
            .collect()
    }

    /// Sends one query and waits for its answer.
    async fn query(&self, addr: SocketAddrV4, query: Query) -> Result<Response, DhtError> {
        let tid = self
            .inner
            .next_tid
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes()
            .to_vec();
        let (tx, rx) = oneshot::channel();
        lock(&self.inner.pending).insert(tid.clone(), (addr, tx));
        let msg = krpc::encode(&Msg {
            tid: tid.clone(),
            body: Body::Query {
                id: self.inner.own,
                query,
            },
        });
        if let Err(e) = self.inner.socket.send_to(&msg, addr).await {
            lock(&self.inner.pending).remove(&tid);
            return Err(DhtError::Io(e.to_string()));
        }
        match timeout(self.inner.cfg.query_timeout, rx).await {
            Ok(Ok(r)) => r,
            _ => {
                lock(&self.inner.pending).remove(&tid);
                Err(DhtError::Timeout)
            }
        }
    }

    /// Kademlia's iterative lookup for `target`. With `info_hash` it sends
    /// `get_peers` (collecting peers and tokens), otherwise `find_node`.
    async fn lookup(
        &self,
        target: NodeId,
        info_hash: Option<[u8; 20]>,
        extra: Vec<(NodeId, SocketAddrV4)>,
    ) -> LookupResult {
        let own = self.inner.own;
        let mut list: BTreeMap<Distance, (NodeId, SocketAddrV4, State)> = BTreeMap::new();
        let add = |list: &mut BTreeMap<Distance, (NodeId, SocketAddrV4, State)>,
                   id: NodeId,
                   addr: SocketAddrV4| {
            if id != own && addr.port() != 0 {
                list.entry(target.distance(&id))
                    .or_insert((id, addr, State::New));
                while list.len() > SHORTLIST {
                    list.pop_last();
                }
            }
        };
        let seeds = lock(&self.inner.table).closest(&target, K);
        for (id, addr) in seeds.into_iter().chain(extra) {
            add(&mut list, id, addr);
        }

        let mut peers = Vec::new();
        let mut in_flight = FuturesUnordered::new();
        loop {
            // Start queries to the closest candidates not yet asked, among
            // the K closest that have not failed.
            let mut considered = 0;
            for (d, (id, addr, state)) in list.iter_mut() {
                if considered >= K || in_flight.len() >= ALPHA {
                    break;
                }
                if *state == State::Failed {
                    continue;
                }
                considered += 1;
                if *state == State::New {
                    *state = State::InFlight;
                    let q = match info_hash {
                        Some(h) => Query::GetPeers { info_hash: h },
                        None => Query::FindNode { target },
                    };
                    let (d, id, addr) = (*d, *id, *addr);
                    let this = self.clone();
                    in_flight.push(async move { (d, id, this.query(addr, q).await) });
                }
            }
            let Some((d, id, result)) = in_flight.next().await else {
                break;
            };
            match result {
                Ok(r) => {
                    if let Some(e) = list.get_mut(&d) {
                        e.2 = State::Answered(r.token.clone());
                    }
                    peers.extend(r.values);
                    for (nid, naddr) in r.nodes {
                        add(&mut list, nid, naddr);
                    }
                }
                Err(e) => {
                    trace!("dht: query to {id:?} failed: {e}");
                    if let Some(e) = list.get_mut(&d) {
                        e.2 = State::Failed;
                    }
                    lock(&self.inner.table).failed(&id);
                }
            }
        }
        let closest = list
            .into_values()
            .filter_map(|(id, addr, s)| match s {
                State::Answered(tok) => Some((id, addr, tok)),
                _ => None,
            })
            .take(K)
            .collect();
        LookupResult { peers, closest }
    }

    async fn receive_loop(self, token: CancellationToken) {
        let mut buf = vec![0u8; 65536];
        loop {
            let (n, from) = tokio::select! {
                _ = token.cancelled() => break,
                r = self.inner.socket.recv_from(&mut buf) => match r {
                    Ok(v) => v,
                    // Windows reports ICMP "port unreachable" from an
                    // earlier send as a receive error; ignore it.
                    Err(_) => continue,
                },
            };
            let Some(from) = krpc::v4(from) else { continue };
            let Ok(msg) = krpc::parse(&buf[..n]) else {
                trace!("dht: unparseable datagram from {from}");
                continue;
            };
            match msg.body {
                Body::Query { id, query } => {
                    lock(&self.inner.table).heard_from(id, from, Instant::now());
                    let body = self.answer(from, query);
                    let out = krpc::encode(&Msg { tid: msg.tid, body });
                    let _ = self.inner.socket.send_to(&out, from).await;
                }
                Body::Response(r) => {
                    let waiter = {
                        let mut p = lock(&self.inner.pending);
                        // Only the node we asked may answer.
                        match p.get(&msg.tid) {
                            Some((addr, _)) if *addr == from => p.remove(&msg.tid),
                            _ => None,
                        }
                    };
                    if let Some((_, tx)) = waiter {
                        lock(&self.inner.table).heard_from(r.id, from, Instant::now());
                        let _ = tx.send(Ok(r));
                    }
                }
                Body::Error { code, message } => {
                    let waiter = {
                        let mut p = lock(&self.inner.pending);
                        match p.get(&msg.tid) {
                            Some((addr, _)) if *addr == from => p.remove(&msg.tid),
                            _ => None,
                        }
                    };
                    if let Some((_, tx)) = waiter {
                        let _ = tx.send(Err(DhtError::Remote(code, message)));
                    }
                }
            }
        }
    }

    fn token_for(&self, ip: Ipv4Addr, secret: &[u8; 16]) -> Vec<u8> {
        let mut h = Sha1::new();
        h.update(secret);
        h.update(ip.octets());
        h.finalize()[..8].to_vec()
    }

    fn answer(&self, from: SocketAddrV4, query: Query) -> Body {
        let own = self.inner.own;
        let closest = |t: &NodeId| lock(&self.inner.table).closest(t, K);
        match query {
            Query::Ping => Body::Response(Response {
                id: own,
                ..Response::default()
            }),
            Query::FindNode { target } => Body::Response(Response {
                id: own,
                nodes: closest(&target),
                ..Response::default()
            }),
            Query::GetPeers { info_hash } => {
                let token = {
                    let s = lock(&self.inner.secrets);
                    self.token_for(*from.ip(), &s.current)
                };
                let now = Instant::now();
                let values: Vec<SocketAddrV4> = lock(&self.inner.stored)
                    .get(&info_hash)
                    .map(|v| {
                        v.iter()
                            .filter(|(_, at)| now.duration_since(*at) < PEER_TTL)
                            .map(|(p, _)| *p)
                            .take(MAX_VALUES)
                            .collect()
                    })
                    .unwrap_or_default();
                let nodes = if values.is_empty() {
                    closest(&NodeId(info_hash))
                } else {
                    Vec::new()
                };
                Body::Response(Response {
                    id: own,
                    nodes,
                    values,
                    token: Some(token),
                })
            }
            Query::AnnouncePeer {
                info_hash,
                port,
                implied_port,
                token,
            } => {
                let valid = {
                    let s = lock(&self.inner.secrets);
                    token == self.token_for(*from.ip(), &s.current)
                        || token == self.token_for(*from.ip(), &s.previous)
                };
                if !valid {
                    return Body::Error {
                        code: krpc::ERROR_PROTOCOL,
                        message: "bad token".to_string(),
                    };
                }
                let port = if implied_port { from.port() } else { port };
                if port == 0 {
                    return Body::Error {
                        code: krpc::ERROR_PROTOCOL,
                        message: "bad port".to_string(),
                    };
                }
                let peer = SocketAddrV4::new(*from.ip(), port);
                let mut stored = lock(&self.inner.stored);
                if stored.len() < MAX_STORED_HASHES || stored.contains_key(&info_hash) {
                    let list = stored.entry(info_hash).or_default();
                    list.retain(|(p, _)| *p != peer);
                    if list.len() >= MAX_STORED_PEERS {
                        list.remove(0);
                    }
                    list.push((peer, Instant::now()));
                }
                Body::Response(Response {
                    id: own,
                    ..Response::default()
                })
            }
        }
    }

    async fn maintenance_loop(self, token: CancellationToken) {
        let mut last_refresh = Instant::now();
        loop {
            if self.node_count() < K {
                self.bootstrap().await;
                last_refresh = Instant::now();
            } else if last_refresh.elapsed() > Duration::from_secs(15 * 60) {
                self.lookup(self.inner.own, None, Vec::new()).await;
                last_refresh = Instant::now();
            }
            {
                let mut s = lock(&self.inner.secrets);
                if s.rotated.elapsed() > SECRET_ROTATION {
                    s.previous = s.current;
                    s.current = rand::random();
                    s.rotated = Instant::now();
                }
            }
            {
                let now = Instant::now();
                let mut stored = lock(&self.inner.stored);
                for list in stored.values_mut() {
                    list.retain(|(_, at)| now.duration_since(*at) < PEER_TTL);
                }
                stored.retain(|_, l| !l.is_empty());
            }
            tokio::select! {
                _ = token.cancelled() => break,
                _ = sleep(self.inner.cfg.maintenance) => {}
            }
        }
    }
}

/// Looks up peers for `info_hash` now and then, announcing our TCP `port`,
/// and sends what it finds to the session as [`SessionEvent::PeersFound`].
/// Searches every `first` interval three times (while the swarm is being
/// discovered), then every `steady` interval.
pub async fn run_search(
    dht: Dht,
    info_hash: [u8; 20],
    port: Option<u16>,
    events: mpsc::Sender<SessionEvent>,
    token: CancellationToken,
    first: Duration,
    steady: Duration,
) {
    let mut round = 0u32;
    loop {
        let peers = tokio::select! {
            _ = token.cancelled() => return,
            p = dht.get_peers(info_hash, port) => p,
        };
        debug!("dht: found {} peers", peers.len());
        if !peers.is_empty() && events.send(SessionEvent::PeersFound(peers)).await.is_err() {
            return;
        }
        round += 1;
        let wait = if round < 3 { first } else { steady };
        tokio::select! {
            _ = token.cancelled() => return,
            _ = sleep(wait) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_and_leading_zeros() {
        let a = NodeId([0; 20]);
        let mut b = [0u8; 20];
        b[2] = 0x10;
        let d = a.distance(&NodeId(b));
        assert_eq!(d.leading_zeros(), 19);
        assert_eq!(a.distance(&a).leading_zeros(), 160);
        assert!(a.distance(&NodeId([1; 20])) < a.distance(&NodeId([2; 20])));
    }

    fn local_cfg(bootstrap: Vec<String>) -> DhtConfig {
        DhtConfig {
            port: 0,
            bootstrap,
            query_timeout: Duration::from_millis(500),
            maintenance: Duration::from_millis(300),
        }
    }

    /// A small DHT on localhost: one node announces, another finds it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn nodes_find_an_announced_peer() {
        let token = CancellationToken::new();
        let first = Dht::start(local_cfg(Vec::new()), token.clone())
            .await
            .unwrap();
        let seed = format!("127.0.0.1:{}", first.local_addr().unwrap().port());
        let mut nodes = vec![first];
        for _ in 0..11 {
            nodes.push(
                Dht::start(local_cfg(vec![seed.clone()]), token.clone())
                    .await
                    .unwrap(),
            );
        }
        for n in &nodes[1..] {
            n.bootstrap().await;
        }
        for n in &nodes {
            assert!(
                n.node_count() >= 3,
                "node knows only {} others",
                n.node_count()
            );
        }

        let hash = [0x42; 20];
        let found = nodes[3].get_peers(hash, Some(7000)).await;
        assert!(found.is_empty());
        let found = nodes[9].get_peers(hash, None).await;
        assert_eq!(found, vec!["127.0.0.1:7000".parse::<SocketAddr>().unwrap()]);
        token.cancel();
    }

    #[tokio::test]
    async fn announce_needs_a_valid_token() {
        let token = CancellationToken::new();
        let node = Dht::start(local_cfg(Vec::new()), token.clone())
            .await
            .unwrap();
        let client = Dht::start(local_cfg(Vec::new()), token.clone())
            .await
            .unwrap();
        let addr = krpc::v4(node.local_addr().unwrap()).unwrap();
        let target = SocketAddrV4::new(Ipv4Addr::LOCALHOST, addr.port());
        let bad = client
            .query(
                target,
                Query::AnnouncePeer {
                    info_hash: [1; 20],
                    port: 9,
                    implied_port: false,
                    token: b"forged".to_vec(),
                },
            )
            .await;
        assert!(matches!(bad, Err(DhtError::Remote(203, _))));

        let r = client
            .query(target, Query::GetPeers { info_hash: [1; 20] })
            .await
            .unwrap();
        let ok = client
            .query(
                target,
                Query::AnnouncePeer {
                    info_hash: [1; 20],
                    port: 9,
                    implied_port: false,
                    token: r.token.unwrap(),
                },
            )
            .await;
        assert!(ok.is_ok());
        let r = client
            .query(target, Query::GetPeers { info_hash: [1; 20] })
            .await
            .unwrap();
        assert_eq!(r.values, vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9)]);
        // The node also learned about the client from its queries.
        assert_eq!(node.node_count(), 1);
        token.cancel();
    }

    #[tokio::test]
    async fn silent_node_times_out() {
        let token = CancellationToken::new();
        let client = Dht::start(local_cfg(Vec::new()), token.clone())
            .await
            .unwrap();
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = krpc::v4(silent.local_addr().unwrap()).unwrap();
        assert_eq!(
            client.query(addr, Query::Ping).await,
            Err(DhtError::Timeout)
        );
        token.cancel();
    }
}
