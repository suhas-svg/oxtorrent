//! The coordinator. One task owns all download state (piece picker, peer
//! table, choke state). Every other task talks to it through channels, so
//! no locks guard the shared state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::bitfield::Bitfield;
use crate::choke::{ChokeStrategy, PeerStats, TitForTat};
use crate::dht::Dht;
use crate::disk::{self, DiskCommand, DiskError, Storage, VerifyResult};
use crate::metainfo::Torrent;
use crate::peer::connection::{self, PeerCommand, PeerParams, PeerTarget, Timeouts};
use crate::peer::handshake::Handshake;
use crate::peer::message::Message;
use crate::ratelimit::Limits;
use crate::picker::{BlockOutcome, Picker, PieceStatus, RarestFirst};
use crate::tracker::{self, TrackerConfig};
use crate::PeerKey;

/// Failed pieces a peer may be involved in before it is banned.
const MAX_STRIKES: u32 = 3;
/// Delay before re-dialling a peer whose connection ended.
const RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_ATTEMPTS: u32 = 5;
const CHOKE_INTERVAL: Duration = Duration::from_secs(10);
const PEER_QUEUE: usize = 512;
/// Seconds of a peer's measured download rate to keep requested ahead.
const REQUEST_QUEUE_SECS: f64 = 3.0;
/// Most block requests outstanding to one peer. Many clients drop
/// requests beyond a few hundred, so stay well under that.
const MAX_QUEUE_DEPTH: usize = 128;
/// Weight of the old value in the per-second rate average.
const RATE_SMOOTHING: f64 = 0.6;
/// Memory allowed for buffered pieces in progress.
const MAX_BUFFERED_BYTES: u64 = 64 * 1024 * 1024;

/// Requests to keep outstanding for a peer: enough to cover
/// [`REQUEST_QUEUE_SECS`] of its measured rate, never fewer than `min`
/// (the `--pipeline` option) and never more than [`MAX_QUEUE_DEPTH`].
pub fn queue_depth(rate: f64, min: usize) -> usize {
    let want = (rate.max(0.0) * REQUEST_QUEUE_SECS / crate::BLOCK_SIZE as f64).ceil() as usize;
    want.clamp(min, MAX_QUEUE_DEPTH.max(min))
}

/// Pieces that may be in progress at once: as many as fit in
/// [`MAX_BUFFERED_BYTES`], and at least `min`.
pub fn max_in_progress(min: usize, piece_length: u64) -> usize {
    ((MAX_BUFFERED_BYTES / piece_length.max(1)) as usize).max(min)
}
/// DHT peer searches: three this far apart at the start, then this often.
const DHT_FIRST_INTERVAL: Duration = Duration::from_secs(60);
const DHT_INTERVAL: Duration = Duration::from_secs(5 * 60);
const EVENT_QUEUE: usize = 1024;
const DISK_QUEUE: usize = 256;

#[derive(Debug, Clone)]
pub struct Config {
    pub output_dir: PathBuf,
    /// The port reported to the tracker (the listener's real port).
    pub port: u16,
    pub max_peers: usize,
    pub seed: bool,
    pub pipeline: usize,
    pub peer_id: [u8; 20],
    pub max_in_progress: usize,
    pub timeouts: Timeouts,
    /// Peers to dial in addition to those the tracker returns.
    pub extra_peers: Vec<SocketAddr>,
    /// DHT node to search for peers on (ignored for private torrents).
    pub dht: Option<Dht>,
    /// Speed limits, shared with the other torrents in the run.
    pub limits: Limits,
}

impl Config {
    pub fn new(output_dir: PathBuf, peer_id: [u8; 20]) -> Self {
        Config {
            output_dir,
            port: 6881,
            max_peers: 50,
            seed: false,
            pipeline: 5,
            peer_id,
            max_in_progress: 16,
            timeouts: Timeouts::default(),
            extra_peers: Vec::new(),
            dht: None,
            limits: Limits::default(),
        }
    }
}

/// Snapshot published once a second for the progress line and the tracker.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Progress {
    /// The torrent's name (empty until the session starts).
    pub name: String,
    pub total: u64,
    pub verified_bytes: u64,
    pub pieces_done: usize,
    pub piece_count: usize,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_rate: f64,
    pub upload_rate: f64,
    pub peers: usize,
    pub complete: bool,
    /// One row per connected peer, for the dashboard.
    pub peer_rows: Vec<PeerRow>,
    /// State of every piece, for the dashboard's piece map.
    pub pieces: Vec<PieceStatus>,
}

impl Progress {
    /// Totals over several downloads: sizes, rates and peers summed, peer
    /// rows joined, complete only when all are. The piece map is left
    /// empty, since piece numbers differ between torrents.
    pub fn combine(items: &[Progress]) -> Progress {
        let mut all = Progress {
            name: format!("{} torrents", items.len()),
            complete: !items.is_empty(),
            ..Progress::default()
        };
        for p in items {
            all.total += p.total;
            all.verified_bytes += p.verified_bytes;
            all.pieces_done += p.pieces_done;
            all.piece_count += p.piece_count;
            all.downloaded += p.downloaded;
            all.uploaded += p.uploaded;
            all.download_rate += p.download_rate;
            all.upload_rate += p.upload_rate;
            all.peers += p.peers;
            all.complete &= p.complete;
            all.peer_rows.extend(p.peer_rows.iter().cloned());
        }
        all
    }
}

/// A connected peer as the dashboard shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerRow {
    pub addr: SocketAddr,
    /// Client name and version guessed from the peer ID.
    pub client: String,
    pub down_rate: f64,
    pub up_rate: f64,
    /// The peer is choking us / we are interested in it.
    pub peer_choking: bool,
    pub am_interested: bool,
    /// We are choking the peer / it is interested in us.
    pub am_choking: bool,
    pub peer_interested: bool,
    /// Block requests we have outstanding to it.
    pub requests: usize,
    /// Pieces it has.
    pub pieces: usize,
}

#[derive(Debug, Clone, Default)]
pub struct SessionStats {
    pub banned: Vec<SocketAddr>,
    pub failed_pieces: u32,
    pub peers_connected: usize,
    pub downloaded: u64,
    pub uploaded: u64,
    pub resumed_pieces: usize,
    /// Where downloaded bytes beyond the file size went.
    pub waste: Waste,
}

/// Bytes received that did not end up in the file, and the request
/// events that cause them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Waste {
    /// A block that had already arrived from another peer.
    pub duplicate_bytes: u64,
    /// A block of a piece no longer in progress (already verified), or of
    /// the wrong size.
    pub unexpected_bytes: u64,
    /// Blocks of pieces that failed their hash check.
    pub failed_bytes: u64,
    /// Requests freed because the peer choked us.
    pub released_on_choke: u64,
    /// Requests freed because the peer disconnected.
    pub released_on_drop: u64,
    /// Requests freed because they timed out.
    pub expired: u64,
    /// Duplicate requests sent in endgame.
    pub endgame_requests: u64,
}

#[derive(Debug, Clone)]
pub struct Outcome {
    /// Every piece is present and verified.
    pub complete: bool,
    pub stats: SessionStats,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("disk error: {0}")]
    Disk(#[from] DiskError),
    #[error("background task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Everything that reaches the session from other tasks.
pub enum SessionEvent {
    PeersFound(Vec<SocketAddr>),
    /// An incoming connection, with its handshake if the shared listener
    /// already read it.
    Incoming(TcpStream, SocketAddr, Option<Handshake>),
    PeerConnected {
        key: PeerKey,
        peer_id: [u8; 20],
    },
    FromPeer {
        key: PeerKey,
        msg: Message,
    },
    BlockSent {
        key: PeerKey,
        bytes: u64,
    },
    PeerDisconnected {
        key: PeerKey,
        reason: String,
    },
    PieceChecked {
        index: u32,
        result: Result<VerifyResult, DiskError>,
    },
}

struct Peer {
    addr: SocketAddr,
    tx: mpsc::Sender<PeerCommand>,
    outgoing: bool,
    connected: bool,
    bitfield: Bitfield,
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    choked_since: Option<Instant>,
    /// Bytes in the current and previous 10-second choke windows.
    dl_cur: u64,
    dl_prev: u64,
    ul_cur: u64,
    ul_prev: u64,
    /// Bytes received since the last one-second tick, and a smoothed
    /// bytes-per-second estimate that sizes the request queue.
    dl_second: u64,
    dl_rate: f64,
    ul_second: u64,
    ul_rate: f64,
}

impl Peer {
    fn new(addr: SocketAddr, tx: mpsc::Sender<PeerCommand>, outgoing: bool, pieces: usize) -> Self {
        Peer {
            addr,
            tx,
            outgoing,
            connected: false,
            bitfield: Bitfield::new(pieces),
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            choked_since: None,
            dl_cur: 0,
            dl_prev: 0,
            ul_cur: 0,
            ul_prev: 0,
            dl_second: 0,
            dl_rate: 0.0,
            ul_second: 0,
            ul_rate: 0.0,
        }
    }
}

/// Starts every task for one download and runs until it completes (and,
/// with `seed`, until `token` is cancelled) or `token` is cancelled.
pub async fn run_download(
    torrent: Torrent,
    config: Config,
    inbound: impl Into<Inbound>,
    progress_tx: watch::Sender<Progress>,
    token: CancellationToken,
) -> Result<Outcome, SessionError> {
    let inbound = inbound.into();
    let torrent = Arc::new(torrent);

    // Open files and re-hash whatever is already there.
    let t = torrent.clone();
    let dir = config.output_dir.clone();
    let (storage, have) = tokio::task::spawn_blocking(move || -> Result<_, DiskError> {
        let mut storage = Storage::create(&dir, &t)?;
        let have = if storage.existed {
            storage.verify_existing(&t)?
        } else {
            Bitfield::new(t.piece_count())
        };
        Ok((storage, have))
    })
    .await??;
    if have.count_ones() > 0 {
        info!(
            "resuming: {} of {} pieces already verified",
            have.count_ones(),
            torrent.piece_count()
        );
    }

    let (disk_tx, disk_rx) = mpsc::channel(DISK_QUEUE);
    let disk_task = disk::spawn(storage, torrent.clone(), disk_rx);
    let (events_tx, mut events_rx) = mpsc::channel(EVENT_QUEUE);
    let inner = token.child_token();

    let mut session = Session::new(
        torrent.clone(),
        config.clone(),
        have,
        disk_tx,
        events_tx.clone(),
        progress_tx,
    );
    session.publish_progress();

    let mut tracker_task = None;
    let urls = torrent.trackers();
    if !urls.is_empty() {
        let cfg = TrackerConfig {
            urls,
            info_hash: torrent.info_hash,
            peer_id: config.peer_id,
            port: config.port,
            udp_retransmit: Default::default(),
        };
        tracker_task = Some(tokio::spawn(tracker::run_tracker(
            cfg,
            session.progress_tx.subscribe(),
            events_tx.clone(),
            inner.clone(),
        )));
    }
    if let Some(dht) = config.dht.clone().filter(|_| !torrent.private) {
        tokio::spawn(crate::dht::run_search(
            dht,
            torrent.info_hash,
            Some(config.port),
            events_tx.clone(),
            inner.clone(),
            DHT_FIRST_INTERVAL,
            DHT_INTERVAL,
        ));
    }
    match inbound {
        Inbound::None => {}
        Inbound::Listener(l) => {
            tokio::spawn(listen(l, events_tx.clone(), inner.clone()));
        }
        Inbound::Routed(rx) => {
            tokio::spawn(forward_routed(rx, events_tx.clone(), inner.clone()));
        }
    }
    if !config.extra_peers.is_empty() {
        session.add_candidates(config.extra_peers.clone());
    }
    drop(events_tx);

    let result = session.run(&mut events_rx, &token).await;
    let flush = session.flush().await;
    let stats = session.stats();
    let w = &stats.waste;
    info!(
        "received {} bytes: {} duplicate, {} unexpected, {} in failed pieces; \
         requests freed: {} on choke, {} on disconnect, {} timed out; {} endgame requests",
        stats.downloaded,
        w.duplicate_bytes,
        w.unexpected_bytes,
        w.failed_bytes,
        w.released_on_choke,
        w.released_on_drop,
        w.expired,
        w.endgame_requests
    );
    let progress_tx = session.shutdown();

    inner.cancel();
    if let Some(t) = tracker_task {
        let _ = timeout(Duration::from_secs(2), t).await;
    }
    let _ = timeout(Duration::from_secs(5), disk_task).await;
    progress_tx.send_modify(|p| p.peers = 0);

    let complete = result?;
    flush?;
    Ok(Outcome { complete, stats })
}

/// Where a download's incoming connections come from.
pub enum Inbound {
    /// None: outgoing connections only.
    None,
    /// The download has the listening socket to itself.
    Listener(TcpListener),
    /// A shared [`crate::router::Router`] hands over connections for this
    /// torrent, with their handshake already read.
    Routed(mpsc::Receiver<crate::router::RoutedPeer>),
}

impl From<Option<TcpListener>> for Inbound {
    fn from(l: Option<TcpListener>) -> Self {
        l.map_or(Inbound::None, Inbound::Listener)
    }
}

impl From<TcpListener> for Inbound {
    fn from(l: TcpListener) -> Self {
        Inbound::Listener(l)
    }
}

async fn forward_routed(
    mut rx: mpsc::Receiver<crate::router::RoutedPeer>,
    events: mpsc::Sender<SessionEvent>,
    token: CancellationToken,
) {
    loop {
        let peer = tokio::select! {
            _ = token.cancelled() => break,
            p = rx.recv() => match p { Some(p) => p, None => break },
        };
        let ev = SessionEvent::Incoming(peer.stream, peer.addr, Some(peer.handshake));
        if events.send(ev).await.is_err() {
            break;
        }
    }
}

/// Accepts incoming connections and hands them to the session, which
/// decides whether to keep them.
pub async fn listen(
    listener: TcpListener,
    events: mpsc::Sender<SessionEvent>,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            r = listener.accept() => match r {
                Ok((stream, addr)) => {
                    if events.send(SessionEvent::Incoming(stream, addr, None)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

struct Session {
    torrent: Arc<Torrent>,
    config: Config,
    picker: Picker,
    choker: Box<dyn ChokeStrategy>,
    peers: HashMap<PeerKey, Peer>,
    next_key: PeerKey,
    candidates: VecDeque<SocketAddr>,
    known: HashSet<SocketAddr>,
    retry: Vec<(Instant, SocketAddr)>,
    attempts: HashMap<SocketAddr, u32>,
    banned: HashSet<SocketAddr>,
    banned_ids: HashSet<[u8; 20]>,
    strikes: HashMap<SocketAddr, u32>,
    peer_ids: HashMap<PeerKey, [u8; 20]>,
    disk: mpsc::Sender<DiskCommand>,
    events_tx: mpsc::Sender<SessionEvent>,
    progress_tx: watch::Sender<Progress>,
    complete: bool,
    downloaded: u64,
    uploaded: u64,
    rate_window: VecDeque<(u64, u64)>,
    last_totals: (u64, u64),
    stats: SessionStats,
}

impl Session {
    fn new(
        torrent: Arc<Torrent>,
        config: Config,
        have: Bitfield,
        disk: mpsc::Sender<DiskCommand>,
        events_tx: mpsc::Sender<SessionEvent>,
        progress_tx: watch::Sender<Progress>,
    ) -> Self {
        let resumed = have.count_ones();
        let picker = Picker::new(
            torrent.total_length,
            torrent.piece_length,
            &have,
            max_in_progress(config.max_in_progress, torrent.piece_length),
            Box::new(RarestFirst::from_entropy()),
        );
        Session {
            torrent,
            config,
            picker,
            choker: Box::new(TitForTat::from_entropy(4)),
            peers: HashMap::new(),
            next_key: 1,
            candidates: VecDeque::new(),
            known: HashSet::new(),
            retry: Vec::new(),
            attempts: HashMap::new(),
            banned: HashSet::new(),
            banned_ids: HashSet::new(),
            strikes: HashMap::new(),
            peer_ids: HashMap::new(),
            disk,
            events_tx,
            progress_tx,
            complete: false,
            downloaded: 0,
            uploaded: 0,
            rate_window: VecDeque::new(),
            last_totals: (0, 0),
            stats: SessionStats {
                resumed_pieces: resumed,
                ..SessionStats::default()
            },
        }
    }

    /// Returns whether the download is complete when the loop ends.
    async fn run(
        &mut self,
        events: &mut mpsc::Receiver<SessionEvent>,
        token: &CancellationToken,
    ) -> Result<bool, SessionError> {
        if self.picker.is_complete() {
            self.on_complete();
            if !self.config.seed {
                return Ok(true);
            }
        }
        let mut tick = interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut choke_tick = interval(CHOKE_INTERVAL);
        choke_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = token.cancelled() => return Ok(self.complete),
                ev = events.recv() => {
                    let Some(ev) = ev else { return Ok(self.complete) };
                    if self.handle(ev).await? {
                        return Ok(true);
                    }
                }
                _ = tick.tick() => self.on_tick(),
                _ = choke_tick.tick() => self.run_choke(),
            }
        }
    }

    /// Handles one event. Returns true when the session should stop
    /// because the download finished and we are not seeding.
    async fn handle(&mut self, ev: SessionEvent) -> Result<bool, SessionError> {
        match ev {
            SessionEvent::PeersFound(addrs) => {
                self.add_candidates(addrs);
                self.connect_more();
            }
            SessionEvent::Incoming(stream, addr, handshake) => {
                if self.peers.len() >= self.config.max_peers || self.banned.contains(&addr) {
                    debug!(peer = %addr, "refusing incoming connection");
                } else {
                    let target = match handshake {
                        Some(hs) => PeerTarget::Routed(stream, hs),
                        None => PeerTarget::Incoming(stream),
                    };
                    self.spawn_peer(target, addr, false);
                }
            }
            SessionEvent::PeerConnected { key, peer_id } => self.on_connected(key, peer_id),
            SessionEvent::FromPeer { key, msg } => self.on_message(key, msg).await?,
            SessionEvent::BlockSent { key, bytes } => {
                self.uploaded += bytes;
                if let Some(p) = self.peers.get_mut(&key) {
                    p.ul_cur += bytes;
                    p.ul_second += bytes;
                }
            }
            SessionEvent::PeerDisconnected { key, reason } => {
                if self.peers.contains_key(&key) {
                    self.drop_peer(key, &reason);
                    self.fill_all();
                }
            }
            SessionEvent::PieceChecked { index, result } => {
                return self.on_piece_checked(index, result?);
            }
        }
        Ok(false)
    }

    fn add_candidates(&mut self, addrs: Vec<SocketAddr>) {
        for a in addrs {
            if !self.banned.contains(&a) && self.known.insert(a) {
                self.candidates.push_back(a);
            }
        }
    }

    fn connect_more(&mut self) {
        while self.peers.len() < self.config.max_peers {
            let Some(addr) = self.candidates.pop_front() else {
                break;
            };
            if self.banned.contains(&addr) {
                continue;
            }
            *self.attempts.entry(addr).or_insert(0) += 1;
            self.spawn_peer(PeerTarget::Outgoing(addr), addr, true);
        }
    }

    fn spawn_peer(&mut self, target: PeerTarget, addr: SocketAddr, outgoing: bool) {
        let key = self.next_key;
        self.next_key += 1;
        let (tx, rx) = mpsc::channel(PEER_QUEUE);
        let params = PeerParams {
            key,
            addr,
            info_hash: self.torrent.info_hash,
            our_id: self.config.peer_id,
            piece_count: self.torrent.piece_count(),
            events: self.events_tx.clone(),
            timeouts: self.config.timeouts,
            metadata: Some(self.torrent.info_bytes.clone()),
            limits: self.config.limits.clone(),
        };
        tokio::spawn(connection::run(target, params, rx));
        self.peers.insert(
            key,
            Peer::new(addr, tx, outgoing, self.torrent.piece_count()),
        );
    }

    fn drop_peer(&mut self, key: PeerKey, reason: &str) {
        let Some(peer) = self.peers.remove(&key) else {
            return;
        };
        debug!(peer = %peer.addr, "dropping peer: {reason}");
        self.picker.remove_peer_pieces(&peer.bitfield);
        self.stats.waste.released_on_drop += self.picker.release_peer(key) as u64;
        self.peer_ids.remove(&key);
        if peer.outgoing && !self.banned.contains(&peer.addr) {
            let tries = self.attempts.get(&peer.addr).copied().unwrap_or(0);
            if tries < MAX_ATTEMPTS {
                self.retry
                    .push((Instant::now() + RETRY_DELAY * tries.max(1), peer.addr));
            }
        }
        // Dropping `peer.tx` closes the command channel, which ends the task.
    }

    /// Sends a message without waiting. A peer whose queue is full is too
    /// slow to keep up and is dropped, so the session never blocks on it.
    fn send(&mut self, key: PeerKey, msg: Message) -> bool {
        let ok = match self.peers.get(&key) {
            Some(p) => p.tx.try_send(PeerCommand::Send(msg)).is_ok(),
            None => return false,
        };
        if !ok {
            self.drop_peer(key, "command queue full");
        }
        ok
    }

    fn on_connected(&mut self, key: PeerKey, peer_id: [u8; 20]) {
        if self.banned_ids.contains(&peer_id) {
            self.drop_peer(key, "banned peer id");
            return;
        }
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.connected = true;
        self.peer_ids.insert(key, peer_id);
        self.stats.peers_connected += 1;
        if let Some(addr) = self.peers.get(&key).map(|p| p.addr) {
            self.attempts.remove(&addr);
            debug!(peer = %addr, "connected");
        }
        if self.picker.verified_count() > 0 {
            let bits = self.picker.have_bitfield().as_bytes().to_vec();
            self.send(key, Message::Bitfield(bits));
        }
    }

    async fn on_message(&mut self, key: PeerKey, msg: Message) -> Result<(), SessionError> {
        let now = Instant::now();
        let Some(peer) = self.peers.get_mut(&key) else {
            return Ok(());
        };
        match msg {
            Message::KeepAlive => {}
            Message::Choke => {
                peer.peer_choking = true;
                if peer.am_interested {
                    peer.choked_since = Some(now);
                }
                self.stats.waste.released_on_choke += self.picker.release_peer(key) as u64;
            }
            Message::Unchoke => {
                peer.peer_choking = false;
                peer.choked_since = None;
                self.fill(key);
            }
            Message::Interested => {
                peer.peer_interested = true;
                let unchoked = self.peers.values().filter(|p| !p.am_choking).count();
                let choking = self.peers.get(&key).is_some_and(|p| p.am_choking);
                if choking && unchoked < self.choker.slots() {
                    self.set_choke(key, false);
                }
            }
            Message::NotInterested => peer.peer_interested = false,
            Message::Have { index } => {
                if !peer.bitfield.has(index as usize) {
                    peer.bitfield.set(index as usize);
                    self.picker.add_have(index);
                }
                if !self.complete && self.picker.wants(index) {
                    self.set_interested(key, true);
                }
                self.fill(key);
            }
            Message::Bitfield(bytes) => {
                let Ok(bits) = Bitfield::from_bytes(&bytes, self.torrent.piece_count()) else {
                    self.drop_peer(key, "invalid bitfield");
                    return Ok(());
                };
                self.picker.remove_peer_pieces(&peer.bitfield);
                self.picker.add_peer_pieces(&bits);
                peer.bitfield = bits;
                self.update_interest(key);
                self.fill(key);
            }
            Message::Request {
                index,
                begin,
                length,
            } => self.on_request(key, index, begin, length).await?,
            Message::Piece { index, begin, data } => {
                let len = data.len() as u64;
                peer.dl_cur += len;
                peer.dl_second += len;
                self.downloaded += len;
                let from = peer.addr;
                match self
                    .picker
                    .block_received(key, index, begin, data.len() as u32)
                {
                    BlockOutcome::Accepted {
                        piece_complete,
                        cancel,
                    } => {
                        self.disk_send(DiskCommand::Write {
                            index,
                            begin,
                            data,
                            from,
                        })
                        .await?;
                        for other in cancel {
                            self.send(
                                other,
                                Message::Cancel {
                                    index,
                                    begin,
                                    length: len as u32,
                                },
                            );
                        }
                        if piece_complete {
                            self.verify(index).await?;
                        }
                    }
                    BlockOutcome::Duplicate => self.stats.waste.duplicate_bytes += len,
                    BlockOutcome::Unexpected => self.stats.waste.unexpected_bytes += len,
                }
                self.fill(key);
            }
            // Extension messages are answered by the peer task itself.
            Message::Cancel { .. } | Message::Extended { .. } => {}
        }
        Ok(())
    }

    async fn on_request(
        &mut self,
        key: PeerKey,
        index: u32,
        begin: u32,
        length: u32,
    ) -> Result<(), SessionError> {
        let Some(peer) = self.peers.get(&key) else {
            return Ok(());
        };
        if peer.am_choking || self.picker.status(index) != Some(PieceStatus::Verified) {
            return Ok(());
        }
        if length == 0 || begin as u64 + length as u64 > self.torrent.piece_size(index) as u64 {
            self.drop_peer(key, "invalid request");
            return Ok(());
        }
        let peer_tx = peer.tx.clone();
        let (reply, rx) = oneshot::channel();
        self.disk_send(DiskCommand::Read {
            index,
            begin,
            length,
            reply,
        })
        .await?;
        tokio::spawn(async move {
            if let Ok(Ok(data)) = rx.await {
                let _ = peer_tx
                    .send(PeerCommand::Send(Message::Piece { index, begin, data }))
                    .await;
            }
        });
        Ok(())
    }

    async fn disk_send(&self, cmd: DiskCommand) -> Result<(), SessionError> {
        self.disk.send(cmd).await.map_err(|_| DiskError::Gone)?;
        Ok(())
    }

    async fn verify(&mut self, index: u32) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.disk_send(DiskCommand::Verify { index, reply }).await?;
        let events = self.events_tx.clone();
        tokio::spawn(async move {
            let result = rx.await.unwrap_or(Err(DiskError::Gone));
            let _ = events
                .send(SessionEvent::PieceChecked { index, result })
                .await;
        });
        Ok(())
    }

    fn on_piece_checked(&mut self, index: u32, result: VerifyResult) -> Result<bool, SessionError> {
        for addr in &result.guilty {
            let strikes = self.strikes.entry(*addr).or_insert(0);
            *strikes += 1;
            debug!(peer = %addr, "strike {} for bad data in piece {index}", *strikes);
            if *strikes >= MAX_STRIKES {
                self.ban(*addr);
            }
        }
        if result.ok {
            self.picker.piece_verified(index);
            let keys: Vec<PeerKey> = self
                .peers
                .iter()
                .filter(|(_, p)| p.connected && !p.bitfield.has(index as usize))
                .map(|(k, _)| *k)
                .collect();
            for k in keys {
                self.send(k, Message::Have { index });
            }
            if self.picker.is_complete() {
                self.on_complete();
                if !self.config.seed {
                    return Ok(true);
                }
            }
        } else {
            warn!("piece {index} failed its hash check; downloading it again");
            self.stats.failed_pieces += 1;
            self.stats.waste.failed_bytes += self.torrent.piece_size(index) as u64;
            self.picker.piece_failed(index);
        }
        self.fill_all();
        Ok(false)
    }

    fn ban(&mut self, addr: SocketAddr) {
        if !self.banned.insert(addr) {
            return;
        }
        warn!(peer = %addr, "banning peer after {MAX_STRIKES} bad pieces");
        self.stats.banned.push(addr);
        let keys: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| p.addr == addr)
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            if let Some(id) = self.peer_ids.get(&k) {
                self.banned_ids.insert(*id);
            }
            if let Some(p) = self.peers.get(&k) {
                let _ = p.tx.try_send(PeerCommand::Close);
            }
            self.drop_peer(k, "banned");
        }
        self.retry.retain(|(_, a)| *a != addr);
    }

    fn on_complete(&mut self) {
        if self.complete {
            return;
        }
        self.complete = true;
        info!(
            "download complete: all {} pieces verified",
            self.picker.piece_count()
        );
        self.choker.set_seeding(true);
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for k in keys {
            self.set_interested(k, false);
        }
        self.publish_progress();
    }

    fn set_interested(&mut self, key: PeerKey, wanted: bool) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.am_interested == wanted || !p.connected {
            return;
        }
        p.am_interested = wanted;
        p.choked_since = (wanted && p.peer_choking).then(Instant::now);
        let msg = if wanted {
            Message::Interested
        } else {
            Message::NotInterested
        };
        self.send(key, msg);
    }

    fn update_interest(&mut self, key: PeerKey) {
        let wanted = !self.complete
            && self
                .peers
                .get(&key)
                .is_some_and(|p| self.picker.wants_any(&p.bitfield));
        self.set_interested(key, wanted);
    }

    fn set_choke(&mut self, key: PeerKey, choke: bool) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.am_choking == choke || !p.connected {
            return;
        }
        p.am_choking = choke;
        self.send(
            key,
            if choke {
                Message::Choke
            } else {
                Message::Unchoke
            },
        );
    }

    /// Tops up the peer's outstanding requests to its queue depth.
    fn fill(&mut self, key: PeerKey) {
        if self.complete {
            return;
        }
        let Some(p) = self.peers.get(&key) else {
            return;
        };
        if !p.connected || p.peer_choking || !p.am_interested {
            return;
        }
        // In endgame every request may be a duplicate, so keep it shallow.
        let depth = if self.picker.in_endgame() {
            self.config.pipeline
        } else {
            queue_depth(p.dl_rate, self.config.pipeline)
        };
        let have = self.picker.outstanding(key);
        if have >= depth {
            return;
        }
        let reqs =
            self.picker
                .next_requests(key, &p.bitfield, depth - have, std::time::Instant::now());
        for r in reqs {
            if !self.send(
                key,
                Message::Request {
                    index: r.index,
                    begin: r.begin,
                    length: r.length,
                },
            ) {
                break;
            }
        }
    }

    fn fill_all(&mut self) {
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for k in keys {
            self.fill(k);
        }
    }

    fn on_tick(&mut self) {
        let now = Instant::now();

        for p in self.peers.values_mut() {
            let second = std::mem::take(&mut p.dl_second) as f64;
            p.dl_rate = RATE_SMOOTHING * p.dl_rate + (1.0 - RATE_SMOOTHING) * second;
            let up = std::mem::take(&mut p.ul_second) as f64;
            p.ul_rate = RATE_SMOOTHING * p.ul_rate + (1.0 - RATE_SMOOTHING) * up;
        }

        // Requests that have waited too long go back to the pool.
        let expired = self
            .picker
            .expire(std::time::Instant::now(), self.config.timeouts.block);
        if !expired.is_empty() {
            debug!("{} block requests timed out", expired.len());
            self.stats.waste.expired += expired.len() as u64;
        }

        // Peers that keep us choked while we want their pieces.
        let choked_limit = self.config.timeouts.choked;
        let snubbed: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| {
                p.choked_since
                    .is_some_and(|t| now.duration_since(t) > choked_limit)
            })
            .map(|(k, _)| *k)
            .collect();
        for k in snubbed {
            self.drop_peer(k, "kept us choked too long");
        }

        if !self.complete {
            let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
            for k in keys {
                self.update_interest(k);
            }
        }

        // Re-dial peers whose retry delay has passed, then connect more.
        let due: Vec<SocketAddr> = self
            .retry
            .iter()
            .filter(|(t, _)| *t <= now)
            .map(|(_, a)| *a)
            .collect();
        self.retry.retain(|(t, _)| *t > now);
        for a in due {
            if !self.banned.contains(&a) {
                self.candidates.push_back(a);
            }
        }
        self.connect_more();
        self.fill_all();
        self.publish_progress();
    }

    fn run_choke(&mut self) {
        let window = (2 * CHOKE_INTERVAL).as_secs_f64();
        let stats: Vec<PeerStats> = self
            .peers
            .iter()
            .filter(|(_, p)| p.connected)
            .map(|(k, p)| PeerStats {
                id: *k,
                interested: p.peer_interested,
                download_rate: (p.dl_cur + p.dl_prev) as f64 / window,
                upload_rate: (p.ul_cur + p.ul_prev) as f64 / window,
            })
            .collect();
        for p in self.peers.values_mut() {
            p.dl_prev = std::mem::take(&mut p.dl_cur);
            p.ul_prev = std::mem::take(&mut p.ul_cur);
        }
        let chosen: HashSet<PeerKey> = self
            .choker
            .choose_unchoked(&stats, std::time::Instant::now())
            .into_iter()
            .collect();
        for s in stats {
            self.set_choke(s.id, !chosen.contains(&s.id));
        }
    }

    fn publish_progress(&mut self) {
        let totals = (self.downloaded, self.uploaded);
        let delta = (
            totals.0.saturating_sub(self.last_totals.0),
            totals.1.saturating_sub(self.last_totals.1),
        );
        self.last_totals = totals;
        self.rate_window.push_back(delta);
        while self.rate_window.len() > 5 {
            self.rate_window.pop_front();
        }
        let n = self.rate_window.len().max(1) as f64;
        let (d, u) = self
            .rate_window
            .iter()
            .fold((0u64, 0u64), |(a, b), (x, y)| (a + x, b + y));
        let progress = Progress {
            name: self.torrent.name.clone(),
            total: self.torrent.total_length,
            verified_bytes: self.picker.verified_bytes(),
            pieces_done: self.picker.verified_count(),
            piece_count: self.picker.piece_count(),
            downloaded: self.downloaded,
            uploaded: self.uploaded,
            download_rate: d as f64 / n,
            upload_rate: u as f64 / n,
            peers: self.peers.values().filter(|p| p.connected).count(),
            complete: self.complete,
            peer_rows: self
                .peers
                .iter()
                .filter(|(_, p)| p.connected)
                .map(|(k, p)| PeerRow {
                    addr: p.addr,
                    client: self
                        .peer_ids
                        .get(k)
                        .map(crate::peer::client_name)
                        .unwrap_or_default(),
                    down_rate: p.dl_rate,
                    up_rate: p.ul_rate,
                    peer_choking: p.peer_choking,
                    am_interested: p.am_interested,
                    am_choking: p.am_choking,
                    peer_interested: p.peer_interested,
                    requests: self.picker.outstanding(*k),
                    pieces: p.bitfield.count_ones(),
                })
                .collect(),
            pieces: self.picker.statuses().to_vec(),
        };
        self.progress_tx.send_replace(progress);
    }

    async fn flush(&mut self) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.disk_send(DiskCommand::Flush { reply }).await?;
        rx.await.map_err(|_| DiskError::Gone)??;
        Ok(())
    }

    fn stats(&self) -> SessionStats {
        let mut s = self.stats.clone();
        s.downloaded = self.downloaded;
        s.uploaded = self.uploaded;
        s.waste.endgame_requests = self.picker.endgame_requests();
        s
    }

    /// Drops every peer connection and the disk sender.
    fn shutdown(mut self) -> watch::Sender<Progress> {
        self.publish_progress();
        for p in self.peers.values() {
            let _ = p.tx.try_send(PeerCommand::Close);
        }
        self.peers.clear();
        self.progress_tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_depth_follows_rate() {
        assert_eq!(queue_depth(0.0, 5), 5);
        // 1 MiB/s for 3 s is 192 blocks, capped at 128.
        assert_eq!(queue_depth(1024.0 * 1024.0, 5), 128);
        // 100 KiB/s for 3 s is 18.75 blocks, rounded up.
        assert_eq!(queue_depth(100.0 * 1024.0, 5), 19);
        // The minimum wins, even above the cap.
        assert_eq!(queue_depth(0.0, 200), 200);
    }

    #[test]
    fn in_progress_cap_is_by_memory() {
        assert_eq!(max_in_progress(16, 4 * 1024 * 1024), 16);
        assert_eq!(max_in_progress(16, 256 * 1024), 256);
        assert_eq!(max_in_progress(16, 64 * 1024 * 1024), 16);
    }
}
