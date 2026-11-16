//! The discrete-event swarm simulation.
//!
//! A fake clock and a queue of future events sorted by time: the loop jumps
//! from one event to the next, so an hour of swarm activity runs in seconds.
//! Peers transfer whole pieces. Every random choice comes from one seeded
//! generator and every collection is ordered, so a seed fully determines
//! the result.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::scenario::{ChokeStrategyKind, PieceStrategyKind, Scenario};
use crate::bitfield::Bitfield;
use crate::choke::{child_rng, ChokeStrategy, NoReward, PeerStats, RandomChoke, TitForTat};
use crate::metainfo::piece_size;
use crate::picker::{PieceState, PieceStatus, PieceStrategy, RandomPiece, RarestFirst, Sequential};

/// Simulated time in microseconds.
type Time = u64;

fn to_time(secs: f64) -> Time {
    (secs * 1e6).round().max(0.0) as Time
}

fn to_secs(t: Time) -> f64 {
    t as f64 / 1e6
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Ev {
    Join(usize),
    Leave(usize),
    ChokeTick(usize),
    Done { from: usize, to: usize, tid: u64 },
    Sample,
}

#[derive(Debug, Clone, Copy)]
struct Transfer {
    piece: u32,
    tid: u64,
}

struct SimPeer {
    group: usize,
    is_seed: bool,
    up: f64,
    down: f64,
    join: Time,
    present: bool,
    finish: Option<Time>,
    left: Option<Time>,
    has: Bitfield,
    /// Verified or in progress: pieces this peer no longer needs to start.
    taken: Bitfield,
    status: Vec<PieceStatus>,
    verified: usize,
    /// How many of this peer's neighbors have each piece.
    availability: Vec<u32>,
    neighbors: BTreeSet<usize>,
    /// Peers this one uploads to.
    unchoked: BTreeSet<usize>,
    uploads: BTreeMap<usize, Transfer>,
    downloads: BTreeMap<usize, Transfer>,
    recv_cur: BTreeMap<usize, u64>,
    recv_prev: BTreeMap<usize, u64>,
    sent_cur: BTreeMap<usize, u64>,
    sent_prev: BTreeMap<usize, u64>,
    uploaded: u64,
    downloaded: u64,
    pick: Box<dyn PieceStrategy>,
    choke: Box<dyn ChokeStrategy>,
    leave_after_complete: Option<f64>,
    leave_after_copies: Option<f64>,
    leave_scheduled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeerResult {
    pub peer: usize,
    pub group: String,
    pub is_seed: bool,
    pub join_time: f64,
    pub finish_time: Option<f64>,
    pub left_time: Option<f64>,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
}

impl PeerResult {
    pub fn download_time(&self) -> Option<f64> {
        self.finish_time.map(|f| f - self.join_time)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunResult {
    pub seed: u64,
    pub peers: Vec<PeerResult>,
    /// (time, lowest number of present peers holding any one piece).
    pub min_availability: Vec<(f64, u32)>,
    pub end_time: f64,
}

pub struct Sim<'a> {
    sc: &'a Scenario,
    peers: Vec<SimPeer>,
    queue: BinaryHeap<Reverse<(Time, u64, Ev)>>,
    seq: u64,
    now: Time,
    rng: ChaCha8Rng,
    base: Instant,
    next_tid: u64,
    pending_joins: usize,
    active_leechers: usize,
    pieces: usize,
    samples: Vec<(f64, u32)>,
}

/// Runs one simulation of `sc` with the given seed.
pub fn run(sc: &Scenario, seed: u64) -> RunResult {
    let mut sim = Sim::new(sc, seed);
    sim.run_to_end();
    sim.result(seed)
}

impl<'a> Sim<'a> {
    fn new(sc: &'a Scenario, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let pieces = sc.file_size.div_ceil(sc.piece_size) as usize;
        let mut peers = Vec::new();
        for (gi, g) in sc.groups.iter().enumerate() {
            for _ in 0..g.count {
                let join_secs = if g.join_end > g.join_start {
                    rng.gen_range(g.join_start..g.join_end)
                } else {
                    g.join_start
                };
                let pick: Box<dyn PieceStrategy> = match sc.piece_strategy {
                    PieceStrategyKind::RarestFirst => {
                        Box::new(RarestFirst::new(child_rng(&mut rng)))
                    }
                    PieceStrategyKind::Random => Box::new(RandomPiece::new(child_rng(&mut rng))),
                    PieceStrategyKind::Sequential => Box::new(Sequential),
                };
                let mut choke: Box<dyn ChokeStrategy> = match sc.choke_strategy {
                    ChokeStrategyKind::TitForTat => {
                        Box::new(TitForTat::new(sc.upload_slots, child_rng(&mut rng)))
                    }
                    ChokeStrategyKind::Random => {
                        Box::new(RandomChoke::new(sc.upload_slots, child_rng(&mut rng)))
                    }
                    ChokeStrategyKind::NoReward => Box::new(NoReward),
                };
                let (has, status, verified) = if g.seed {
                    choke.set_seeding(true);
                    (
                        Bitfield::full(pieces),
                        vec![PieceStatus::Verified; pieces],
                        pieces,
                    )
                } else {
                    (Bitfield::new(pieces), vec![PieceStatus::Missing; pieces], 0)
                };
                peers.push(SimPeer {
                    group: gi,
                    is_seed: g.seed,
                    up: g.upload_speed,
                    down: g.download_speed,
                    join: to_time(join_secs),
                    present: false,
                    finish: None,
                    left: None,
                    taken: has.clone(),
                    has,
                    status,
                    verified,
                    availability: vec![0; pieces],
                    neighbors: BTreeSet::new(),
                    unchoked: BTreeSet::new(),
                    uploads: BTreeMap::new(),
                    downloads: BTreeMap::new(),
                    recv_cur: BTreeMap::new(),
                    recv_prev: BTreeMap::new(),
                    sent_cur: BTreeMap::new(),
                    sent_prev: BTreeMap::new(),
                    uploaded: 0,
                    downloaded: 0,
                    pick,
                    choke,
                    leave_after_complete: g.leave_after_complete,
                    leave_after_copies: g.leave_after_uploaded_copies,
                    leave_scheduled: false,
                });
            }
        }
        let mut sim = Sim {
            sc,
            queue: BinaryHeap::new(),
            seq: 0,
            now: 0,
            rng,
            base: Instant::now(),
            next_tid: 0,
            pending_joins: peers.len(),
            active_leechers: 0,
            pieces,
            samples: Vec::new(),
            peers,
        };
        for p in 0..sim.peers.len() {
            let t = sim.peers[p].join;
            sim.schedule(t, Ev::Join(p));
        }
        sim.schedule(0, Ev::Sample);
        sim
    }

    fn schedule(&mut self, at: Time, ev: Ev) {
        self.seq += 1;
        self.queue.push(Reverse((at, self.seq, ev)));
    }

    fn run_to_end(&mut self) {
        let max = to_time(self.sc.max_time_secs);
        while let Some(Reverse((t, _, ev))) = self.queue.pop() {
            if t > max {
                self.now = max;
                break;
            }
            self.now = t;
            match ev {
                Ev::Join(p) => self.on_join(p),
                Ev::Leave(p) => self.on_leave(p),
                Ev::ChokeTick(p) => self.on_choke_tick(p),
                Ev::Done { from, to, tid } => self.on_done(from, to, tid),
                Ev::Sample => self.on_sample(),
            }
            if self.pending_joins == 0 && self.active_leechers == 0 {
                break;
            }
        }
    }

    fn result(&self, seed: u64) -> RunResult {
        RunResult {
            seed,
            peers: self
                .peers
                .iter()
                .enumerate()
                .map(|(i, p)| PeerResult {
                    peer: i,
                    group: self.sc.groups[p.group].name.clone(),
                    is_seed: p.is_seed,
                    join_time: to_secs(p.join),
                    finish_time: p.finish.map(to_secs),
                    left_time: p.left.map(to_secs),
                    bytes_uploaded: p.uploaded,
                    bytes_downloaded: p.downloaded,
                })
                .collect(),
            min_availability: self.samples.clone(),
            end_time: to_secs(self.now),
        }
    }

    /// A peer dials up to `max_neighbors` others but, like a real client
    /// with spare incoming slots, accepts connections up to twice that, so
    /// late joiners are never left with nobody to talk to.
    fn accept_limit(&self) -> usize {
        self.sc.max_neighbors * 2
    }

    fn piece_bytes(&self, piece: u32) -> u64 {
        piece_size(self.sc.file_size, self.sc.piece_size, piece as u64)
    }

    /// `d` wants something `u` has.
    fn interested(&self, d: usize, u: usize) -> bool {
        let dp = &self.peers[d];
        dp.finish.is_none() && !dp.is_seed && self.peers[u].has.has_any_not_in(&dp.taken)
    }

    fn connect(&mut self, a: usize, b: usize) {
        if a == b || !self.peers[a].neighbors.insert(b) {
            return;
        }
        self.peers[b].neighbors.insert(a);
        let (ha, hb) = (self.peers[a].has.clone(), self.peers[b].has.clone());
        for i in hb.iter_ones() {
            self.peers[a].availability[i] += 1;
        }
        for i in ha.iter_ones() {
            self.peers[b].availability[i] += 1;
        }
    }

    /// If `d` is interested in `u`, give it a free slot of `u` and start a
    /// transfer if possible.
    fn wake(&mut self, u: usize, d: usize) {
        if !self.interested(d, u) {
            return;
        }
        let slots = self.peers[u].choke.slots();
        let up = &mut self.peers[u];
        if !up.unchoked.contains(&d) && up.unchoked.len() < slots {
            up.unchoked.insert(d);
        }
        self.try_start(u, d);
    }

    fn try_start(&mut self, u: usize, d: usize) {
        {
            let (up, dp) = (&self.peers[u], &self.peers[d]);
            if !up.present
                || !dp.present
                || up.up <= 0.0
                || !up.unchoked.contains(&d)
                || up.uploads.contains_key(&d)
                || dp.finish.is_some()
                || dp.is_seed
            {
                return;
            }
        }
        let u_has = self.peers[u].has.clone();
        let piece = {
            let dp = &mut self.peers[d];
            let state = PieceState {
                status: &dp.status,
                availability: &dp.availability,
                verified: dp.verified,
            };
            dp.pick.pick(&u_has, &state)
        };
        let Some(piece) = piece else { return };
        if self.peers[d].status.get(piece as usize) != Some(&PieceStatus::Missing) {
            return;
        }
        let bytes = self.piece_bytes(piece);
        let up = &self.peers[u];
        let dp = &self.peers[d];
        let up_share = up.up / up.unchoked.len().max(1) as f64;
        let down_share = dp.down / (dp.downloads.len() + 1) as f64;
        let rate = up_share.min(down_share);
        let dur = ((bytes as f64 / rate) * 1e6).ceil().max(1.0) as Time;

        self.next_tid += 1;
        let tid = self.next_tid;
        let t = Transfer { piece, tid };
        let dp = &mut self.peers[d];
        dp.status[piece as usize] = PieceStatus::InProgress;
        dp.taken.set(piece as usize);
        dp.downloads.insert(u, t);
        self.peers[u].uploads.insert(d, t);
        self.schedule(
            self.now + dur,
            Ev::Done {
                from: u,
                to: d,
                tid,
            },
        );
    }

    fn on_join(&mut self, p: usize) {
        self.pending_joins -= 1;
        self.peers[p].present = true;
        if !self.peers[p].is_seed {
            self.active_leechers += 1;
        }
        let max = self.sc.max_neighbors;
        let accept = self.accept_limit();
        let mut cands: Vec<usize> = (0..self.peers.len())
            .filter(|&q| q != p && self.peers[q].present && self.peers[q].neighbors.len() < accept)
            .collect();
        cands.shuffle(&mut self.rng);
        cands.truncate(max);
        for &q in &cands {
            self.connect(p, q);
        }
        for q in cands {
            self.wake(p, q);
            self.wake(q, p);
        }
        let interval = self.sc.choke_interval_secs;
        let offset = to_time(self.rng.gen_range(0.0..interval));
        self.schedule(self.now + offset, Ev::ChokeTick(p));
    }

    fn on_leave(&mut self, p: usize) {
        if !self.peers[p].present {
            return;
        }
        self.peers[p].present = false;
        self.peers[p].left = Some(self.now);
        if !self.peers[p].is_seed && self.peers[p].finish.is_none() {
            self.active_leechers -= 1;
        }
        let neighbors: Vec<usize> = std::mem::take(&mut self.peers[p].neighbors)
            .into_iter()
            .collect();
        let has = self.peers[p].has.clone();
        for &n in &neighbors {
            let np = &mut self.peers[n];
            np.neighbors.remove(&p);
            np.unchoked.remove(&p);
            for i in has.iter_ones() {
                np.availability[i] = np.availability[i].saturating_sub(1);
            }
            if let Some(t) = self.peers[p].uploads.remove(&n) {
                let np = &mut self.peers[n];
                np.downloads.remove(&p);
                np.status[t.piece as usize] = PieceStatus::Missing;
                np.taken.clear(t.piece as usize);
            }
            if self.peers[p].downloads.remove(&n).is_some() {
                self.peers[n].uploads.remove(&p);
            }
        }
        self.peers[p].unchoked.clear();
        // Neighbors may now have free slots or need new sources.
        for &n in &neighbors {
            let others: Vec<usize> = self.peers[n].neighbors.iter().copied().collect();
            for m in others {
                self.wake(n, m);
                self.wake(m, n);
            }
        }
    }

    fn on_choke_tick(&mut self, p: usize) {
        if !self.peers[p].present {
            return;
        }
        // Top up the neighbor set, as a client would after re-announcing.
        let max = self.sc.max_neighbors;
        let accept = self.accept_limit();
        let need = max.saturating_sub(self.peers[p].neighbors.len());
        if need > 0 {
            let mut cands: Vec<usize> = (0..self.peers.len())
                .filter(|&q| {
                    q != p
                        && self.peers[q].present
                        && self.peers[q].neighbors.len() < accept
                        && !self.peers[p].neighbors.contains(&q)
                })
                .collect();
            cands.shuffle(&mut self.rng);
            cands.truncate(need);
            for &q in &cands {
                self.connect(p, q);
            }
            for q in cands {
                self.wake(q, p);
            }
        }

        let window = 2.0 * self.sc.choke_interval_secs;
        let neighbors: Vec<usize> = self.peers[p].neighbors.iter().copied().collect();
        let stats: Vec<PeerStats> = neighbors
            .iter()
            .map(|&n| {
                let pp = &self.peers[p];
                let got = pp.recv_cur.get(&n).copied().unwrap_or(0)
                    + pp.recv_prev.get(&n).copied().unwrap_or(0);
                let gave = pp.sent_cur.get(&n).copied().unwrap_or(0)
                    + pp.sent_prev.get(&n).copied().unwrap_or(0);
                PeerStats {
                    id: n as u64,
                    interested: self.interested(n, p),
                    download_rate: got as f64 / window,
                    upload_rate: gave as f64 / window,
                }
            })
            .collect();
        let now = self.base + Duration::from_micros(self.now);
        let pp = &mut self.peers[p];
        pp.recv_prev = std::mem::take(&mut pp.recv_cur);
        pp.sent_prev = std::mem::take(&mut pp.sent_cur);
        let chosen = pp.choke.choose_unchoked(&stats, now);
        pp.unchoked = chosen.into_iter().map(|id| id as usize).collect();
        let unchoked: Vec<usize> = pp.unchoked.iter().copied().collect();
        for d in unchoked {
            self.try_start(p, d);
        }
        let next = self.now + to_time(self.sc.choke_interval_secs);
        self.schedule(next, Ev::ChokeTick(p));
    }

    fn on_done(&mut self, u: usize, d: usize, tid: u64) {
        let valid = self.peers[u].uploads.get(&d).map(|t| t.tid) == Some(tid)
            && self.peers[d].downloads.get(&u).map(|t| t.tid) == Some(tid);
        if !valid {
            return; // cancelled when a peer left
        }
        let Some(t) = self.peers[u].uploads.remove(&d) else {
            return;
        };
        self.peers[d].downloads.remove(&u);
        let piece = t.piece as usize;
        let bytes = self.piece_bytes(t.piece);

        let dp = &mut self.peers[d];
        dp.status[piece] = PieceStatus::Verified;
        dp.has.set(piece);
        dp.verified += 1;
        dp.downloaded += bytes;
        *dp.recv_cur.entry(u).or_insert(0) += bytes;
        let up = &mut self.peers[u];
        up.uploaded += bytes;
        *up.sent_cur.entry(d).or_insert(0) += bytes;

        let neighbors: Vec<usize> = self.peers[d].neighbors.iter().copied().collect();
        for &n in &neighbors {
            self.peers[n].availability[piece] += 1;
        }

        if self.peers[d].verified == self.pieces {
            self.finish(d);
        }
        self.check_copies_leave(u);

        self.try_start(u, d);
        for n in neighbors {
            self.wake(d, n);
        }
    }

    fn finish(&mut self, d: usize) {
        let dp = &mut self.peers[d];
        dp.finish = Some(self.now);
        dp.choke.set_seeding(true);
        self.active_leechers -= 1;
        if let Some(after) = dp.leave_after_complete {
            if !dp.leave_scheduled {
                dp.leave_scheduled = true;
                let at = self.now + to_time(after);
                self.schedule(at, Ev::Leave(d));
            }
        }
    }

    fn check_copies_leave(&mut self, u: usize) {
        let file = self.sc.file_size as f64;
        let up = &mut self.peers[u];
        if let Some(copies) = up.leave_after_copies {
            if !up.leave_scheduled && up.uploaded as f64 >= copies * file {
                up.leave_scheduled = true;
                let now = self.now;
                self.schedule(now, Ev::Leave(u));
            }
        }
    }

    fn on_sample(&mut self) {
        let mut counts = vec![0u32; self.pieces];
        for p in self.peers.iter().filter(|p| p.present) {
            for i in p.has.iter_ones() {
                counts[i] += 1;
            }
        }
        let min = counts.iter().copied().min().unwrap_or(0);
        self.samples.push((to_secs(self.now), min));
        let next = self.now + to_time(self.sc.sample_interval_secs);
        self.schedule(next, Ev::Sample);
    }
}
