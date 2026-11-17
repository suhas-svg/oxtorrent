//! Piece and block selection. Pure logic with no I/O.
//!
//! The [`Picker`] tracks which pieces are missing, in progress or verified,
//! which blocks of in-progress pieces have been requested from whom, and how
//! many connected peers have each piece. Which *new* piece to start is
//! delegated to a [`PieceStrategy`] so the swarm simulator can swap it.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::bitfield::Bitfield;
use crate::metainfo::{block_count, block_size, piece_size};
use crate::{PeerKey, BLOCK_SIZE};

/// In endgame, the most peers one block is requested from at once.
const ENDGAME_REQUESTERS: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceStatus {
    Missing,
    InProgress,
    Verified,
}

/// Read-only view of the download handed to a [`PieceStrategy`].
pub struct PieceState<'a> {
    pub status: &'a [PieceStatus],
    /// How many connected peers have each piece.
    pub availability: &'a [u32],
    /// Number of verified pieces.
    pub verified: usize,
}

/// Chooses which missing piece to start next from a peer that has
/// `peer_has`. Must only return a piece that is `Missing` in `state` and set
/// in `peer_has`.
pub trait PieceStrategy: Send + Sync {
    fn pick(&mut self, peer_has: &Bitfield, state: &PieceState) -> Option<u32>;
}

fn candidates<'a>(
    peer_has: &'a Bitfield,
    state: &'a PieceState,
) -> impl Iterator<Item = usize> + 'a {
    state
        .status
        .iter()
        .enumerate()
        .filter(move |(i, s)| **s == PieceStatus::Missing && peer_has.has(*i))
        .map(|(i, _)| i)
}

/// Uniform random choice from an iterator in one pass (reservoir sampling).
fn random_choice(iter: impl Iterator<Item = usize>, rng: &mut impl Rng) -> Option<u32> {
    let mut chosen = None;
    for (seen, i) in iter.enumerate() {
        if rng.gen_range(0..=seen) == 0 {
            chosen = Some(i as u32);
        }
    }
    chosen
}

/// The default: the first few pieces at random, then the piece the fewest
/// peers have, with ties broken at random.
pub struct RarestFirst {
    rng: ChaCha8Rng,
    random_first: usize,
}

impl RarestFirst {
    pub fn new(rng: ChaCha8Rng) -> Self {
        RarestFirst {
            rng,
            random_first: 4,
        }
    }

    pub fn from_entropy() -> Self {
        Self::new(ChaCha8Rng::from_entropy())
    }
}

impl PieceStrategy for RarestFirst {
    fn pick(&mut self, peer_has: &Bitfield, state: &PieceState) -> Option<u32> {
        if state.verified < self.random_first {
            return random_choice(candidates(peer_has, state), &mut self.rng);
        }
        let mut best = None;
        let mut best_avail = u32::MAX;
        let mut ties = 0usize;
        for i in candidates(peer_has, state) {
            let a = state.availability[i];
            if a < best_avail {
                best_avail = a;
                best = Some(i as u32);
                ties = 1;
            } else if a == best_avail {
                ties += 1;
                if self.rng.gen_range(0..ties) == 0 {
                    best = Some(i as u32);
                }
            }
        }
        best
    }
}

/// Any needed piece, chosen at random.
pub struct RandomPiece {
    rng: ChaCha8Rng,
}

impl RandomPiece {
    pub fn new(rng: ChaCha8Rng) -> Self {
        RandomPiece { rng }
    }
}

impl PieceStrategy for RandomPiece {
    fn pick(&mut self, peer_has: &Bitfield, state: &PieceState) -> Option<u32> {
        random_choice(candidates(peer_has, state), &mut self.rng)
    }
}

/// The lowest-numbered needed piece first.
pub struct Sequential;

impl PieceStrategy for Sequential {
    fn pick(&mut self, peer_has: &Bitfield, state: &PieceState) -> Option<u32> {
        candidates(peer_has, state).next().map(|i| i as u32)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockRequest {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[derive(Clone, Debug)]
enum BlockState {
    Free,
    /// Who it was requested from and when. More than one entry only in
    /// endgame mode.
    Requested(Vec<(PeerKey, Instant)>),
    Received,
}

#[derive(Debug)]
struct Partial {
    blocks: Vec<BlockState>,
    received: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlockOutcome {
    /// Not a block of a piece in progress, or the wrong size.
    Unexpected,
    /// Already received from someone else (endgame).
    Duplicate,
    Accepted {
        piece_complete: bool,
        /// Other peers this block was requested from; send them a cancel.
        cancel: Vec<PeerKey>,
    },
}

pub struct Picker {
    total_length: u64,
    piece_length: u64,
    status: Vec<PieceStatus>,
    availability: Vec<u32>,
    partial: BTreeMap<u32, Partial>,
    outstanding: HashMap<PeerKey, usize>,
    verified: usize,
    missing: usize,
    max_in_progress: usize,
    strategy: Box<dyn PieceStrategy>,
    /// Duplicate requests sent in endgame mode.
    endgame_requests: u64,
}

impl Picker {
    pub fn new(
        total_length: u64,
        piece_length: u64,
        have: &Bitfield,
        max_in_progress: usize,
        strategy: Box<dyn PieceStrategy>,
    ) -> Self {
        let n = have.len();
        let status: Vec<PieceStatus> = (0..n)
            .map(|i| {
                if have.has(i) {
                    PieceStatus::Verified
                } else {
                    PieceStatus::Missing
                }
            })
            .collect();
        let verified = have.count_ones();
        Picker {
            total_length,
            piece_length,
            status,
            availability: vec![0; n],
            partial: BTreeMap::new(),
            outstanding: HashMap::new(),
            verified,
            missing: n - verified,
            max_in_progress: max_in_progress.max(1),
            strategy,
            endgame_requests: 0,
        }
    }

    pub fn piece_count(&self) -> usize {
        self.status.len()
    }

    pub fn piece_size(&self, index: u32) -> u32 {
        piece_size(self.total_length, self.piece_length, index as u64) as u32
    }

    pub fn status(&self, index: u32) -> Option<PieceStatus> {
        self.status.get(index as usize).copied()
    }

    pub fn verified_count(&self) -> usize {
        self.verified
    }

    pub fn is_complete(&self) -> bool {
        self.verified == self.status.len()
    }

    pub fn verified_bytes(&self) -> u64 {
        self.status
            .iter()
            .enumerate()
            .filter(|(_, s)| **s == PieceStatus::Verified)
            .map(|(i, _)| self.piece_size(i as u32) as u64)
            .sum()
    }

    pub fn have_bitfield(&self) -> Bitfield {
        let mut b = Bitfield::new(self.status.len());
        for (i, s) in self.status.iter().enumerate() {
            if *s == PieceStatus::Verified {
                b.set(i);
            }
        }
        b
    }

    pub fn availability(&self, index: u32) -> u32 {
        self.availability.get(index as usize).copied().unwrap_or(0)
    }

    pub fn add_peer_pieces(&mut self, has: &Bitfield) {
        for i in has.iter_ones() {
            self.availability[i] += 1;
        }
    }

    pub fn remove_peer_pieces(&mut self, has: &Bitfield) {
        for i in has.iter_ones() {
            self.availability[i] = self.availability[i].saturating_sub(1);
        }
    }

    pub fn add_have(&mut self, index: u32) {
        if let Some(a) = self.availability.get_mut(index as usize) {
            *a += 1;
        }
    }

    /// True if we still need piece `index`.
    pub fn wants(&self, index: u32) -> bool {
        matches!(
            self.status(index),
            Some(PieceStatus::Missing | PieceStatus::InProgress)
        )
    }

    /// True if a peer with `has` has any piece we still need.
    pub fn wants_any(&self, has: &Bitfield) -> bool {
        has.iter_ones()
            .any(|i| self.status[i] != PieceStatus::Verified)
    }

    pub fn outstanding(&self, peer: PeerKey) -> usize {
        self.outstanding.get(&peer).copied().unwrap_or(0)
    }

    fn dec_outstanding(&mut self, peer: PeerKey) {
        if let Some(n) = self.outstanding.get_mut(&peer) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.outstanding.remove(&peer);
            }
        }
    }

    /// Endgame: every missing block has already been requested from someone.
    pub fn in_endgame(&self) -> bool {
        self.missing == 0
            && !self.partial.is_empty()
            && self
                .partial
                .values()
                .all(|p| p.blocks.iter().all(|b| !matches!(b, BlockState::Free)))
    }

    /// Picks up to `max` blocks to request from `peer`, which has `has`.
    pub fn next_requests(
        &mut self,
        peer: PeerKey,
        has: &Bitfield,
        max: usize,
        now: Instant,
    ) -> Vec<BlockRequest> {
        let mut out = Vec::new();
        if max == 0 {
            return out;
        }

        // 1. Free blocks of pieces already in progress, so pieces finish
        //    instead of piling up half done.
        'partial: for (&index, p) in self.partial.iter_mut() {
            if !has.has(index as usize) {
                continue;
            }
            let size = piece_size(self.total_length, self.piece_length, index as u64) as u32;
            for (b, st) in p.blocks.iter_mut().enumerate() {
                if matches!(st, BlockState::Free) {
                    *st = BlockState::Requested(vec![(peer, now)]);
                    out.push(BlockRequest {
                        index,
                        begin: b as u32 * BLOCK_SIZE,
                        length: block_size(size, b as u32),
                    });
                    if out.len() == max {
                        break 'partial;
                    }
                }
            }
        }

        // 2. Start new pieces chosen by the strategy.
        while out.len() < max && self.partial.len() < self.max_in_progress {
            let state = PieceState {
                status: &self.status,
                availability: &self.availability,
                verified: self.verified,
            };
            let Some(index) = self.strategy.pick(has, &state) else {
                break;
            };
            if self.status.get(index as usize) != Some(&PieceStatus::Missing) {
                break; // A misbehaving strategy; never loop on it.
            }
            self.status[index as usize] = PieceStatus::InProgress;
            self.missing -= 1;
            let size = self.piece_size(index);
            let n = block_count(size) as usize;
            let mut blocks = vec![BlockState::Free; n];
            for (b, st) in blocks.iter_mut().enumerate() {
                if out.len() == max {
                    break;
                }
                *st = BlockState::Requested(vec![(peer, now)]);
                out.push(BlockRequest {
                    index,
                    begin: b as u32 * BLOCK_SIZE,
                    length: block_size(size, b as u32),
                });
            }
            self.partial.insert(
                index,
                Partial {
                    blocks,
                    received: 0,
                },
            );
        }

        // 3. Endgame: ask for blocks already requested from another peer.
        //    Each block gets at most ENDGAME_REQUESTERS, and the oldest
        //    requests go first: they are the likeliest to be stuck. Without
        //    these limits every outstanding block is asked of every peer,
        //    and with deep request queues most of those copies arrive after
        //    the piece is already done.
        if out.len() < max && self.in_endgame() {
            let mut candidates: Vec<(Instant, u32, usize)> = Vec::new();
            for (&index, p) in &self.partial {
                if !has.has(index as usize) {
                    continue;
                }
                for (b, st) in p.blocks.iter().enumerate() {
                    if let BlockState::Requested(list) = st {
                        if list.len() < ENDGAME_REQUESTERS && !list.iter().any(|(k, _)| *k == peer)
                        {
                            let oldest = list.iter().map(|(_, t)| *t).min().unwrap_or(now);
                            candidates.push((oldest, index, b));
                        }
                    }
                }
            }
            candidates.sort_unstable();
            for (_, index, b) in candidates.into_iter().take(max - out.len()) {
                let size = piece_size(self.total_length, self.piece_length, index as u64) as u32;
                if let Some(BlockState::Requested(list)) = self
                    .partial
                    .get_mut(&index)
                    .and_then(|p| p.blocks.get_mut(b))
                {
                    list.push((peer, now));
                    self.endgame_requests += 1;
                    out.push(BlockRequest {
                        index,
                        begin: b as u32 * BLOCK_SIZE,
                        length: block_size(size, b as u32),
                    });
                }
            }
        }

        if !out.is_empty() {
            *self.outstanding.entry(peer).or_insert(0) += out.len();
        }
        out
    }

    /// Records a block that arrived from `peer`.
    pub fn block_received(
        &mut self,
        peer: PeerKey,
        index: u32,
        begin: u32,
        len: u32,
    ) -> BlockOutcome {
        let size = self.piece_size(index);
        let Some(p) = self.partial.get_mut(&index) else {
            return BlockOutcome::Unexpected;
        };
        if !begin.is_multiple_of(BLOCK_SIZE) {
            return BlockOutcome::Unexpected;
        }
        let b = (begin / BLOCK_SIZE) as usize;
        if b >= p.blocks.len() || len != block_size(size, b as u32) {
            return BlockOutcome::Unexpected;
        }
        let requesters = match std::mem::replace(&mut p.blocks[b], BlockState::Received) {
            BlockState::Received => return BlockOutcome::Duplicate,
            BlockState::Free => Vec::new(),
            BlockState::Requested(list) => list,
        };
        p.received += 1;
        let piece_complete = p.received == p.blocks.len();
        let mut cancel = Vec::new();
        for (k, _) in requesters {
            self.dec_outstanding(k);
            if k != peer {
                cancel.push(k);
            }
        }
        BlockOutcome::Accepted {
            piece_complete,
            cancel,
        }
    }

    pub fn piece_verified(&mut self, index: u32) {
        if self.status(index) == Some(PieceStatus::InProgress) {
            self.partial.remove(&index);
            self.status[index as usize] = PieceStatus::Verified;
            self.verified += 1;
        }
    }

    /// The piece failed its hash check: every block becomes free again.
    pub fn piece_failed(&mut self, index: u32) {
        if let Some(p) = self.partial.get_mut(&index) {
            p.blocks.fill(BlockState::Free);
            p.received = 0;
        }
    }

    /// Frees every block requested from `peer` (it choked us or left).
    /// Returns how many requests were dropped.
    pub fn release_peer(&mut self, peer: PeerKey) -> usize {
        let mut n = 0;
        for p in self.partial.values_mut() {
            for b in &mut p.blocks {
                if let BlockState::Requested(list) = b {
                    let before = list.len();
                    list.retain(|(k, _)| *k != peer);
                    n += before - list.len();
                    if list.is_empty() {
                        *b = BlockState::Free;
                    }
                }
            }
        }
        self.outstanding.remove(&peer);
        n
    }

    /// The state of every piece, by index.
    pub fn statuses(&self) -> &[PieceStatus] {
        &self.status
    }

    pub fn endgame_requests(&self) -> u64 {
        self.endgame_requests
    }

    /// Frees requests older than `timeout`. Returns the expired requests.
    pub fn expire(&mut self, now: Instant, timeout: Duration) -> Vec<(PeerKey, BlockRequest)> {
        let mut expired = Vec::new();
        for (&index, p) in self.partial.iter_mut() {
            let size = piece_size(self.total_length, self.piece_length, index as u64) as u32;
            for (b, st) in p.blocks.iter_mut().enumerate() {
                if let BlockState::Requested(list) = st {
                    list.retain(|(k, t)| {
                        let keep = now.saturating_duration_since(*t) < timeout;
                        if !keep {
                            expired.push((
                                *k,
                                BlockRequest {
                                    index,
                                    begin: b as u32 * BLOCK_SIZE,
                                    length: block_size(size, b as u32),
                                },
                            ));
                        }
                        keep
                    });
                    if list.is_empty() {
                        *st = BlockState::Free;
                    }
                }
            }
        }
        for (k, _) in &expired {
            self.dec_outstanding(*k);
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE: u64 = BLOCK_SIZE as u64 * 2;

    fn picker(pieces: usize, strategy: Box<dyn PieceStrategy>) -> Picker {
        Picker::new(
            PIECE * pieces as u64,
            PIECE,
            &Bitfield::new(pieces),
            16,
            strategy,
        )
    }

    fn rng() -> ChaCha8Rng {
        ChaCha8Rng::seed_from_u64(7)
    }

    #[test]
    fn rarest_piece_is_chosen() {
        let mut p = picker(8, Box::new(RarestFirst::new(rng())));
        p.verified = 4; // past the random-first phase
        let full = Bitfield::full(8);
        p.add_peer_pieces(&full);
        p.add_peer_pieces(&full);
        let mut most = Bitfield::full(8);
        most.clear(5);
        p.add_peer_pieces(&most);
        // Piece 5 is held by 2 peers, the rest by 3.
        let reqs = p.next_requests(1, &full, 1, Instant::now());
        assert_eq!(reqs[0].index, 5);
    }

    #[test]
    fn partly_done_pieces_come_first() {
        let mut p = picker(8, Box::new(Sequential));
        let full = Bitfield::full(8);
        let now = Instant::now();
        let first = p.next_requests(1, &full, 1, now);
        assert_eq!(
            first,
            vec![BlockRequest {
                index: 0,
                begin: 0,
                length: BLOCK_SIZE
            }]
        );
        // Peer 2 gets the second block of piece 0 before any new piece.
        let second = p.next_requests(2, &full, 2, now);
        assert_eq!(
            second[0],
            BlockRequest {
                index: 0,
                begin: BLOCK_SIZE,
                length: BLOCK_SIZE
            }
        );
        assert_eq!(second[1].index, 1);
    }

    #[test]
    fn blocks_are_freed_on_choke_and_timeout() {
        let mut p = picker(2, Box::new(Sequential));
        let full = Bitfield::full(2);
        let t0 = Instant::now();
        assert_eq!(p.next_requests(1, &full, 4, t0).len(), 4);
        assert_eq!(p.outstanding(1), 4);
        assert!(p.next_requests(2, &full, 4, t0).len() == 4); // endgame duplicates
        p.release_peer(1);
        assert_eq!(p.outstanding(1), 0);

        let mut p = picker(2, Box::new(Sequential));
        p.next_requests(1, &full, 2, t0);
        let expired = p.expire(t0 + Duration::from_secs(31), Duration::from_secs(30));
        assert_eq!(expired.len(), 2);
        assert_eq!(p.outstanding(1), 0);
        // Now free again for another peer.
        let again = p.next_requests(2, &full, 2, t0);
        assert_eq!(again[0].index, 0);
    }

    #[test]
    fn piece_completion_and_failure() {
        let mut p = picker(1, Box::new(Sequential));
        let full = Bitfield::full(1);
        let reqs = p.next_requests(1, &full, 5, Instant::now());
        assert_eq!(reqs.len(), 2);
        assert_eq!(
            p.block_received(1, 0, 0, BLOCK_SIZE),
            BlockOutcome::Accepted {
                piece_complete: false,
                cancel: vec![]
            }
        );
        assert_eq!(
            p.block_received(1, 0, 0, BLOCK_SIZE),
            BlockOutcome::Duplicate
        );
        assert_eq!(
            p.block_received(1, 0, 3, BLOCK_SIZE),
            BlockOutcome::Unexpected
        );
        assert_eq!(
            p.block_received(1, 0, BLOCK_SIZE, BLOCK_SIZE),
            BlockOutcome::Accepted {
                piece_complete: true,
                cancel: vec![]
            }
        );
        p.piece_failed(0);
        assert_eq!(p.next_requests(2, &full, 5, Instant::now()).len(), 2);
        p.block_received(2, 0, 0, BLOCK_SIZE);
        p.block_received(2, 0, BLOCK_SIZE, BLOCK_SIZE);
        p.piece_verified(0);
        assert!(p.is_complete());
    }

    #[test]
    fn endgame_requests_duplicates_and_cancels() {
        let mut p = picker(1, Box::new(Sequential));
        let full = Bitfield::full(1);
        let now = Instant::now();
        p.next_requests(1, &full, 5, now);
        assert!(p.in_endgame());
        let dup = p.next_requests(2, &full, 5, now);
        assert_eq!(dup.len(), 2);
        assert_eq!(
            p.block_received(2, 0, 0, BLOCK_SIZE),
            BlockOutcome::Accepted {
                piece_complete: false,
                cancel: vec![1]
            }
        );
        assert_eq!(p.outstanding(1), 1);
        assert_eq!(p.outstanding(2), 1);
    }

    #[test]
    fn endgame_limits_requesters_and_prefers_old_requests() {
        // One piece of 4 blocks: two requested long ago, two just now.
        let full = Bitfield::full(1);
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(10);
        let mut p2 = Picker::new(PIECE * 2, PIECE * 2, &Bitfield::new(1), 4, Box::new(Sequential));
        assert_eq!(p2.next_requests(1, &full, 2, t0).len(), 2);
        assert_eq!(p2.next_requests(2, &full, 2, later).len(), 2);
        assert!(p2.in_endgame());
        // Peer 3 gets the two old blocks first (requested at t0 by peer 1).
        let dup = p2.next_requests(3, &full, 2, later);
        assert_eq!(dup.iter().map(|r| r.begin).collect::<Vec<_>>(), vec![0, BLOCK_SIZE]);
        // Each block now has two requesters; peer 4 may take only the two
        // that peer 2 holds alone.
        let dup = p2.next_requests(4, &full, 4, later);
        assert_eq!(
            dup.iter().map(|r| r.begin).collect::<Vec<_>>(),
            vec![2 * BLOCK_SIZE, 3 * BLOCK_SIZE]
        );
        assert!(p2.next_requests(5, &full, 4, later).is_empty());
        assert_eq!(p2.endgame_requests(), 4);
    }

    #[test]
    fn short_last_piece() {
        let total = PIECE + 100;
        let mut p = Picker::new(total, PIECE, &Bitfield::new(2), 16, Box::new(Sequential));
        let full = Bitfield::full(2);
        let reqs = p.next_requests(1, &full, 10, Instant::now());
        assert_eq!(
            reqs.last().unwrap(),
            &BlockRequest {
                index: 1,
                begin: 0,
                length: 100
            }
        );
    }

    #[test]
    fn random_first_four() {
        let mut p = picker(100, Box::new(RarestFirst::new(rng())));
        let full = Bitfield::full(100);
        p.add_peer_pieces(&full);
        let first: Vec<u32> = p
            .next_requests(1, &full, 8, Instant::now())
            .iter()
            .map(|r| r.index)
            .collect();
        // Random choice is very unlikely to give 0,0,1,1,2,2,3,3.
        assert_ne!(first, vec![0, 0, 1, 1, 2, 2, 3, 3]);
    }
}
