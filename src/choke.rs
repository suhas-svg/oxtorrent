//! Upload slot decisions (choking). Pure logic with no I/O.

use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::PeerKey;

/// Alias used by the strategy traits.
pub type PeerId = PeerKey;

#[derive(Clone, Debug, PartialEq)]
pub struct PeerStats {
    pub id: PeerId,
    /// The peer is interested in our pieces.
    pub interested: bool,
    /// Bytes per second the peer sent us recently.
    pub download_rate: f64,
    /// Bytes per second we sent the peer recently.
    pub upload_rate: f64,
}

/// Decides which peers get upload slots. Called every 10 seconds.
pub trait ChokeStrategy: Send + Sync {
    fn choose_unchoked(&mut self, peers: &[PeerStats], now: Instant) -> Vec<PeerId>;

    /// Called once the local download is complete.
    fn set_seeding(&mut self, _seeding: bool) {}

    /// Total upload slots, used to fill free slots between rounds.
    fn slots(&self) -> usize;
}

/// Tit-for-tat: unchoke the fastest uploaders plus one optimistic slot that
/// rotates every 30 seconds. When seeding, rank by our upload rate instead.
pub struct TitForTat {
    regular: usize,
    optimistic: Option<PeerId>,
    last_rotation: Option<Instant>,
    rotate_every: Duration,
    seeding: bool,
    rng: ChaCha8Rng,
}

impl TitForTat {
    /// `slots` is the total including the optimistic slot (default 4).
    pub fn new(slots: usize, rng: ChaCha8Rng) -> Self {
        TitForTat {
            regular: slots.saturating_sub(1),
            optimistic: None,
            last_rotation: None,
            rotate_every: Duration::from_secs(30),
            seeding: false,
            rng,
        }
    }

    pub fn from_entropy(slots: usize) -> Self {
        Self::new(slots, ChaCha8Rng::from_entropy())
    }
}

impl ChokeStrategy for TitForTat {
    fn choose_unchoked(&mut self, peers: &[PeerStats], now: Instant) -> Vec<PeerId> {
        let mut interested: Vec<&PeerStats> = peers.iter().filter(|p| p.interested).collect();
        let seeding = self.seeding;
        let rate = |p: &PeerStats| {
            if seeding {
                p.upload_rate
            } else {
                p.download_rate
            }
        };
        interested.sort_by(|a, b| rate(b).total_cmp(&rate(a)).then(a.id.cmp(&b.id)));

        let mut chosen: Vec<PeerId> = interested.iter().take(self.regular).map(|p| p.id).collect();

        let optimistic_valid = self
            .optimistic
            .is_some_and(|o| !chosen.contains(&o) && interested.iter().any(|p| p.id == o));
        let due = self
            .last_rotation
            .is_none_or(|t| now.saturating_duration_since(t) >= self.rotate_every);
        if due || !optimistic_valid {
            let rest: Vec<PeerId> = interested
                .iter()
                .map(|p| p.id)
                .filter(|id| !chosen.contains(id))
                .collect();
            self.optimistic = rest.choose(&mut self.rng).copied();
            self.last_rotation = Some(now);
        }
        if let Some(o) = self.optimistic {
            chosen.push(o);
        }
        chosen
    }

    fn set_seeding(&mut self, seeding: bool) {
        self.seeding = seeding;
    }

    fn slots(&self) -> usize {
        self.regular + 1
    }
}

/// Random: a fixed number of random interested peers every round.
pub struct RandomChoke {
    slots: usize,
    rng: ChaCha8Rng,
}

impl RandomChoke {
    pub fn new(slots: usize, rng: ChaCha8Rng) -> Self {
        RandomChoke { slots, rng }
    }
}

impl ChokeStrategy for RandomChoke {
    fn choose_unchoked(&mut self, peers: &[PeerStats], _now: Instant) -> Vec<PeerId> {
        let mut ids: Vec<PeerId> = peers
            .iter()
            .filter(|p| p.interested)
            .map(|p| p.id)
            .collect();
        ids.shuffle(&mut self.rng);
        ids.truncate(self.slots);
        ids
    }

    fn slots(&self) -> usize {
        self.slots
    }
}

/// No reward: upload to every interested peer equally.
pub struct NoReward;

impl ChokeStrategy for NoReward {
    fn choose_unchoked(&mut self, peers: &[PeerStats], _now: Instant) -> Vec<PeerId> {
        peers
            .iter()
            .filter(|p| p.interested)
            .map(|p| p.id)
            .collect()
    }

    fn slots(&self) -> usize {
        usize::MAX
    }
}

/// Helper for tests and the simulator: a seeded generator from a parent.
pub fn child_rng(parent: &mut impl Rng) -> ChaCha8Rng {
    ChaCha8Rng::seed_from_u64(parent.gen())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(id: u64, interested: bool, down: f64) -> PeerStats {
        PeerStats {
            id,
            interested,
            download_rate: down,
            upload_rate: 0.0,
        }
    }

    #[test]
    fn three_fastest_are_unchoked() {
        let mut c = TitForTat::new(4, ChaCha8Rng::seed_from_u64(1));
        let peers = vec![
            stats(1, true, 10.0),
            stats(2, true, 50.0),
            stats(3, false, 999.0),
            stats(4, true, 30.0),
            stats(5, true, 40.0),
            stats(6, true, 5.0),
        ];
        let chosen = c.choose_unchoked(&peers, Instant::now());
        assert_eq!(chosen.len(), 4);
        assert_eq!(&chosen[..3], &[2, 5, 4]);
        assert!([1, 6].contains(&chosen[3]));
        assert!(!chosen.contains(&3));
    }

    #[test]
    fn optimistic_slot_rotates() {
        let mut c = TitForTat::new(2, ChaCha8Rng::seed_from_u64(3));
        let peers: Vec<PeerStats> = (1..=20)
            .map(|i| stats(i, true, if i == 1 { 100.0 } else { 0.0 }))
            .collect();
        let t0 = Instant::now();
        let first = c.choose_unchoked(&peers, t0)[1];
        // Within 30 s the optimistic peer stays.
        assert_eq!(
            c.choose_unchoked(&peers, t0 + Duration::from_secs(10))[1],
            first
        );
        assert_eq!(
            c.choose_unchoked(&peers, t0 + Duration::from_secs(20))[1],
            first
        );
        // Over several rotations it changes at least once.
        let mut seen = std::collections::HashSet::new();
        for k in 1..10 {
            seen.insert(c.choose_unchoked(&peers, t0 + Duration::from_secs(30 * k))[1]);
        }
        assert!(seen.len() > 1);
    }

    #[test]
    fn seeding_ranks_by_upload_rate() {
        let mut c = TitForTat::new(2, ChaCha8Rng::seed_from_u64(1));
        c.set_seeding(true);
        let peers = vec![
            PeerStats {
                id: 1,
                interested: true,
                download_rate: 100.0,
                upload_rate: 1.0,
            },
            PeerStats {
                id: 2,
                interested: true,
                download_rate: 0.0,
                upload_rate: 50.0,
            },
        ];
        assert_eq!(c.choose_unchoked(&peers, Instant::now())[0], 2);
    }
}
