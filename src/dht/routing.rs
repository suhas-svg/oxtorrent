//! The Kademlia routing table: up to [`K`] nodes per bucket, where bucket
//! `i` holds nodes whose ID shares exactly `i` leading bits with ours. Close
//! nodes are kept in fine detail and far ones sparsely, so any ID can be
//! found in about log2(network size) steps.

use std::net::SocketAddrV4;
use std::time::Duration;

use tokio::time::Instant;

use super::NodeId;

/// Nodes per bucket, and the number of closest nodes a lookup converges on.
pub const K: usize = 8;
const BUCKETS: usize = 160;
/// A node that has not been heard from for this long may be replaced.
const STALE: Duration = Duration::from_secs(15 * 60);
/// Failed queries before a node may be replaced, and before it is dropped.
const REPLACEABLE_FAILS: u32 = 2;
const DROP_FAILS: u32 = 5;

#[derive(Debug, Clone)]
struct Entry {
    id: NodeId,
    addr: SocketAddrV4,
    last_seen: Instant,
    fails: u32,
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    own: NodeId,
    buckets: Vec<Vec<Entry>>,
}

impl RoutingTable {
    pub fn new(own: NodeId) -> Self {
        RoutingTable {
            own,
            buckets: vec![Vec::new(); BUCKETS],
        }
    }

    pub fn own_id(&self) -> NodeId {
        self.own
    }

    fn bucket_of(&self, id: &NodeId) -> Option<usize> {
        let d = self.own.distance(id);
        let zeros = d.leading_zeros();
        (zeros < BUCKETS as u32).then_some(zeros as usize)
    }

    /// Records that `id` at `addr` answered or sent us a valid message.
    /// Returns whether it is now in the table.
    pub fn heard_from(&mut self, id: NodeId, addr: SocketAddrV4, now: Instant) -> bool {
        let Some(b) = self.bucket_of(&id) else {
            return false; // our own ID
        };
        let bucket = &mut self.buckets[b];
        if let Some(e) = bucket.iter_mut().find(|e| e.id == id) {
            e.addr = addr;
            e.last_seen = now;
            e.fails = 0;
            return true;
        }
        let entry = Entry {
            id,
            addr,
            last_seen: now,
            fails: 0,
        };
        if bucket.len() < K {
            bucket.push(entry);
            return true;
        }
        // Full: replace a node that keeps failing or has gone quiet. Nodes
        // that are still answering are kept, which is what makes the table
        // resist being flooded with new IDs.
        let replace = bucket
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                e.fails >= REPLACEABLE_FAILS || now.duration_since(e.last_seen) > STALE
            })
            .max_by_key(|(_, e)| (e.fails, now.duration_since(e.last_seen)))
            .map(|(i, _)| i);
        match replace {
            Some(i) => {
                bucket[i] = entry;
                true
            }
            None => false,
        }
    }

    /// Records a query to `id` that got no answer.
    pub fn failed(&mut self, id: &NodeId) {
        let Some(b) = self.bucket_of(id) else { return };
        let bucket = &mut self.buckets[b];
        if let Some(i) = bucket.iter().position(|e| e.id == *id) {
            bucket[i].fails += 1;
            if bucket[i].fails >= DROP_FAILS {
                bucket.remove(i);
            }
        }
    }

    /// The `n` known nodes closest to `target`, closest first. Nodes that
    /// have failed recently are left out.
    pub fn closest(&self, target: &NodeId, n: usize) -> Vec<(NodeId, SocketAddrV4)> {
        let mut all: Vec<&Entry> = self
            .buckets
            .iter()
            .flatten()
            .filter(|e| e.fails < REPLACEABLE_FAILS)
            .collect();
        all.sort_by_key(|e| e.id.distance(target));
        all.into_iter().take(n).map(|e| (e.id, e.addr)).collect()
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(n: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, n)
    }

    /// An ID that differs from all-zeros first at bit `bit` (0 = top bit),
    /// with `tag` in the last byte to make it unique.
    fn id_at(bit: usize, tag: u8) -> NodeId {
        let mut b = [0u8; 20];
        b[bit / 8] |= 0x80 >> (bit % 8);
        b[19] |= tag;
        NodeId(b)
    }

    #[test]
    fn buckets_fill_and_keep_live_nodes() {
        let now = Instant::now();
        let mut t = RoutingTable::new(NodeId([0; 20]));
        assert!(!t.heard_from(NodeId([0; 20]), addr(1), now));
        for tag in 0..K as u8 {
            assert!(t.heard_from(id_at(0, tag + 1), addr(tag as u16 + 1), now));
        }
        // Bucket 0 is full of live nodes: a newcomer is turned away.
        assert!(!t.heard_from(id_at(0, 100), addr(100), now));
        // Other buckets are independent.
        assert!(t.heard_from(id_at(40, 1), addr(200), now));
        assert_eq!(t.len(), K + 1);

        // Once a node fails twice, a newcomer may take its place.
        t.failed(&id_at(0, 3));
        t.failed(&id_at(0, 3));
        assert!(t.heard_from(id_at(0, 100), addr(100), now));
        assert_eq!(t.len(), K + 1);
        assert!(!t
            .closest(&id_at(0, 3), 20)
            .iter()
            .any(|(id, _)| *id == id_at(0, 3)));

        // A node that has been silent for a long time can be replaced too.
        let later = now + STALE + Duration::from_secs(1);
        assert!(t.heard_from(id_at(0, 101), addr(101), later));
    }

    #[test]
    fn hearing_again_refreshes_a_node() {
        let now = Instant::now();
        let mut t = RoutingTable::new(NodeId([0; 20]));
        let n = id_at(3, 1);
        t.heard_from(n, addr(1), now);
        t.failed(&n);
        t.failed(&n);
        assert!(t.closest(&n, 1).is_empty());
        t.heard_from(n, addr(2), now);
        assert_eq!(t.closest(&n, 1), vec![(n, addr(2))]);
        for _ in 0..DROP_FAILS {
            t.failed(&n);
        }
        assert!(t.is_empty());
    }

    #[test]
    fn closest_is_sorted_by_xor_distance() {
        let now = Instant::now();
        let mut t = RoutingTable::new(NodeId([0; 20]));
        for bit in [1usize, 5, 9, 30, 100] {
            t.heard_from(id_at(bit, 1), addr(bit as u16), now);
        }
        let target = id_at(9, 0);
        let got: Vec<NodeId> = t
            .closest(&target, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(got[0], id_at(9, 1));
        let d: Vec<_> = got.iter().map(|g| g.distance(&target)).collect();
        assert!(d.windows(2).all(|w| w[0] <= w[1]));
    }
}
