use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oxtorrent::bitfield::Bitfield;
use oxtorrent::picker::{BlockOutcome, Picker, RarestFirst};
use oxtorrent::BLOCK_SIZE;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

/// Downloads a whole 4096-piece torrent through the picker from 50 peers:
/// request, receive and verify every block.
fn full_download() {
    let pieces = 4096usize;
    let piece_len = BLOCK_SIZE as u64 * 16; // 256 KiB
    let mut picker = Picker::new(
        pieces as u64 * piece_len,
        piece_len,
        &Bitfield::new(pieces),
        16,
        Box::new(RarestFirst::new(ChaCha8Rng::seed_from_u64(1))),
    );
    let full = Bitfield::full(pieces);
    for _ in 0..50 {
        picker.add_peer_pieces(&full);
    }
    let now = Instant::now();
    let mut peer = 0u64;
    while !picker.is_complete() {
        peer = (peer + 1) % 50;
        let reqs = picker.next_requests(peer, &full, 5, now);
        for r in reqs {
            if let BlockOutcome::Accepted {
                piece_complete: true,
                ..
            } = picker.block_received(peer, r.index, r.begin, r.length)
            {
                picker.piece_verified(r.index);
            }
        }
    }
    black_box(picker.verified_count());
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("picker");
    g.sample_size(10);
    g.bench_function("full_download_4096_pieces_50_peers", |b| {
        b.iter(full_download)
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
