use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use oxtorrent::peer::message::{Message, MessageCodec};
use oxtorrent::BLOCK_SIZE;
use tokio_util::codec::Decoder;

fn bench(c: &mut Criterion) {
    let block = Bytes::from(vec![7u8; BLOCK_SIZE as usize]);
    let msgs: Vec<Message> = (0..64)
        .map(|i| Message::Piece {
            index: i,
            begin: 0,
            data: block.clone(),
        })
        .collect();
    let mut wire = BytesMut::new();
    for m in &msgs {
        m.encode_to(&mut wire);
    }
    let wire = wire.freeze();

    let mut g = c.benchmark_group("codec");
    g.throughput(Throughput::Bytes(wire.len() as u64));
    g.bench_function("decode_64_piece_messages", |b| {
        b.iter(|| {
            let mut codec = MessageCodec::new(4096);
            let mut buf = BytesMut::from(&wire[..]);
            let mut n = 0;
            while let Some(m) = codec.decode(&mut buf).unwrap() {
                black_box(m);
                n += 1;
            }
            assert_eq!(n, 64);
        })
    });
    g.bench_function("encode_64_piece_messages", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(wire.len());
            for m in &msgs {
                m.encode_to(&mut buf);
            }
            black_box(buf)
        })
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
