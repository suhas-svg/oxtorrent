use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use oxtorrent::bencode;
use oxtorrent::metainfo::{build_single_file, Torrent};

fn bench(c: &mut Criterion) {
    // A torrent for a 1 GiB file with 256 KiB pieces: 4096 hashes, ~80 KiB.
    let data = vec![0u8; 1024 * 1024];
    let small = build_single_file("x", &data, 256 * 1024, Some("http://t.example/announce"));
    let t = Torrent::from_bytes(&small).unwrap();
    let mut big_info = Vec::new();
    for _ in 0..4096 {
        big_info.extend_from_slice(&t.piece_hashes[0]);
    }
    let raw = format!(
        "d8:announce25:http://t.example/announce4:infod6:lengthi{}e4:name1:x12:piece lengthi262144e6:pieces{}:",
        4096u64 * 262144,
        big_info.len()
    );
    let mut raw = raw.into_bytes();
    raw.extend_from_slice(&big_info);
    raw.extend_from_slice(b"ee");

    let mut g = c.benchmark_group("bencode");
    g.throughput(Throughput::Bytes(raw.len() as u64));
    g.bench_function("decode_torrent_4096_pieces", |b| {
        b.iter(|| bencode::decode(black_box(&raw)).unwrap())
    });
    g.bench_function("parse_metainfo_4096_pieces", |b| {
        b.iter(|| Torrent::from_bytes(black_box(&raw)).unwrap())
    });
    let v = bencode::decode(&raw).unwrap();
    g.bench_function("encode_torrent_4096_pieces", |b| {
        b.iter(|| bencode::encode(black_box(&v)))
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
