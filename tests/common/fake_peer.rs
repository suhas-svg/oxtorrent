//! A fake peer that speaks the wire protocol by hand (independently of the
//! client's own codec) and serves a generated file, with a behaviour knob.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Behaviour {
    Honest,
    /// The first block it sends is corrupt.
    CorruptOnce,
    /// Every block it sends is corrupt.
    AlwaysCorrupt,
    /// Closes the connection after sending this many blocks.
    DisconnectAfter(usize),
    NeverUnchoke,
    /// Sends a 2 GB length prefix right after the handshake.
    OversizedLength,
}

#[derive(Default)]
pub struct FakeStats {
    pub connections: AtomicUsize,
    pub blocks_sent: AtomicUsize,
    pub closed: AtomicUsize,
    corrupted_once: AtomicBool,
}

pub struct FakePeer {
    pub addr: SocketAddr,
    pub stats: Arc<FakeStats>,
}

pub async fn spawn(
    data: Arc<Vec<u8>>,
    piece_length: u64,
    info_hash: [u8; 20],
    behaviour: Behaviour,
) -> FakePeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stats = Arc::new(FakeStats::default());
    let st = stats.clone();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                break;
            };
            st.connections.fetch_add(1, Ordering::SeqCst);
            let (data, st) = (data.clone(), st.clone());
            tokio::spawn(async move {
                let _ = serve(
                    s,
                    &data,
                    piece_length,
                    info_hash,
                    behaviour,
                    &st,
                    addr.port(),
                )
                .await;
                st.closed.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    FakePeer { addr, stats }
}

async fn serve(
    mut s: TcpStream,
    data: &[u8],
    piece_length: u64,
    info_hash: [u8; 20],
    behaviour: Behaviour,
    st: &FakeStats,
    port: u16,
) -> std::io::Result<()> {
    let mut hs = [0u8; 68];
    s.read_exact(&mut hs).await?;
    if hs[28..48] != info_hash {
        return Ok(());
    }
    let mut reply = vec![19u8];
    reply.extend_from_slice(b"BitTorrent protocol");
    reply.extend_from_slice(&[0; 8]);
    reply.extend_from_slice(&info_hash);
    let mut id = format!("-FK0001-{port:012}").into_bytes();
    id.truncate(20);
    reply.extend_from_slice(&id);
    s.write_all(&reply).await?;

    if behaviour == Behaviour::OversizedLength {
        s.write_all(&[0x7f, 0xff, 0xff, 0xff, 7]).await?;
        // Wait for the client to hang up.
        let mut sink = [0u8; 1024];
        while s.read(&mut sink).await? > 0 {}
        return Ok(());
    }

    // Bitfield: everything.
    let pieces = (data.len() as u64).div_ceil(piece_length) as usize;
    let mut bits = vec![0u8; pieces.div_ceil(8)];
    for i in 0..pieces {
        bits[i / 8] |= 0x80 >> (i % 8);
    }
    let mut msg = ((bits.len() + 1) as u32).to_be_bytes().to_vec();
    msg.push(5);
    msg.extend_from_slice(&bits);
    s.write_all(&msg).await?;

    loop {
        let mut len = [0u8; 4];
        s.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 {
            continue;
        }
        if len > 1 << 20 {
            return Ok(());
        }
        let mut payload = vec![0u8; len];
        s.read_exact(&mut payload).await?;
        match payload[0] {
            2 if behaviour != Behaviour::NeverUnchoke => {
                s.write_all(&[0, 0, 0, 1, 1]).await?;
            }
            6 if payload.len() == 13 => {
                if let Behaviour::DisconnectAfter(n) = behaviour {
                    if st.blocks_sent.load(Ordering::SeqCst) >= n {
                        return Ok(());
                    }
                }
                let index = u32::from_be_bytes(payload[1..5].try_into().unwrap()) as u64;
                let begin = u32::from_be_bytes(payload[5..9].try_into().unwrap()) as u64;
                let length = u32::from_be_bytes(payload[9..13].try_into().unwrap()) as u64;
                let start = (index * piece_length + begin) as usize;
                let mut block = data[start..start + length as usize].to_vec();
                let corrupt = match behaviour {
                    Behaviour::AlwaysCorrupt => true,
                    Behaviour::CorruptOnce => !st.corrupted_once.swap(true, Ordering::SeqCst),
                    _ => false,
                };
                if corrupt {
                    block[0] ^= 0xff;
                }
                let mut out = ((9 + block.len()) as u32).to_be_bytes().to_vec();
                out.push(7);
                out.extend_from_slice(&(index as u32).to_be_bytes());
                out.extend_from_slice(&(begin as u32).to_be_bytes());
                out.extend_from_slice(&block);
                s.write_all(&out).await?;
                st.blocks_sent.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}
