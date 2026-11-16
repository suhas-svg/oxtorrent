//! The per-peer task: connects, handshakes, then turns socket bytes into
//! session events and session commands into socket bytes.

use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::codec::Framed;
use tracing::{debug, trace};

use super::handshake::{self, Handshake, HandshakeError};
use super::message::{Message, MessageCodec, MessageError};
use crate::bitfield::Bitfield;
use crate::session::SessionEvent;
use crate::{PeerKey, BLOCK_SIZE};

/// Commands the session sends to a peer task.
#[derive(Debug)]
pub enum PeerCommand {
    Send(Message),
    Close,
}

pub enum PeerTarget {
    Outgoing(SocketAddr),
    Incoming(TcpStream),
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect: Duration,
    pub handshake: Duration,
    /// Drop a peer that has sent nothing for this long.
    pub idle: Duration,
    /// Send a keep-alive after this long without sending anything.
    pub keepalive: Duration,
    /// Free a requested block that has not arrived after this long.
    pub block: Duration,
    /// Drop a peer that keeps us choked this long while we want its pieces.
    pub choked: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            connect: Duration::from_secs(10),
            handshake: Duration::from_secs(10),
            idle: Duration::from_secs(150),
            keepalive: Duration::from_secs(120),
            block: Duration::from_secs(30),
            choked: Duration::from_secs(90),
        }
    }
}

pub struct PeerParams {
    pub key: PeerKey,
    pub addr: SocketAddr,
    pub info_hash: [u8; 20],
    pub our_id: [u8; 20],
    pub piece_count: usize,
    pub events: mpsc::Sender<SessionEvent>,
    pub timeouts: Timeouts,
}

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("connect timed out")]
    ConnectTimeout,
    #[error("connect failed: {0}")]
    Connect(std::io::Error),
    #[error("handshake timed out")]
    HandshakeTimeout,
    #[error(transparent)]
    Handshake(#[from] HandshakeError),
    #[error(transparent)]
    Message(#[from] MessageError),
    #[error("peer sent nothing for too long")]
    Idle,
    #[error("peer sent an invalid bitfield")]
    BadBitfield,
    #[error("peer requested {0} bytes in one block")]
    RequestTooLarge(u32),
    #[error("peer referred to piece {0}, which does not exist")]
    BadIndex(u32),
    #[error("connection closed by peer")]
    Closed,
    #[error("session is gone")]
    SessionGone,
}

/// Runs one peer connection to completion and always reports
/// [`SessionEvent::PeerDisconnected`] at the end.
pub async fn run(target: PeerTarget, params: PeerParams, cmds: mpsc::Receiver<PeerCommand>) {
    let key = params.key;
    let events = params.events.clone();
    let reason = match run_inner(target, &params, cmds).await {
        Ok(()) => "closed".to_string(),
        Err(e) => e.to_string(),
    };
    debug!(peer = %params.addr, "disconnected: {reason}");
    let _ = events
        .send(SessionEvent::PeerDisconnected { key, reason })
        .await;
}

async fn run_inner(
    target: PeerTarget,
    p: &PeerParams,
    mut cmds: mpsc::Receiver<PeerCommand>,
) -> Result<(), PeerError> {
    let (mut stream, initiator) = match target {
        PeerTarget::Outgoing(addr) => {
            let s = timeout(p.timeouts.connect, TcpStream::connect(addr))
                .await
                .map_err(|_| PeerError::ConnectTimeout)?
                .map_err(PeerError::Connect)?;
            (s, true)
        }
        PeerTarget::Incoming(s) => (s, false),
    };
    let _ = stream.set_nodelay(true);

    let ours = Handshake::new(p.info_hash, p.our_id);
    let theirs = timeout(
        p.timeouts.handshake,
        handshake::exchange(&mut stream, &ours, initiator),
    )
    .await
    .map_err(|_| PeerError::HandshakeTimeout)??;

    p.events
        .send(SessionEvent::PeerConnected {
            key: p.key,
            peer_id: theirs.peer_id,
        })
        .await
        .map_err(|_| PeerError::SessionGone)?;

    let framed = Framed::new(stream, MessageCodec::new(p.piece_count));
    let (mut sink, mut source) = framed.split();
    let mut last_recv = Instant::now();
    let mut last_sent = Instant::now();
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            incoming = source.next() => {
                let msg = match incoming {
                    None => return Err(PeerError::Closed),
                    Some(r) => r?,
                };
                last_recv = Instant::now();
                trace!(peer = %p.addr, "recv {:?}", short(&msg));
                if let Some(msg) = validate(msg, p.piece_count)? {
                    p.events
                        .send(SessionEvent::FromPeer { key: p.key, msg })
                        .await
                        .map_err(|_| PeerError::SessionGone)?;
                }
            }
            cmd = cmds.recv() => {
                let mut batch = Vec::new();
                match cmd {
                    None | Some(PeerCommand::Close) => return Ok(()),
                    Some(PeerCommand::Send(m)) => batch.push(m),
                }
                // Drain whatever else is queued so it goes out in one flush.
                let mut close = false;
                while batch.len() < 64 {
                    match cmds.try_recv() {
                        Ok(PeerCommand::Send(m)) => batch.push(m),
                        Ok(PeerCommand::Close) => { close = true; break; }
                        Err(_) => break,
                    }
                }
                let mut sent_bytes = 0u64;
                for m in batch {
                    if let Message::Piece { data, .. } = &m {
                        sent_bytes += data.len() as u64;
                    }
                    sink.feed(m).await?;
                }
                sink.flush().await?;
                last_sent = Instant::now();
                if sent_bytes > 0 {
                    p.events
                        .send(SessionEvent::BlockSent { key: p.key, bytes: sent_bytes })
                        .await
                        .map_err(|_| PeerError::SessionGone)?;
                }
                if close {
                    return Ok(());
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if now.duration_since(last_recv) > p.timeouts.idle {
                    return Err(PeerError::Idle);
                }
                if now.duration_since(last_sent) > p.timeouts.keepalive {
                    sink.send(Message::KeepAlive).await?;
                    last_sent = now;
                }
            }
        }
    }
}

/// Checks what the peer task can check without the session's state.
/// Returns `None` for messages the session does not need (keep-alives).
fn validate(msg: Message, piece_count: usize) -> Result<Option<Message>, PeerError> {
    let check_index = |i: u32| {
        if (i as usize) < piece_count {
            Ok(())
        } else {
            Err(PeerError::BadIndex(i))
        }
    };
    match &msg {
        Message::KeepAlive => return Ok(None),
        Message::Bitfield(bits) => {
            Bitfield::from_bytes(bits, piece_count).map_err(|_| PeerError::BadBitfield)?;
        }
        Message::Have { index } => check_index(*index)?,
        Message::Request { index, length, .. } => {
            check_index(*index)?;
            if *length > BLOCK_SIZE {
                return Err(PeerError::RequestTooLarge(*length));
            }
        }
        Message::Piece { index, .. } | Message::Cancel { index, .. } => check_index(*index)?,
        _ => {}
    }
    Ok(Some(msg))
}

fn short(m: &Message) -> String {
    match m {
        Message::Piece { index, begin, data } => {
            format!(
                "Piece {{ index: {index}, begin: {begin}, len: {} }}",
                data.len()
            )
        }
        Message::Bitfield(b) => format!("Bitfield({} bytes)", b.len()),
        other => format!("{other:?}"),
    }
}
