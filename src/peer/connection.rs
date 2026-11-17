//! The per-peer task: connects, handshakes, then turns socket bytes into
//! session events and session commands into socket bytes.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::codec::Framed;
use tracing::{debug, trace};

use super::extension::{self, MetadataMsg};
use super::handshake::{self, Handshake, HandshakeError};
use super::message::{Message, MessageCodec, MessageError};
use crate::bitfield::Bitfield;
use crate::ratelimit::Limits;
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
    /// An incoming connection whose handshake the shared listener has
    /// already read, to pick the torrent.
    Routed(TcpStream, Handshake),
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
    /// The info dictionary, served to peers that ask for it (BEP 9).
    pub metadata: Option<Bytes>,
    /// Speed limits shared by every connection in the run.
    pub limits: Limits,
}

/// Blocks held back by the upload limit for one peer (2 MiB); requests
/// beyond this are dropped and the peer asks again.
const MAX_HELD_UPLOADS: usize = 128;

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
    #[error("peer sent an extension message without enabling extensions")]
    UnexpectedExtension,
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
    let mut ours = Handshake::new(p.info_hash, p.our_id);
    extension::set_extension_bit(&mut ours.reserved);
    let (mut stream, initiator, read) = match target {
        PeerTarget::Outgoing(addr) => {
            let s = timeout(p.timeouts.connect, TcpStream::connect(addr))
                .await
                .map_err(|_| PeerError::ConnectTimeout)?
                .map_err(PeerError::Connect)?;
            (s, true, None)
        }
        PeerTarget::Incoming(s) => (s, false, None),
        PeerTarget::Routed(s, hs) => (s, false, Some(hs)),
    };
    let _ = stream.set_nodelay(true);

    let theirs = match read {
        Some(hs) => timeout(
            p.timeouts.handshake,
            handshake::answer(&mut stream, &ours, hs),
        )
        .await
        .map_err(|_| PeerError::HandshakeTimeout)??,
        None => timeout(
            p.timeouts.handshake,
            handshake::exchange(&mut stream, &ours, initiator),
        )
        .await
        .map_err(|_| PeerError::HandshakeTimeout)??,
    };

    let framed = Framed::new(stream, MessageCodec::new(p.piece_count));
    let (mut sink, mut source) = framed.split();
    let extensions = extension::supports_extensions(&theirs.reserved);
    // The peer's id for ut_metadata messages, once its extended handshake
    // arrives.
    let mut their_ut_metadata: Option<u8> = None;
    if extensions {
        let hs = extension::encode_handshake(p.metadata.as_ref().map(Bytes::len));
        sink.send(Message::Extended {
            id: extension::HANDSHAKE_ID,
            payload: hs,
        })
        .await?;
    }

    p.events
        .send(SessionEvent::PeerConnected {
            key: p.key,
            peer_id: theirs.peer_id,
        })
        .await
        .map_err(|_| PeerError::SessionGone)?;
    let mut last_recv = Instant::now();
    let mut last_sent = Instant::now();
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Download limit: reading pauses until this time.
    let mut read_after: Option<Instant> = None;
    // Upload limit: blocks wait here, and the front one (already charged
    // to the limiter) may go at `send_after`.
    let mut uploads: VecDeque<Message> = VecDeque::new();
    let mut send_after: Option<Instant> = None;
    let far = || Instant::now() + Duration::from_secs(3600);

    loop {
        tokio::select! {
            incoming = source.next(), if read_after.is_none() => {
                let msg = match incoming {
                    None => return Err(PeerError::Closed),
                    Some(r) => r?,
                };
                last_recv = Instant::now();
                trace!(peer = %p.addr, "recv {:?}", short(&msg));
                if let Message::Extended { id, payload } = msg {
                    // Extension messages are handled here; the session never
                    // sees them.
                    if !extensions {
                        return Err(PeerError::UnexpectedExtension);
                    }
                    if let Some(reply) = on_extended(id, &payload, &mut their_ut_metadata, p) {
                        sink.send(reply).await?;
                        last_sent = Instant::now();
                    }
                    continue;
                }
                if let (Message::Piece { data, .. }, Some(limit)) = (&msg, &p.limits.download) {
                    let wait = limit.reserve(data.len());
                    if !wait.is_zero() {
                        read_after = Some(Instant::now() + wait);
                    }
                }
                if let Some(msg) = validate(msg, p.piece_count)? {
                    p.events
                        .send(SessionEvent::FromPeer { key: p.key, msg })
                        .await
                        .map_err(|_| PeerError::SessionGone)?;
                }
            }
            _ = tokio::time::sleep_until(read_after.unwrap_or_else(far)), if read_after.is_some() => {
                read_after = None;
                last_recv = Instant::now();
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
                    match &m {
                        Message::Piece { .. } if p.limits.upload.is_some() => {
                            if uploads.len() < MAX_HELD_UPLOADS {
                                uploads.push_back(m);
                            } else {
                                // The peer asked for more than we hold back;
                                // it will ask again after its timeout.
                                debug!(peer = %p.addr, "upload queue full; dropping a block");
                            }
                            continue;
                        }
                        Message::Piece { data, .. } => sent_bytes += data.len() as u64,
                        _ => {}
                    }
                    sink.feed(m).await?;
                }
                sent_bytes += send_uploads(&mut sink, &mut uploads, &mut send_after, p).await?;
                sink.flush().await?;
                last_sent = Instant::now();
                report_sent(p, sent_bytes).await?;
                if close {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep_until(send_after.unwrap_or_else(far)), if send_after.is_some() => {
                let sent_bytes = send_uploads(&mut sink, &mut uploads, &mut send_after, p).await?;
                sink.flush().await?;
                last_sent = Instant::now();
                report_sent(p, sent_bytes).await?;
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

type PeerSink = futures::stream::SplitSink<Framed<TcpStream, MessageCodec>, Message>;

/// Sends held-back blocks while the upload limit allows, and sets
/// `send_after` for the next one. Returns the block bytes fed to `sink`
/// (the caller flushes).
async fn send_uploads(
    sink: &mut PeerSink,
    uploads: &mut VecDeque<Message>,
    send_after: &mut Option<Instant>,
    p: &PeerParams,
) -> Result<u64, PeerError> {
    let Some(limit) = &p.limits.upload else {
        return Ok(0);
    };
    let mut sent = 0u64;
    while let Some(front) = uploads.front() {
        let now = Instant::now();
        match *send_after {
            // Charged already; wait for its turn.
            Some(t) if t > now => break,
            Some(_) => *send_after = None,
            None => {
                let len = match front {
                    Message::Piece { data, .. } => data.len(),
                    _ => 0,
                };
                let wait = limit.reserve(len);
                if !wait.is_zero() {
                    *send_after = Some(now + wait);
                    break;
                }
            }
        }
        if let Some(m) = uploads.pop_front() {
            if let Message::Piece { data, .. } = &m {
                sent += data.len() as u64;
            }
            sink.feed(m).await?;
        }
    }
    Ok(sent)
}

async fn report_sent(p: &PeerParams, bytes: u64) -> Result<(), PeerError> {
    if bytes > 0 {
        p.events
            .send(SessionEvent::BlockSent { key: p.key, bytes })
            .await
            .map_err(|_| PeerError::SessionGone)?;
    }
    Ok(())
}
/// Handles one extension message and returns the reply to send, if any.
/// Malformed extension messages are ignored rather than fatal: they are
/// optional, and a peer that garbles one may still trade pieces correctly.
fn on_extended(
    id: u8,
    payload: &Bytes,
    their_ut_metadata: &mut Option<u8>,
    p: &PeerParams,
) -> Option<Message> {
    if id == extension::HANDSHAKE_ID {
        match extension::parse_handshake(payload) {
            Ok(h) => {
                debug!(peer = %p.addr, client = ?h.client, "extended handshake");
                *their_ut_metadata = h.ut_metadata;
            }
            Err(e) => debug!(peer = %p.addr, "bad extended handshake: {e}"),
        }
        return None;
    }
    if id != extension::UT_METADATA_ID {
        return None;
    }
    let reply_id = (*their_ut_metadata)?;
    let MetadataMsg::Request { piece } = MetadataMsg::parse(payload).ok()? else {
        return None;
    };
    let reply = match p.metadata.as_ref() {
        Some(info) => match extension::metadata_piece(info, piece) {
            Some(data) => MetadataMsg::Data {
                piece,
                total_size: Some(info.len()),
                data,
            },
            None => MetadataMsg::Reject { piece },
        },
        None => MetadataMsg::Reject { piece },
    };
    Some(Message::Extended {
        id: reply_id,
        payload: reply.encode(),
    })
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
