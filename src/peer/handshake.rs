//! The fixed 68-byte opening exchange.

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 68;

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("peer does not speak the BitTorrent protocol")]
    BadProtocol,
    #[error("peer is serving a different torrent")]
    InfoHashMismatch,
    #[error("connected to ourselves")]
    SelfConnection,
    #[error("I/O error during handshake: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Handshake {
            reserved: [0; 8],
            info_hash,
            peer_id,
        }
    }

    pub fn to_bytes(&self) -> [u8; HANDSHAKE_LEN] {
        let mut b = [0u8; HANDSHAKE_LEN];
        b[0] = 19;
        b[1..20].copy_from_slice(PROTOCOL);
        b[20..28].copy_from_slice(&self.reserved);
        b[28..48].copy_from_slice(&self.info_hash);
        b[48..68].copy_from_slice(&self.peer_id);
        b
    }

    pub fn parse(b: &[u8; HANDSHAKE_LEN]) -> Result<Self, HandshakeError> {
        if b[0] != 19 || &b[1..20] != PROTOCOL {
            return Err(HandshakeError::BadProtocol);
        }
        let mut h = Handshake::new([0; 20], [0; 20]);
        h.reserved.copy_from_slice(&b[20..28]);
        h.info_hash.copy_from_slice(&b[28..48]);
        h.peer_id.copy_from_slice(&b[48..68]);
        Ok(h)
    }
}

/// Performs the handshake. The side that opened the connection sends first;
/// the accepting side checks the info hash before answering.
pub async fn exchange<S>(
    stream: &mut S,
    ours: &Handshake,
    initiator: bool,
) -> Result<Handshake, HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if initiator {
        stream.write_all(&ours.to_bytes()).await?;
    }
    let mut buf = [0u8; HANDSHAKE_LEN];
    stream.read_exact(&mut buf).await?;
    let theirs = Handshake::parse(&buf)?;
    if theirs.info_hash != ours.info_hash {
        return Err(HandshakeError::InfoHashMismatch);
    }
    if theirs.peer_id == ours.peer_id {
        return Err(HandshakeError::SelfConnection);
    }
    if !initiator {
        stream.write_all(&ours.to_bytes()).await?;
    }
    Ok(theirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let h = Handshake::new([7; 20], *b"-OX0001-123456789012");
        let b = h.to_bytes();
        assert_eq!(b.len(), 68);
        assert_eq!(b[0], 19);
        assert_eq!(Handshake::parse(&b).unwrap(), h);
    }

    #[tokio::test]
    async fn exchange_checks_info_hash() {
        let (mut a, mut b) = tokio::io::duplex(256);
        let ha = Handshake::new([1; 20], [b'a'; 20]);
        let hb = Handshake::new([2; 20], [b'b'; 20]);
        let responder = async move {
            // Dropping `b` closes the stream, as the real peer task does.
            exchange(&mut b, &hb, false).await
        };
        let (ra, rb) = tokio::join!(exchange(&mut a, &ha, true), responder);
        assert!(matches!(rb, Err(HandshakeError::InfoHashMismatch)));
        assert!(ra.is_err());
    }

    #[tokio::test]
    async fn exchange_succeeds() {
        let (mut a, mut b) = tokio::io::duplex(256);
        let ha = Handshake::new([1; 20], [b'a'; 20]);
        let hb = Handshake::new([1; 20], [b'b'; 20]);
        let (ra, rb) = tokio::join!(exchange(&mut a, &ha, true), exchange(&mut b, &hb, false));
        assert_eq!(ra.unwrap().peer_id, [b'b'; 20]);
        assert_eq!(rb.unwrap().peer_id, [b'a'; 20]);
    }
}
