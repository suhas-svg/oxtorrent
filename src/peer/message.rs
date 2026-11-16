//! Peer messages and their length-prefixed framing.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use crate::BLOCK_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have { index: u32 },
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, data: Bytes },
    Cancel { index: u32, begin: u32, length: u32 },
}

#[derive(Debug, Error)]
pub enum MessageError {
    #[error("message of {0} bytes exceeds the limit")]
    TooLarge(usize),
    #[error("message id {id} has an invalid payload length {len}")]
    BadLength { id: u8, len: usize },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl Message {
    /// Appends the framed message (length prefix included) to `dst`.
    pub fn encode_to(&self, dst: &mut BytesMut) {
        let header = |dst: &mut BytesMut, len: usize, id: u8| {
            dst.reserve(4 + len);
            dst.put_u32(len as u32);
            dst.put_u8(id);
        };
        match self {
            Message::KeepAlive => dst.put_u32(0),
            Message::Choke => header(dst, 1, 0),
            Message::Unchoke => header(dst, 1, 1),
            Message::Interested => header(dst, 1, 2),
            Message::NotInterested => header(dst, 1, 3),
            Message::Have { index } => {
                header(dst, 5, 4);
                dst.put_u32(*index);
            }
            Message::Bitfield(bits) => {
                header(dst, 1 + bits.len(), 5);
                dst.put_slice(bits);
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                header(dst, 13, 6);
                dst.put_u32(*index);
                dst.put_u32(*begin);
                dst.put_u32(*length);
            }
            Message::Piece { index, begin, data } => {
                header(dst, 9 + data.len(), 7);
                dst.put_u32(*index);
                dst.put_u32(*begin);
                dst.put_slice(data);
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                header(dst, 13, 8);
                dst.put_u32(*index);
                dst.put_u32(*begin);
                dst.put_u32(*length);
            }
        }
    }
}

/// Frames messages on a TCP stream. Returns a message only once all of its
/// bytes have arrived, so messages split across reads decode correctly.
#[derive(Debug, Clone)]
pub struct MessageCodec {
    max_len: usize,
}

impl MessageCodec {
    /// The largest accepted message is a full block or the torrent's
    /// bitfield, whichever is bigger. Anything larger closes the connection.
    pub fn new(piece_count: usize) -> Self {
        let block_msg = 9 + BLOCK_SIZE as usize;
        MessageCodec {
            max_len: block_msg.max(1 + piece_count.div_ceil(8)),
        }
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }
}

fn expect_len(id: u8, len: usize, want: usize) -> Result<(), MessageError> {
    if len == want {
        Ok(())
    } else {
        Err(MessageError::BadLength { id, len })
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = MessageError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, MessageError> {
        loop {
            if src.len() < 4 {
                return Ok(None);
            }
            let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
            if len > self.max_len {
                return Err(MessageError::TooLarge(len));
            }
            if src.len() < 4 + len {
                src.reserve(4 + len - src.len());
                return Ok(None);
            }
            src.advance(4);
            if len == 0 {
                return Ok(Some(Message::KeepAlive));
            }
            let mut p = src.split_to(len);
            let id = p.get_u8();
            let n = p.len();
            let msg = match id {
                0 => expect_len(id, n, 0).map(|_| Message::Choke)?,
                1 => expect_len(id, n, 0).map(|_| Message::Unchoke)?,
                2 => expect_len(id, n, 0).map(|_| Message::Interested)?,
                3 => expect_len(id, n, 0).map(|_| Message::NotInterested)?,
                4 => {
                    expect_len(id, n, 4)?;
                    Message::Have { index: p.get_u32() }
                }
                5 => Message::Bitfield(p.to_vec()),
                6 | 8 => {
                    expect_len(id, n, 12)?;
                    let (index, begin, length) = (p.get_u32(), p.get_u32(), p.get_u32());
                    if id == 6 {
                        Message::Request {
                            index,
                            begin,
                            length,
                        }
                    } else {
                        Message::Cancel {
                            index,
                            begin,
                            length,
                        }
                    }
                }
                7 => {
                    if n < 8 {
                        return Err(MessageError::BadLength { id, len: n });
                    }
                    let index = p.get_u32();
                    let begin = p.get_u32();
                    Message::Piece {
                        index,
                        begin,
                        data: p.freeze(),
                    }
                }
                // Unknown or extension messages (we advertise no extensions):
                // skip them and look at the next frame.
                _ => continue,
            };
            return Ok(Some(msg));
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = MessageError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), MessageError> {
        item.encode_to(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn all_messages() -> Vec<Message> {
        vec![
            Message::KeepAlive,
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have { index: 42 },
            Message::Bitfield(vec![0b1010_0000, 0xff]),
            Message::Request {
                index: 1,
                begin: 16384,
                length: 16384,
            },
            Message::Piece {
                index: 3,
                begin: 0,
                data: Bytes::from_static(b"hello"),
            },
            Message::Cancel {
                index: 1,
                begin: 16384,
                length: 16384,
            },
        ]
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut codec = MessageCodec::new(100);
        for m in all_messages() {
            let mut buf = BytesMut::new();
            m.encode_to(&mut buf);
            assert_eq!(codec.decode(&mut buf).unwrap(), Some(m));
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn split_messages_decode() {
        let mut codec = MessageCodec::new(100);
        let mut all = BytesMut::new();
        for m in all_messages() {
            m.encode_to(&mut all);
        }
        // Feed one byte at a time.
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for b in all.iter() {
            buf.put_u8(*b);
            while let Some(m) = codec.decode(&mut buf).unwrap() {
                out.push(m);
            }
        }
        assert_eq!(out, all_messages());
    }

    #[test]
    fn oversized_length_is_rejected_without_allocating() {
        let mut codec = MessageCodec::new(100);
        let mut buf = BytesMut::from(&[0xff, 0xff, 0xff, 0xf0, 7][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(MessageError::TooLarge(_))
        ));
        assert!(buf.capacity() < 1024);
    }

    #[test]
    fn bad_payload_lengths_are_rejected() {
        let mut codec = MessageCodec::new(100);
        let mut buf = BytesMut::from(&[0, 0, 0, 2, 4, 1][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(MessageError::BadLength { .. })
        ));
    }

    #[test]
    fn unknown_ids_are_skipped() {
        let mut codec = MessageCodec::new(100);
        let mut buf = BytesMut::from(&[0, 0, 0, 3, 9, 0x1a, 0xe1, 0, 0, 0, 1, 1][..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Message::Unchoke));
    }

    proptest! {
        #[test]
        fn random_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..512)) {
            let mut codec = MessageCodec::new(64);
            let mut buf = BytesMut::from(&data[..]);
            while let Ok(Some(_)) = codec.decode(&mut buf) {}
        }
    }
}
