//! The extension protocol (BEP 10) and metadata exchange (BEP 9).
//!
//! Peers that set bit 20 of the handshake's reserved bytes can send message
//! 20. Its first byte says which extension: 0 is the extended handshake, a
//! dictionary whose `m` entry maps extension names to the ids the sender
//! wants to receive them on. The only extension used here is
//! `ut_metadata`, which transfers the torrent's info dictionary in 16 KiB
//! pieces so a download can start from a magnet link.

use bytes::Bytes;
use thiserror::Error;

use crate::bencode::{self, Value};

/// Byte and mask of the extension-protocol bit in the reserved bytes.
const RESERVED_BYTE: usize = 5;
const RESERVED_MASK: u8 = 0x10;

/// Extended message id of the handshake.
pub const HANDSHAKE_ID: u8 = 0;
/// The id we ask peers to use when sending us `ut_metadata` messages.
pub const UT_METADATA_ID: u8 = 1;
/// Metadata is exchanged in pieces of this size (the last may be shorter).
pub const METADATA_PIECE: usize = 16 * 1024;
/// Largest info dictionary accepted from a peer. A 4096-piece torrent's is
/// about 80 KiB; this allows torrents with roughly 800,000 pieces.
pub const MAX_METADATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExtensionError {
    #[error("extension message is not valid bencode")]
    Bencode,
    #[error("extension message has an invalid `{0}` field")]
    Invalid(&'static str),
}

pub fn set_extension_bit(reserved: &mut [u8; 8]) {
    reserved[RESERVED_BYTE] |= RESERVED_MASK;
}

pub fn supports_extensions(reserved: &[u8; 8]) -> bool {
    reserved[RESERVED_BYTE] & RESERVED_MASK != 0
}

/// What we need from a peer's extended handshake.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtHandshake {
    /// The id the peer wants `ut_metadata` messages on; `None` if it does
    /// not support the extension (or turned it off with id 0).
    pub ut_metadata: Option<u8>,
    /// Size of the info dictionary, if the peer has it.
    pub metadata_size: Option<usize>,
    /// Client name and version (`v`), for logs.
    pub client: Option<String>,
}

/// Our extended handshake. `metadata_size` is set when we have the info
/// dictionary and can serve it.
pub fn encode_handshake(metadata_size: Option<usize>) -> Bytes {
    let mut entries = vec![
        (
            "m",
            Value::dict([("ut_metadata", Value::Int(UT_METADATA_ID as i64))]),
        ),
        (
            "v",
            Value::str(concat!("Oxtorrent ", env!("CARGO_PKG_VERSION"))),
        ),
    ];
    if let Some(size) = metadata_size {
        entries.push(("metadata_size", Value::Int(size as i64)));
    }
    Bytes::from(bencode::encode(&Value::dict(entries)))
}

pub fn parse_handshake(payload: &[u8]) -> Result<ExtHandshake, ExtensionError> {
    let (v, _) = bencode::decode_prefix_lenient(payload).map_err(|_| ExtensionError::Bencode)?;
    if v.as_dict().is_none() {
        return Err(ExtensionError::Invalid("handshake"));
    }
    let ut_metadata = match v.get("m").and_then(|m| m.get("ut_metadata")) {
        None => None,
        Some(id) => match id.as_int() {
            Some(0) => None,
            Some(n) => Some(u8::try_from(n).map_err(|_| ExtensionError::Invalid("m"))?),
            None => return Err(ExtensionError::Invalid("m")),
        },
    };
    let metadata_size = match v.get("metadata_size") {
        None => None,
        Some(s) => Some(
            s.as_int()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or(ExtensionError::Invalid("metadata_size"))?,
        ),
    };
    let client = v.get("v").and_then(Value::as_str).map(str::to_string);
    Ok(ExtHandshake {
        ut_metadata,
        metadata_size,
        client,
    })
}

/// A `ut_metadata` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataMsg {
    Request {
        piece: u32,
    },
    Data {
        piece: u32,
        total_size: Option<usize>,
        data: Bytes,
    },
    Reject {
        piece: u32,
    },
}

impl MetadataMsg {
    pub fn encode(&self) -> Bytes {
        let head = |t: i64, piece: u32| {
            vec![
                ("msg_type", Value::Int(t)),
                ("piece", Value::Int(piece as i64)),
            ]
        };
        match self {
            MetadataMsg::Request { piece } => {
                Bytes::from(bencode::encode(&Value::dict(head(0, *piece))))
            }
            MetadataMsg::Reject { piece } => {
                Bytes::from(bencode::encode(&Value::dict(head(2, *piece))))
            }
            MetadataMsg::Data {
                piece,
                total_size,
                data,
            } => {
                let mut d = head(1, *piece);
                if let Some(t) = total_size {
                    d.push(("total_size", Value::Int(*t as i64)));
                }
                let mut out = bencode::encode(&Value::dict(d));
                out.extend_from_slice(data);
                Bytes::from(out)
            }
        }
    }

    /// Parses a `ut_metadata` payload. For data messages the piece bytes
    /// follow the dictionary.
    pub fn parse(payload: &Bytes) -> Result<MetadataMsg, ExtensionError> {
        let (v, used) =
            bencode::decode_prefix_lenient(payload).map_err(|_| ExtensionError::Bencode)?;
        let int = |k: &'static str| v.get(k).and_then(Value::as_int);
        let piece = int("piece")
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(ExtensionError::Invalid("piece"))?;
        match int("msg_type") {
            Some(0) => Ok(MetadataMsg::Request { piece }),
            Some(1) => {
                let total_size = match v.get("total_size") {
                    None => None,
                    Some(t) => Some(
                        t.as_int()
                            .and_then(|n| usize::try_from(n).ok())
                            .ok_or(ExtensionError::Invalid("total_size"))?,
                    ),
                };
                Ok(MetadataMsg::Data {
                    piece,
                    total_size,
                    data: payload.slice(used..),
                })
            }
            Some(2) => Ok(MetadataMsg::Reject { piece }),
            _ => Err(ExtensionError::Invalid("msg_type")),
        }
    }
}

/// Number of metadata pieces for an info dictionary of `size` bytes.
pub fn metadata_pieces(size: usize) -> usize {
    size.div_ceil(METADATA_PIECE)
}

/// The bytes of metadata piece `piece`, or `None` if it is out of range.
pub fn metadata_piece(info: &Bytes, piece: u32) -> Option<Bytes> {
    let start = (piece as usize).checked_mul(METADATA_PIECE)?;
    if start >= info.len() {
        return None;
    }
    let end = (start + METADATA_PIECE).min(info.len());
    Some(info.slice(start..end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_bit() {
        let mut r = [0u8; 8];
        assert!(!supports_extensions(&r));
        set_extension_bit(&mut r);
        assert_eq!(r, [0, 0, 0, 0, 0, 0x10, 0, 0]);
        assert!(supports_extensions(&r));
    }

    #[test]
    fn handshake_round_trip() {
        let ours = encode_handshake(Some(12345));
        let h = parse_handshake(&ours).unwrap();
        assert_eq!(h.ut_metadata, Some(UT_METADATA_ID));
        assert_eq!(h.metadata_size, Some(12345));
        assert!(h.client.unwrap().starts_with("Oxtorrent "));

        let none = parse_handshake(&encode_handshake(None)).unwrap();
        assert_eq!(none.metadata_size, None);
    }

    #[test]
    fn handshake_from_other_clients() {
        // Unsorted keys, extra fields, and ut_metadata turned off.
        let h = parse_handshake(b"d1:v4:Test1:md6:ut_pexi1e11:ut_metadatai3eee").unwrap();
        assert_eq!(h.ut_metadata, Some(3));
        assert_eq!(h.client.as_deref(), Some("Test"));
        let off = parse_handshake(b"d1:md11:ut_metadatai0eee").unwrap();
        assert_eq!(off.ut_metadata, None);
        assert!(parse_handshake(b"d1:md11:ut_metadatai300eee").is_err());
        assert!(parse_handshake(b"le").is_err());
        assert!(parse_handshake(b"garbage").is_err());
    }

    #[test]
    fn metadata_messages_round_trip() {
        for m in [
            MetadataMsg::Request { piece: 3 },
            MetadataMsg::Reject { piece: 0 },
            MetadataMsg::Data {
                piece: 1,
                total_size: Some(20000),
                data: Bytes::from_static(b"\x00\x01raw bytes e"),
            },
        ] {
            assert_eq!(MetadataMsg::parse(&m.encode()).unwrap(), m);
        }
        // The wire format from BEP 9.
        assert_eq!(
            &MetadataMsg::Request { piece: 0 }.encode()[..],
            b"d8:msg_typei0e5:piecei0ee"
        );
        assert!(MetadataMsg::parse(&Bytes::from_static(b"d8:msg_typei7e5:piecei0ee")).is_err());
        assert!(MetadataMsg::parse(&Bytes::from_static(b"d8:msg_typei0ee")).is_err());
    }

    #[test]
    fn metadata_pieces_split_the_info_dict() {
        let info = Bytes::from(vec![7u8; METADATA_PIECE * 2 + 5]);
        assert_eq!(metadata_pieces(info.len()), 3);
        assert_eq!(metadata_piece(&info, 0).unwrap().len(), METADATA_PIECE);
        assert_eq!(metadata_piece(&info, 2).unwrap().len(), 5);
        assert!(metadata_piece(&info, 3).is_none());
        assert!(metadata_piece(&info, u32::MAX).is_none());
    }
}
