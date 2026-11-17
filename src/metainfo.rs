//! Turns a parsed `.torrent` file into a typed [`Torrent`] and computes its
//! info hash. Also holds the piece and block size arithmetic.

use std::path::PathBuf;

use bytes::Bytes;
use sha1::{Digest, Sha1};
use thiserror::Error;

use crate::bencode::{self, BencodeError, Value};
use crate::BLOCK_SIZE;

/// Largest piece length accepted. Pieces are buffered in memory, so this
/// bounds memory use (16 pieces in progress at most).
pub const MAX_PIECE_LENGTH: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum MetainfoError {
    #[error("not valid bencode: {0}")]
    Bencode(#[from] BencodeError),
    #[error("missing field `{0}`")]
    Missing(&'static str),
    #[error("field `{0}` has the wrong type or an invalid value")]
    Invalid(&'static str),
    #[error("unsafe path component {0:?} in field `{1}`")]
    UnsafePath(String, &'static str),
    #[error("field `info.pieces` is {0} bytes, which is not a multiple of 20")]
    PiecesLength(usize),
    #[error("field `info.pieces` holds {got} hashes but the total size needs {expected}")]
    PieceCount { got: u64, expected: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Path relative to the output folder (includes the torrent's folder name
    /// for multi-file torrents).
    pub path: PathBuf,
    pub length: u64,
    /// Position of the file's first byte in the torrent's byte stream.
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct Torrent {
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub name: String,
    pub piece_length: u64,
    pub piece_hashes: Vec<[u8; 20]>,
    pub files: Vec<FileEntry>,
    pub total_length: u64,
    pub info_hash: [u8; 20],
    pub multi_file: bool,
    /// `info.private = 1` (BEP 27): peers come only from the trackers, never
    /// from the DHT.
    pub private: bool,
    /// The exact bytes of the `info` dictionary, served to peers that fetch
    /// metadata from us (BEP 9).
    pub info_bytes: Bytes,
}

impl Torrent {
    pub fn from_bytes(data: &[u8]) -> Result<Torrent, MetainfoError> {
        let (root, spans) = bencode::decode_with_spans(data)?;
        let info_span = spans
            .iter()
            .find(|(k, _)| k == b"info")
            .map(|(_, r)| r.clone())
            .ok_or(MetainfoError::Missing("info"))?;
        let info_bytes = Bytes::copy_from_slice(&data[info_span]);
        let info_hash: [u8; 20] = Sha1::digest(&info_bytes).into();
        let info = root.get("info").ok_or(MetainfoError::Missing("info"))?;
        if info.as_dict().is_none() {
            return Err(MetainfoError::Invalid("info"));
        }

        let announce = match root.get("announce") {
            None => None,
            Some(v) => Some(
                v.as_str()
                    .ok_or(MetainfoError::Invalid("announce"))?
                    .to_string(),
            ),
        };
        let announce_list = parse_announce_list(root.get("announce-list"))?;

        let name = info
            .get("name")
            .ok_or(MetainfoError::Missing("info.name"))?
            .as_str()
            .ok_or(MetainfoError::Invalid("info.name"))?
            .to_string();
        check_component(&name, "info.name")?;

        let piece_length = info
            .get("piece length")
            .ok_or(MetainfoError::Missing("info.piece length"))?
            .as_int()
            .filter(|&n| n > 0 && n as u64 <= MAX_PIECE_LENGTH)
            .ok_or(MetainfoError::Invalid("info.piece length"))? as u64;

        let pieces = info
            .get("pieces")
            .ok_or(MetainfoError::Missing("info.pieces"))?
            .as_bytes()
            .ok_or(MetainfoError::Invalid("info.pieces"))?;
        if pieces.len() % 20 != 0 {
            return Err(MetainfoError::PiecesLength(pieces.len()));
        }
        let piece_hashes: Vec<[u8; 20]> = pieces.as_chunks::<20>().0.to_vec();

        let (files, multi_file) = match (info.get("length"), info.get("files")) {
            (Some(len), None) => {
                let length = non_negative(len, "info.length")?;
                (
                    vec![FileEntry {
                        path: PathBuf::from(&name),
                        length,
                        offset: 0,
                    }],
                    false,
                )
            }
            (None, Some(list)) => (parse_files(&name, list)?, true),
            (None, None) => return Err(MetainfoError::Missing("info.length")),
            (Some(_), Some(_)) => return Err(MetainfoError::Invalid("info.files")),
        };

        let total_length = files
            .iter()
            .try_fold(0u64, |acc, f| acc.checked_add(f.length))
            .ok_or(MetainfoError::Invalid("info.length"))?;
        if total_length == 0 {
            return Err(MetainfoError::Invalid("info.length"));
        }
        let expected = piece_count(total_length, piece_length);
        if expected != piece_hashes.len() as u64 || expected > u32::MAX as u64 {
            return Err(MetainfoError::PieceCount {
                got: piece_hashes.len() as u64,
                expected,
            });
        }

        Ok(Torrent {
            announce,
            announce_list,
            name,
            piece_length,
            piece_hashes,
            files,
            total_length,
            info_hash,
            multi_file,
            private: info.get("private").and_then(Value::as_int) == Some(1),
            info_bytes,
        })
    }

    /// Builds a torrent from a bare `info` dictionary, as fetched from peers
    /// for a magnet link, plus the tracker URLs to use.
    pub fn from_info(info: &[u8], trackers: &[String]) -> Result<Torrent, MetainfoError> {
        let tiers: Vec<Vec<String>> = trackers.iter().map(|t| vec![t.clone()]).collect();
        Torrent::from_bytes(&torrent_file(info, &tiers))
    }

    /// A `.torrent` file for this torrent: its trackers (as an announce
    /// list) and the original info dictionary bytes.
    pub fn to_file_bytes(&self) -> Vec<u8> {
        let mut tiers = self.announce_list.clone();
        if tiers.is_empty() {
            tiers.extend(self.announce.iter().map(|a| vec![a.clone()]));
        }
        torrent_file(&self.info_bytes, &tiers)
    }

    pub fn piece_count(&self) -> usize {
        self.piece_hashes.len()
    }

    pub fn piece_size(&self, index: u32) -> u32 {
        piece_size(self.total_length, self.piece_length, index as u64) as u32
    }

    pub fn piece_offset(&self, index: u32) -> u64 {
        index as u64 * self.piece_length
    }

    /// Tracker URLs in the order to try them: the announce-list tiers if
    /// present (BEP 12), otherwise the single announce URL.
    pub fn trackers(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for url in self
            .announce_list
            .iter()
            .flatten()
            .chain(self.announce.iter())
        {
            if !out.contains(url) {
                out.push(url.clone());
            }
        }
        out
    }

    pub fn info_hash_hex(&self) -> String {
        hex(&self.info_hash)
    }
}

/// Writes `{announce-list, info}` with the info dictionary copied verbatim,
/// so the info hash is unchanged. Keys are in sorted order.
fn torrent_file(info: &[u8], tiers: &[Vec<String>]) -> Vec<u8> {
    let mut root = b"d".to_vec();
    if !tiers.is_empty() {
        let list = Value::List(
            tiers
                .iter()
                .map(|tier| Value::List(tier.iter().map(|u| Value::str(u)).collect()))
                .collect(),
        );
        root.extend_from_slice(b"13:announce-list");
        bencode::encode_into(&list, &mut root);
    }
    root.extend_from_slice(b"4:info");
    root.extend_from_slice(info);
    root.push(b'e');
    root
}

fn non_negative(v: &Value, field: &'static str) -> Result<u64, MetainfoError> {
    v.as_int()
        .filter(|&n| n >= 0)
        .map(|n| n as u64)
        .ok_or(MetainfoError::Invalid(field))
}

fn parse_announce_list(v: Option<&Value>) -> Result<Vec<Vec<String>>, MetainfoError> {
    let Some(v) = v else { return Ok(Vec::new()) };
    let tiers = v.as_list().ok_or(MetainfoError::Invalid("announce-list"))?;
    let mut out = Vec::new();
    for tier in tiers {
        let urls = tier
            .as_list()
            .ok_or(MetainfoError::Invalid("announce-list"))?;
        let mut t = Vec::new();
        for url in urls {
            t.push(
                url.as_str()
                    .ok_or(MetainfoError::Invalid("announce-list"))?
                    .to_string(),
            );
        }
        if !t.is_empty() {
            out.push(t);
        }
    }
    Ok(out)
}

fn parse_files(name: &str, list: &Value) -> Result<Vec<FileEntry>, MetainfoError> {
    let entries = list.as_list().ok_or(MetainfoError::Invalid("info.files"))?;
    if entries.is_empty() {
        return Err(MetainfoError::Invalid("info.files"));
    }
    let mut files = Vec::with_capacity(entries.len());
    let mut offset = 0u64;
    for entry in entries {
        let length = non_negative(
            entry
                .get("length")
                .ok_or(MetainfoError::Missing("info.files.length"))?,
            "info.files.length",
        )?;
        let parts = entry
            .get("path")
            .ok_or(MetainfoError::Missing("info.files.path"))?
            .as_list()
            .filter(|p| !p.is_empty())
            .ok_or(MetainfoError::Invalid("info.files.path"))?;
        let mut path = PathBuf::from(name);
        for part in parts {
            let s = part
                .as_str()
                .ok_or(MetainfoError::Invalid("info.files.path"))?;
            check_component(s, "info.files.path")?;
            path.push(s);
        }
        files.push(FileEntry {
            path,
            length,
            offset,
        });
        offset = offset
            .checked_add(length)
            .ok_or(MetainfoError::Invalid("info.files.length"))?;
    }
    Ok(files)
}

/// Rejects path parts that could escape the output folder.
fn check_component(s: &str, field: &'static str) -> Result<(), MetainfoError> {
    let bad = s.is_empty()
        || s == "."
        || s == ".."
        || s.contains(['/', '\\', '\0'])
        || (cfg!(windows) && s.contains(':'));
    if bad {
        Err(MetainfoError::UnsafePath(s.to_string(), field))
    } else {
        Ok(())
    }
}

/// Number of pieces: the total length divided by the piece length, rounded up.
pub fn piece_count(total_length: u64, piece_length: u64) -> u64 {
    total_length.div_ceil(piece_length)
}

/// Size of piece `index`. Every piece is `piece_length` except the last,
/// which holds whatever remains.
pub fn piece_size(total_length: u64, piece_length: u64, index: u64) -> u64 {
    let count = piece_count(total_length, piece_length);
    if index + 1 == count {
        total_length - (count - 1) * piece_length
    } else if index < count {
        piece_length
    } else {
        0
    }
}

/// Number of 16 KiB blocks in a piece of the given size.
pub fn block_count(piece_size: u32) -> u32 {
    piece_size.div_ceil(BLOCK_SIZE)
}

/// Size of block `block` within a piece of the given size.
pub fn block_size(piece_size: u32, block: u32) -> u32 {
    let start = block.saturating_mul(BLOCK_SIZE);
    piece_size.saturating_sub(start).min(BLOCK_SIZE)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Builds a single-file `.torrent` for `data`. Used by tests and benchmarks.
pub fn build_single_file(
    name: &str,
    data: &[u8],
    piece_length: u64,
    announce: Option<&str>,
) -> Vec<u8> {
    let info = Value::dict([
        ("length", Value::Int(data.len() as i64)),
        ("name", Value::str(name)),
        ("piece length", Value::Int(piece_length as i64)),
        ("pieces", Value::Bytes(hash_pieces(data, piece_length))),
    ]);
    wrap(info, announce)
}

/// Builds a multi-file `.torrent`. Each file is (path parts, contents).
pub fn build_multi_file(
    name: &str,
    files: &[(Vec<&str>, Vec<u8>)],
    piece_length: u64,
    announce: Option<&str>,
) -> Vec<u8> {
    let stream: Vec<u8> = files.iter().flat_map(|(_, d)| d.iter().copied()).collect();
    let list = files
        .iter()
        .map(|(path, d)| {
            Value::dict([
                ("length", Value::Int(d.len() as i64)),
                (
                    "path",
                    Value::List(path.iter().map(|p| Value::str(p)).collect()),
                ),
            ])
        })
        .collect();
    let info = Value::dict([
        ("files", Value::List(list)),
        ("name", Value::str(name)),
        ("piece length", Value::Int(piece_length as i64)),
        ("pieces", Value::Bytes(hash_pieces(&stream, piece_length))),
    ]);
    wrap(info, announce)
}

fn hash_pieces(data: &[u8], piece_length: u64) -> Vec<u8> {
    data.chunks(piece_length as usize)
        .flat_map(|c| Sha1::digest(c).to_vec())
        .collect()
}

fn wrap(info: Value, announce: Option<&str>) -> Vec<u8> {
    let mut root = vec![("info", info)];
    if let Some(a) = announce {
        root.push(("announce", Value::str(a)));
    }
    bencode::encode(&Value::dict(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_torrent() -> Vec<u8> {
        let mut info =
            b"d6:lengthi1048576e4:name8:test.bin12:piece lengthi262144e6:pieces80:".to_vec();
        for c in *b"abcd" {
            info.extend([c; 20]);
        }
        info.push(b'e');
        let mut t = b"d8:announce31:http://tracker.example/announce4:info".to_vec();
        t.extend(info);
        t.push(b'e');
        t
    }

    #[test]
    fn parses_known_torrent_and_info_hash() {
        let t = Torrent::from_bytes(&known_torrent()).unwrap();
        // Computed independently with Python's hashlib over the info bytes.
        assert_eq!(
            t.info_hash_hex(),
            "442dad80c21eb22d703fdc6ca0da1baa32ad9d7d"
        );
        assert_eq!(t.name, "test.bin");
        assert_eq!(t.piece_count(), 4);
        assert_eq!(t.total_length, 1_048_576);
        assert_eq!(t.trackers(), vec!["http://tracker.example/announce"]);
        assert!(!t.multi_file);
    }

    #[test]
    fn piece_arithmetic() {
        // Exact multiple of the piece length.
        assert_eq!(piece_count(1024, 256), 4);
        assert_eq!(piece_size(1024, 256, 3), 256);
        // One byte longer.
        assert_eq!(piece_count(1025, 256), 5);
        assert_eq!(piece_size(1025, 256, 4), 1);
        assert_eq!(piece_size(1025, 256, 3), 256);
        assert_eq!(piece_size(1025, 256, 5), 0);
        // Blocks.
        assert_eq!(block_count(BLOCK_SIZE * 2), 2);
        assert_eq!(block_count(BLOCK_SIZE * 2 + 1), 3);
        assert_eq!(block_size(BLOCK_SIZE * 2 + 1, 2), 1);
        assert_eq!(block_size(BLOCK_SIZE * 2 + 1, 0), BLOCK_SIZE);
    }

    #[test]
    fn multi_file_layout() {
        let raw = build_multi_file(
            "dir",
            &[
                (vec!["a.txt"], vec![1; 300]),
                (vec!["sub", "b.txt"], vec![2; 200]),
            ],
            256,
            None,
        );
        let t = Torrent::from_bytes(&raw).unwrap();
        assert!(t.multi_file);
        assert_eq!(t.total_length, 500);
        assert_eq!(t.files[1].offset, 300);
        assert_eq!(
            t.files[1].path,
            PathBuf::from("dir").join("sub").join("b.txt")
        );
        assert_eq!(t.piece_count(), 2);
    }

    #[test]
    fn from_info_keeps_the_info_hash() {
        let original = Torrent::from_bytes(&known_torrent()).unwrap();
        let rebuilt = Torrent::from_info(
            &original.info_bytes,
            &[
                "udp://a.example:1/".to_string(),
                "http://b.example/".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(rebuilt.info_hash, original.info_hash);
        assert_eq!(rebuilt.info_bytes, original.info_bytes);
        assert_eq!(
            rebuilt.trackers(),
            vec!["udp://a.example:1/", "http://b.example/"]
        );
        assert!(Torrent::from_info(b"d4:name1:xe", &[]).is_err());
        let saved = Torrent::from_bytes(&rebuilt.to_file_bytes()).unwrap();
        assert_eq!(saved.info_hash, original.info_hash);
        assert_eq!(saved.trackers(), rebuilt.trackers());
    }

    #[test]
    fn rejects_unsafe_paths() {
        for bad in ["..", ".", "", "a/b", "a\\b"] {
            let raw = build_multi_file("dir", &[(vec![bad], vec![1; 10])], 256, None);
            assert!(
                matches!(
                    Torrent::from_bytes(&raw),
                    Err(MetainfoError::UnsafePath(..))
                ),
                "accepted {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_wrong_piece_count() {
        let mut info = b"d6:lengthi1048577e4:name1:x12:piece lengthi262144e6:pieces80:".to_vec();
        info.extend([0u8; 80]);
        info.push(b'e');
        let mut t = b"d4:info".to_vec();
        t.extend(info);
        t.push(b'e');
        assert!(matches!(
            Torrent::from_bytes(&t),
            Err(MetainfoError::PieceCount {
                got: 4,
                expected: 5
            })
        ));
    }
}
