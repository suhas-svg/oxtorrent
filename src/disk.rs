//! File layout, piece buffering, hashing and resume.
//!
//! The disk task is the only code that touches files. It runs on a thread
//! from Tokio's blocking pool, so SHA-1 hashing and file I/O never stall the
//! async threads that drive the peer connections.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::bitfield::Bitfield;
use crate::metainfo::{block_count, Torrent};
use crate::BLOCK_SIZE;

#[derive(Debug, Error)]
pub enum DiskError {
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("byte range {offset}+{len} is outside the torrent")]
    OutOfRange { offset: u64, len: u64 },
    #[error("the disk task has stopped")]
    Gone,
}

fn io_err(path: &Path) -> impl FnOnce(std::io::Error) -> DiskError + '_ {
    move |source| DiskError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Result of hashing a finished piece.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyResult {
    pub ok: bool,
    /// Peers shown to have sent a bad block for this piece. Known right away
    /// when one peer sent the whole failed piece, otherwise once a good copy
    /// arrives and the bad blocks can be compared against it.
    pub guilty: Vec<SocketAddr>,
}

pub enum DiskCommand {
    /// Buffer a block in memory. Nothing reaches the file until the whole
    /// piece is verified.
    Write {
        index: u32,
        begin: u32,
        data: Bytes,
        from: SocketAddr,
    },
    /// Hash the buffered piece and write it if it matches.
    Verify {
        index: u32,
        reply: oneshot::Sender<Result<VerifyResult, DiskError>>,
    },
    /// Read a block of a verified piece for upload.
    Read {
        index: u32,
        begin: u32,
        length: u32,
        reply: oneshot::Sender<Result<Bytes, DiskError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), DiskError>>,
    },
}

struct StoredFile {
    path: PathBuf,
    length: u64,
    offset: u64,
    handle: Option<File>,
}

/// The torrent's files viewed as one long byte stream.
pub struct Storage {
    files: Vec<StoredFile>,
    total_length: u64,
    /// True if any output file already existed with data in it.
    pub existed: bool,
}

/// One contiguous piece of a stream range that lies inside a single file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub file: usize,
    pub file_offset: u64,
    pub buf_offset: usize,
    pub len: usize,
}

/// Maps the stream range `offset..offset+len` onto the files it covers.
/// `files` holds (offset, length) pairs in stream order.
pub fn locate(files: &[(u64, u64)], offset: u64, len: u64) -> Vec<Segment> {
    let end = offset + len;
    let mut out = Vec::new();
    // First file whose end is past `offset`.
    let start = files.partition_point(|&(o, l)| o + l <= offset);
    for (i, &(f_off, f_len)) in files.iter().enumerate().skip(start) {
        if f_off >= end {
            break;
        }
        if f_len == 0 {
            continue;
        }
        let lo = offset.max(f_off);
        let hi = end.min(f_off + f_len);
        if lo < hi {
            out.push(Segment {
                file: i,
                file_offset: lo - f_off,
                buf_offset: (lo - offset) as usize,
                len: (hi - lo) as usize,
            });
        }
    }
    out
}

impl Storage {
    /// Creates every file at full size (with `set_len`, so most file systems
    /// reserve it without writing zeros) and any folders they need.
    pub fn create(dir: &Path, torrent: &Torrent) -> Result<Storage, DiskError> {
        let mut existed = false;
        let mut files = Vec::with_capacity(torrent.files.len());
        for f in &torrent.files {
            let path = dir.join(&f.path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(io_err(parent))?;
            }
            if fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false) {
                existed = true;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(io_err(&path))?;
            let current = file.metadata().map_err(io_err(&path))?.len();
            if current != f.length {
                file.set_len(f.length).map_err(io_err(&path))?;
            }
            files.push(StoredFile {
                path,
                length: f.length,
                offset: f.offset,
                handle: None,
            });
        }
        Ok(Storage {
            files,
            total_length: torrent.total_length,
            existed,
        })
    }

    fn segments(&self, offset: u64, len: u64) -> Result<Vec<Segment>, DiskError> {
        if offset
            .checked_add(len)
            .is_none_or(|e| e > self.total_length)
        {
            return Err(DiskError::OutOfRange { offset, len });
        }
        let spans: Vec<(u64, u64)> = self.files.iter().map(|f| (f.offset, f.length)).collect();
        Ok(locate(&spans, offset, len))
    }

    fn handle(&mut self, i: usize) -> Result<&mut File, DiskError> {
        let f = &mut self.files[i];
        if f.handle.is_none() {
            let h = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&f.path)
                .map_err(io_err(&f.path))?;
            f.handle = Some(h);
        }
        let path = f.path.clone();
        f.handle.as_mut().ok_or_else(|| DiskError::Io {
            path,
            source: std::io::Error::other("file handle missing"),
        })
    }

    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DiskError> {
        for seg in self.segments(offset, data.len() as u64)? {
            let path = self.files[seg.file].path.clone();
            let h = self.handle(seg.file)?;
            h.seek(SeekFrom::Start(seg.file_offset))
                .map_err(io_err(&path))?;
            h.write_all(&data[seg.buf_offset..seg.buf_offset + seg.len])
                .map_err(io_err(&path))?;
        }
        Ok(())
    }

    pub fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, DiskError> {
        let mut buf = vec![0u8; len];
        for seg in self.segments(offset, len as u64)? {
            let path = self.files[seg.file].path.clone();
            let h = self.handle(seg.file)?;
            h.seek(SeekFrom::Start(seg.file_offset))
                .map_err(io_err(&path))?;
            h.read_exact(&mut buf[seg.buf_offset..seg.buf_offset + seg.len])
                .map_err(io_err(&path))?;
        }
        Ok(buf)
    }

    pub fn flush(&mut self) -> Result<(), DiskError> {
        for f in &mut self.files {
            if let Some(h) = f.handle.as_mut() {
                h.flush().map_err(io_err(&f.path))?;
                h.sync_data().map_err(io_err(&f.path))?;
            }
        }
        Ok(())
    }

    /// Hashes every piece already on disk and marks the ones that match.
    /// Always correct, even after a crash mid-write.
    pub fn verify_existing(&mut self, torrent: &Torrent) -> Result<Bitfield, DiskError> {
        let mut have = Bitfield::new(torrent.piece_count());
        for i in 0..torrent.piece_count() as u32 {
            let data = self.read_at(torrent.piece_offset(i), torrent.piece_size(i) as usize)?;
            let hash: [u8; 20] = Sha1::digest(&data).into();
            if hash == torrent.piece_hashes[i as usize] {
                have.set(i as usize);
            }
        }
        Ok(have)
    }
}

struct PieceBuffer {
    data: Vec<u8>,
    /// Which peer sent each block.
    sources: Vec<Option<SocketAddr>>,
}

/// Block hashes from failed attempts at a piece, used to find out who sent
/// the bad blocks once a good copy arrives.
type FailedAttempts = Vec<Vec<(SocketAddr, [u8; 20])>>;

/// The disk task's state. Separate from the thread loop so it can be tested.
pub struct Disk {
    storage: Storage,
    torrent: Arc<Torrent>,
    buffers: HashMap<u32, PieceBuffer>,
    failed: HashMap<u32, FailedAttempts>,
}

impl Disk {
    pub fn new(storage: Storage, torrent: Arc<Torrent>) -> Self {
        Disk {
            storage,
            torrent,
            buffers: HashMap::new(),
            failed: HashMap::new(),
        }
    }

    pub fn write_block(&mut self, index: u32, begin: u32, data: &[u8], from: SocketAddr) {
        let size = self.torrent.piece_size(index) as usize;
        if index as usize >= self.torrent.piece_count() || begin as usize + data.len() > size {
            warn!("dropping out-of-range block {index}+{begin}");
            return;
        }
        let buf = self.buffers.entry(index).or_insert_with(|| PieceBuffer {
            data: vec![0; size],
            sources: vec![None; block_count(size as u32) as usize],
        });
        buf.data[begin as usize..begin as usize + data.len()].copy_from_slice(data);
        if let Some(s) = buf.sources.get_mut((begin / BLOCK_SIZE) as usize) {
            *s = Some(from);
        }
    }

    pub fn verify(&mut self, index: u32) -> Result<VerifyResult, DiskError> {
        let Some(buf) = self.buffers.remove(&index) else {
            return Ok(VerifyResult {
                ok: false,
                guilty: Vec::new(),
            });
        };
        let hash: [u8; 20] = Sha1::digest(&buf.data).into();
        let ok = self.torrent.piece_hashes.get(index as usize) == Some(&hash);
        let block_hashes: Vec<[u8; 20]> = buf
            .data
            .chunks(BLOCK_SIZE as usize)
            .map(|c| Sha1::digest(c).into())
            .collect();

        let mut guilty = Vec::new();
        if ok {
            self.storage
                .write_at(self.torrent.piece_offset(index), &buf.data)?;
            if let Some(attempts) = self.failed.remove(&index) {
                for (b, entries) in attempts.iter().enumerate() {
                    for (peer, h) in entries {
                        if block_hashes.get(b) != Some(h) && !guilty.contains(peer) {
                            guilty.push(*peer);
                        }
                    }
                }
            }
        } else {
            let mut senders: Vec<SocketAddr> = buf.sources.iter().flatten().copied().collect();
            senders.dedup();
            senders.sort();
            senders.dedup();
            if senders.len() == 1 {
                guilty = senders;
            } else {
                let attempts = self
                    .failed
                    .entry(index)
                    .or_insert_with(|| vec![Vec::new(); block_hashes.len()]);
                for (b, src) in buf.sources.iter().enumerate() {
                    if let (Some(peer), Some(h)) = (src, block_hashes.get(b)) {
                        if let Some(list) = attempts.get_mut(b) {
                            list.push((*peer, *h));
                        }
                    }
                }
            }
        }
        Ok(VerifyResult { ok, guilty })
    }

    pub fn read_block(&mut self, index: u32, begin: u32, length: u32) -> Result<Bytes, DiskError> {
        let offset = self.torrent.piece_offset(index) + begin as u64;
        if begin as u64 + length as u64 > self.torrent.piece_size(index) as u64 {
            return Err(DiskError::OutOfRange {
                offset,
                len: length as u64,
            });
        }
        self.storage
            .read_at(offset, length as usize)
            .map(Bytes::from)
    }

    pub fn flush(&mut self) -> Result<(), DiskError> {
        self.storage.flush()
    }
}

/// Starts the disk task. It exits once every sender of `rx` is dropped.
pub fn spawn(
    storage: Storage,
    torrent: Arc<Torrent>,
    mut rx: mpsc::Receiver<DiskCommand>,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut disk = Disk::new(storage, torrent);
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                DiskCommand::Write {
                    index,
                    begin,
                    data,
                    from,
                } => disk.write_block(index, begin, &data, from),
                DiskCommand::Verify { index, reply } => {
                    let _ = reply.send(disk.verify(index));
                }
                DiskCommand::Read {
                    index,
                    begin,
                    length,
                    reply,
                } => {
                    let _ = reply.send(disk.read_block(index, begin, length));
                }
                DiskCommand::Flush { reply } => {
                    let _ = reply.send(disk.flush());
                }
            }
        }
        if let Err(e) = disk.flush() {
            warn!("final flush failed: {e}");
        }
        debug!("disk task stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{build_multi_file, build_single_file};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn locate_crosses_file_boundary() {
        let files = [(0, 300), (300, 0), (300, 200)];
        let segs = locate(&files, 256, 100);
        assert_eq!(
            segs,
            vec![
                Segment {
                    file: 0,
                    file_offset: 256,
                    buf_offset: 0,
                    len: 44
                },
                Segment {
                    file: 2,
                    file_offset: 0,
                    buf_offset: 44,
                    len: 56
                },
            ]
        );
        assert_eq!(locate(&files, 0, 300).len(), 1);
    }

    #[test]
    fn piece_crossing_files_lands_in_both() {
        let dir = tempfile::tempdir().unwrap();
        let a: Vec<u8> = (0..300u32).map(|i| i as u8).collect();
        let b: Vec<u8> = (0..200u32).map(|i| (i * 7) as u8).collect();
        let raw = build_multi_file(
            "multi",
            &[(vec!["a"], a.clone()), (vec!["d", "b"], b.clone())],
            256,
            None,
        );
        let t = Arc::new(Torrent::from_bytes(&raw).unwrap());
        let storage = Storage::create(dir.path(), &t).unwrap();
        assert!(!storage.existed);
        let mut disk = Disk::new(storage, t.clone());
        let stream: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
        for i in 0..t.piece_count() as u32 {
            let off = t.piece_offset(i) as usize;
            let size = t.piece_size(i) as usize;
            disk.write_block(i, 0, &stream[off..off + size], addr(1));
            assert!(disk.verify(i).unwrap().ok);
        }
        disk.flush().unwrap();
        assert_eq!(fs::read(dir.path().join("multi").join("a")).unwrap(), a);
        assert_eq!(
            fs::read(dir.path().join("multi").join("d").join("b")).unwrap(),
            b
        );
        assert_eq!(
            disk.read_block(0, 250, 6).unwrap().to_vec(),
            stream[250..256].to_vec()
        );
    }

    #[test]
    fn bad_piece_never_reaches_file_and_resume_rehashes() {
        let dir = tempfile::tempdir().unwrap();
        let data = vec![9u8; BLOCK_SIZE as usize * 3];
        let raw = build_single_file("f.bin", &data, BLOCK_SIZE as u64 * 2, None);
        let t = Arc::new(Torrent::from_bytes(&raw).unwrap());
        let mut disk = Disk::new(Storage::create(dir.path(), &t).unwrap(), t.clone());

        // Piece 0 from two peers, one block corrupt: guilt unknown yet.
        let blk = BLOCK_SIZE as usize;
        disk.write_block(0, 0, &data[..blk], addr(1));
        disk.write_block(0, BLOCK_SIZE, &vec![0u8; blk], addr(2));
        assert_eq!(
            disk.verify(0).unwrap(),
            VerifyResult {
                ok: false,
                guilty: vec![]
            }
        );
        // Good copy arrives: peer 2 is shown guilty.
        disk.write_block(0, 0, &data[..blk], addr(1));
        disk.write_block(0, BLOCK_SIZE, &data[blk..2 * blk], addr(3));
        assert_eq!(
            disk.verify(0).unwrap(),
            VerifyResult {
                ok: true,
                guilty: vec![addr(2)]
            }
        );

        // Piece 1 entirely from one bad peer: guilty right away.
        disk.write_block(1, 0, &vec![1u8; blk], addr(4));
        assert_eq!(
            disk.verify(1).unwrap(),
            VerifyResult {
                ok: false,
                guilty: vec![addr(4)]
            }
        );
        disk.flush().unwrap();
        drop(disk);

        let mut storage = Storage::create(dir.path(), &t).unwrap();
        assert!(storage.existed);
        let have = storage.verify_existing(&t).unwrap();
        assert!(have.has(0) && !have.has(1));
    }
}
