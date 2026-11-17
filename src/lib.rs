//! Oxtorrent: a command-line BitTorrent client with a built-in swarm simulator.
//!
//! The binary in `main.rs` is a thin wrapper around [`cli::run`]; everything
//! else lives here so integration tests and benchmarks use the same code.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod bencode;
pub mod bitfield;
pub mod choke;
pub mod cli;
pub mod dashboard;
pub mod dht;
pub mod disk;
pub mod magnet;
pub mod metainfo;
pub mod peer;
pub mod picker;
pub mod router;
pub mod session;
pub mod sim;
pub mod tracker;

/// Identifies one peer connection inside a session (or one simulated peer).
pub type PeerKey = u64;

/// Size of a block, the unit requested from peers (16 KiB).
pub const BLOCK_SIZE: u32 = 16_384;
