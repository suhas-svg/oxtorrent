//! The peer wire protocol: handshake, message codec, extensions, and the
//! per-peer task.

pub mod connection;
pub mod extension;
pub mod handshake;
pub mod message;

/// Builds a peer ID in the Azureus style (BEP 20): `-OX0001-` plus 12
/// random alphanumeric characters.
pub fn generate_peer_id() -> [u8; 20] {
    use rand::Rng;
    const CHARS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-OX0001-");
    let mut rng = rand::thread_rng();
    for b in &mut id[8..] {
        *b = CHARS[rng.gen_range(0..CHARS.len())];
    }
    id
}
