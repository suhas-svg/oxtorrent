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

/// Guesses the client from an Azureus-style peer ID (`-qB4630-...`, BEP 20).
/// Unknown codes are shown as-is; IDs in other styles give "unknown".
pub fn client_name(id: &[u8; 20]) -> String {
    if id[0] != b'-' || id[7] != b'-' {
        return "unknown".to_string();
    }
    let code = std::str::from_utf8(&id[1..3]).unwrap_or("??");
    let name = match code {
        "qB" => "qBittorrent",
        "TR" => "Transmission",
        "UT" => "µTorrent",
        "UM" => "µTorrent Mac",
        "BT" => "BitTorrent",
        "LT" | "lt" => "libtorrent",
        "DE" => "Deluge",
        "AZ" => "Vuze",
        "BI" => "BiglyBT",
        "BC" => "BitComet",
        "FD" => "Free Download Manager",
        "OX" => "Oxtorrent",
        "AR" => "Ares",
        "KT" => "KTorrent",
        "XL" => "Xunlei",
        "SD" => "Thunder",
        "WW" => "WebTorrent",
        "RT" => "rTorrent",
        _ => code,
    };
    let version: String = id[3..7]
        .iter()
        .map(|&b| b as char)
        .filter(char::is_ascii_alphanumeric)
        .collect();
    format!("{name} {version}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_names() {
        assert_eq!(client_name(b"-qB4630-abcdefghijkl"), "qBittorrent 4630");
        assert_eq!(client_name(b"-ZZ0001-abcdefghijkl"), "ZZ 0001");
        assert_eq!(client_name(b"M7-2-0--abcdefghijkl"), "unknown");
        let ours = generate_peer_id();
        assert_eq!(client_name(&ours), "Oxtorrent 0001");
    }
}
