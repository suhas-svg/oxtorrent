//! KRPC, the DHT's message format (BEP 5): one bencoded dictionary per UDP
//! datagram. `t` is the transaction ID, `y` says query (`q`), response
//! (`r`) or error (`e`).

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use thiserror::Error;

use super::NodeId;
use crate::bencode::{self, Value};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KrpcError {
    #[error("not valid bencode")]
    Bencode,
    #[error("missing or invalid field `{0}`")]
    Field(&'static str),
    #[error("unknown message type")]
    UnknownType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Ping,
    FindNode {
        target: NodeId,
    },
    GetPeers {
        info_hash: [u8; 20],
    },
    AnnouncePeer {
        info_hash: [u8; 20],
        port: u16,
        /// Use the UDP source port instead of `port` (for peers behind NAT).
        implied_port: bool,
        token: Vec<u8>,
    },
}

impl Query {
    pub fn method(&self) -> &'static str {
        match self {
            Query::Ping => "ping",
            Query::FindNode { .. } => "find_node",
            Query::GetPeers { .. } => "get_peers",
            Query::AnnouncePeer { .. } => "announce_peer",
        }
    }
}

/// A response. Fields a method does not use stay empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Response {
    pub id: NodeId,
    /// Closer nodes (`nodes`, compact IPv4 form).
    pub nodes: Vec<(NodeId, SocketAddrV4)>,
    /// Peers for the info hash (`values`).
    pub values: Vec<SocketAddrV4>,
    /// Write token for a later `announce_peer`.
    pub token: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Query { id: NodeId, query: Query },
    Response(Response),
    Error { code: i64, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Msg {
    pub tid: Vec<u8>,
    pub body: Body,
}

/// Standard error codes.
pub const ERROR_GENERIC: i64 = 201;
pub const ERROR_PROTOCOL: i64 = 203;
pub const ERROR_METHOD: i64 = 204;

fn compact_nodes(nodes: &[(NodeId, SocketAddrV4)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nodes.len() * 26);
    for (id, addr) in nodes {
        out.extend_from_slice(&id.0);
        out.extend_from_slice(&addr.ip().octets());
        out.extend_from_slice(&addr.port().to_be_bytes());
    }
    out
}

pub fn compact_peer(addr: SocketAddrV4) -> Vec<u8> {
    let mut v = addr.ip().octets().to_vec();
    v.extend_from_slice(&addr.port().to_be_bytes());
    v
}

fn parse_peer(b: &[u8]) -> Option<SocketAddrV4> {
    if b.len() != 6 {
        return None;
    }
    let addr = SocketAddrV4::new(
        Ipv4Addr::new(b[0], b[1], b[2], b[3]),
        u16::from_be_bytes([b[4], b[5]]),
    );
    (addr.port() != 0).then_some(addr)
}

pub fn encode(msg: &Msg) -> Vec<u8> {
    let mut d: Vec<(&str, Value)> = vec![("t", Value::Bytes(msg.tid.clone()))];
    match &msg.body {
        Body::Query { id, query } => {
            let mut a: Vec<(&str, Value)> = vec![("id", Value::Bytes(id.0.to_vec()))];
            match query {
                Query::Ping => {}
                Query::FindNode { target } => a.push(("target", Value::Bytes(target.0.to_vec()))),
                Query::GetPeers { info_hash } => {
                    a.push(("info_hash", Value::Bytes(info_hash.to_vec())))
                }
                Query::AnnouncePeer {
                    info_hash,
                    port,
                    implied_port,
                    token,
                } => {
                    a.push(("info_hash", Value::Bytes(info_hash.to_vec())));
                    a.push(("port", Value::Int(*port as i64)));
                    a.push(("implied_port", Value::Int(*implied_port as i64)));
                    a.push(("token", Value::Bytes(token.clone())));
                }
            }
            d.push(("y", Value::str("q")));
            d.push(("q", Value::str(query.method())));
            d.push(("a", Value::dict(a)));
        }
        Body::Response(r) => {
            let mut rd: Vec<(&str, Value)> = vec![("id", Value::Bytes(r.id.0.to_vec()))];
            if !r.nodes.is_empty() {
                rd.push(("nodes", Value::Bytes(compact_nodes(&r.nodes))));
            }
            if !r.values.is_empty() {
                rd.push((
                    "values",
                    Value::List(
                        r.values
                            .iter()
                            .map(|&p| Value::Bytes(compact_peer(p)))
                            .collect(),
                    ),
                ));
            }
            if let Some(t) = &r.token {
                rd.push(("token", Value::Bytes(t.clone())));
            }
            d.push(("y", Value::str("r")));
            d.push(("r", Value::dict(rd)));
        }
        Body::Error { code, message } => {
            d.push(("y", Value::str("e")));
            d.push((
                "e",
                Value::List(vec![Value::Int(*code), Value::str(message)]),
            ));
        }
    }
    bencode::encode(&Value::dict(d))
}

fn id_field(v: &Value, key: &'static str) -> Result<[u8; 20], KrpcError> {
    v.get(key)
        .and_then(Value::as_bytes)
        .and_then(|b| <[u8; 20]>::try_from(b).ok())
        .ok_or(KrpcError::Field(key))
}

pub fn parse(data: &[u8]) -> Result<Msg, KrpcError> {
    let (v, _) = bencode::decode_prefix_lenient(data).map_err(|_| KrpcError::Bencode)?;
    let tid = v
        .get("t")
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::Field("t"))?
        .to_vec();
    let kind = v
        .get("y")
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::Field("y"))?;
    let body = match kind {
        b"q" => {
            let method = v
                .get("q")
                .and_then(Value::as_bytes)
                .ok_or(KrpcError::Field("q"))?;
            let a = v
                .get("a")
                .filter(|a| a.as_dict().is_some())
                .ok_or(KrpcError::Field("a"))?;
            let id = NodeId(id_field(a, "id")?);
            let query = match method {
                b"ping" => Query::Ping,
                b"find_node" => Query::FindNode {
                    target: NodeId(id_field(a, "target")?),
                },
                b"get_peers" => Query::GetPeers {
                    info_hash: id_field(a, "info_hash")?,
                },
                b"announce_peer" => Query::AnnouncePeer {
                    info_hash: id_field(a, "info_hash")?,
                    port: a
                        .get("port")
                        .and_then(Value::as_int)
                        .and_then(|p| u16::try_from(p).ok())
                        .ok_or(KrpcError::Field("port"))?,
                    implied_port: a.get("implied_port").and_then(Value::as_int).unwrap_or(0) != 0,
                    token: a
                        .get("token")
                        .and_then(Value::as_bytes)
                        .ok_or(KrpcError::Field("token"))?
                        .to_vec(),
                },
                _ => return Err(KrpcError::Field("q")),
            };
            Body::Query { id, query }
        }
        b"r" => {
            let r = v
                .get("r")
                .filter(|r| r.as_dict().is_some())
                .ok_or(KrpcError::Field("r"))?;
            let id = NodeId(id_field(r, "id")?);
            let mut nodes = Vec::new();
            if let Some(b) = r.get("nodes").and_then(Value::as_bytes) {
                for c in b.as_chunks::<26>().0 {
                    let mut nid = [0u8; 20];
                    nid.copy_from_slice(&c[..20]);
                    if let Some(addr) = parse_peer(&c[20..]) {
                        nodes.push((NodeId(nid), addr));
                    }
                }
            }
            let values = r
                .get("values")
                .and_then(Value::as_list)
                .map(|l| {
                    l.iter()
                        .filter_map(|p| p.as_bytes().and_then(parse_peer))
                        .collect()
                })
                .unwrap_or_default();
            let token = r.get("token").and_then(Value::as_bytes).map(<[u8]>::to_vec);
            Body::Response(Response {
                id,
                nodes,
                values,
                token,
            })
        }
        b"e" => {
            let e = v.get("e").and_then(Value::as_list).unwrap_or(&[]);
            Body::Error {
                code: e.first().and_then(Value::as_int).unwrap_or(ERROR_GENERIC),
                message: e
                    .get(1)
                    .and_then(Value::as_bytes)
                    .map(|m| String::from_utf8_lossy(m).into_owned())
                    .unwrap_or_default(),
            }
        }
        _ => return Err(KrpcError::UnknownType),
    };
    Ok(Msg { tid, body })
}

/// IPv4 address of a socket address, if it has one (the DHT here is
/// IPv4-only, as in BEP 5).
pub fn v4(addr: SocketAddr) -> Option<SocketAddrV4> {
    match addr {
        SocketAddr::V4(a) => Some(a),
        SocketAddr::V6(a) => a
            .ip()
            .to_ipv4_mapped()
            .map(|ip| SocketAddrV4::new(ip, a.port())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> NodeId {
        NodeId([b; 20])
    }

    #[test]
    fn bep5_ping_example() {
        // The examples in BEP 5 use the id "abcdefghij0123456789".
        let q = Msg {
            tid: b"aa".to_vec(),
            body: Body::Query {
                id: NodeId(*b"abcdefghij0123456789"),
                query: Query::Ping,
            },
        };
        assert_eq!(
            encode(&q),
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".to_vec()
        );
        let r = parse(b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re").unwrap();
        assert_eq!(r.tid, b"aa");
        assert!(
            matches!(r.body, Body::Response(Response { id, .. }) if id.0 == *b"mnopqrstuvwxyz123456")
        );
        let e = parse(b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee").unwrap();
        assert_eq!(
            e.body,
            Body::Error {
                code: 201,
                message: "A Generic Error Ocurred".to_string()
            }
        );
    }

    #[test]
    fn every_message_round_trips() {
        let addr = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 6881);
        let bodies = vec![
            Body::Query {
                id: id(1),
                query: Query::FindNode { target: id(2) },
            },
            Body::Query {
                id: id(1),
                query: Query::GetPeers { info_hash: [3; 20] },
            },
            Body::Query {
                id: id(1),
                query: Query::AnnouncePeer {
                    info_hash: [3; 20],
                    port: 51413,
                    implied_port: true,
                    token: b"tok".to_vec(),
                },
            },
            Body::Response(Response {
                id: id(4),
                nodes: vec![(id(5), addr), (id(6), addr)],
                values: vec![addr],
                token: Some(b"xyz".to_vec()),
            }),
            Body::Response(Response {
                id: id(4),
                ..Response::default()
            }),
            Body::Error {
                code: ERROR_METHOD,
                message: "Method Unknown".to_string(),
            },
        ];
        for body in bodies {
            let m = Msg {
                tid: vec![0, 7],
                body,
            };
            assert_eq!(parse(&encode(&m)).unwrap(), m);
        }
    }

    #[test]
    fn malformed_messages_are_rejected() {
        assert_eq!(parse(b"garbage"), Err(KrpcError::Bencode));
        assert_eq!(parse(b"d1:y1:qe"), Err(KrpcError::Field("t")));
        assert_eq!(parse(b"d1:t1:a1:y1:xe"), Err(KrpcError::UnknownType));
        // Short id.
        assert_eq!(
            parse(b"d1:ad2:id3:abce1:q4:ping1:t2:aa1:y1:qe"),
            Err(KrpcError::Field("id"))
        );
        // Unknown method.
        assert_eq!(
            parse(b"d1:ad2:id20:abcdefghij0123456789e1:q4:vote1:t2:aa1:y1:qe"),
            Err(KrpcError::Field("q"))
        );
    }
}
