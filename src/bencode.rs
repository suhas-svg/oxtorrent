//! Bencode parsing and writing. Knows nothing about torrents.
//!
//! Byte strings are kept as `Vec<u8>` because they are raw bytes that need
//! not be valid UTF-8 (piece hashes, compact peer lists).

use std::collections::BTreeMap;
use std::ops::Range;

use thiserror::Error;

/// Maximum nesting depth of lists and dictionaries.
pub const MAX_DEPTH: usize = 64;

/// Longest accepted decimal length prefix or integer, in digits.
const MAX_DIGITS: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

impl Value {
    pub fn str(s: &str) -> Value {
        Value::Bytes(s.as_bytes().to_vec())
    }

    pub fn dict<K: AsRef<[u8]>>(entries: impl IntoIterator<Item = (K, Value)>) -> Value {
        Value::Dict(
            entries
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_vec(), v))
                .collect(),
        )
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }

    /// Looks up `key` if this value is a dictionary.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_dict().and_then(|d| d.get(key.as_bytes()))
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BencodeError {
    #[error("unexpected end of input at byte {0}")]
    UnexpectedEof(usize),
    #[error("unexpected byte {byte:#04x} at byte {pos}")]
    UnexpectedByte { pos: usize, byte: u8 },
    #[error("invalid integer at byte {0}")]
    InvalidInteger(usize),
    #[error("invalid string length at byte {0}")]
    InvalidLength(usize),
    #[error("dictionary key at byte {0} is not a byte string")]
    NonStringKey(usize),
    #[error("dictionary key at byte {0} is out of order or duplicated")]
    UnsortedKeys(usize),
    #[error("nesting deeper than {MAX_DEPTH} levels at byte {0}")]
    TooDeep(usize),
    #[error("trailing data after the value at byte {0}")]
    TrailingData(usize),
}

/// Byte ranges of the values of a top-level dictionary, by key.
pub type Spans = Vec<(Vec<u8>, Range<usize>)>;

/// Decodes exactly one value that must span the whole input.
pub fn decode(data: &[u8]) -> Result<Value, BencodeError> {
    decode_with_spans(data).map(|(v, _)| v)
}

/// Decodes one value and also returns, for each entry of a top-level
/// dictionary, the byte range its value occupies in `data`.
///
/// The info hash must be computed over the original bytes of the `info`
/// dictionary, never over a re-encoding, so the parser records them here.
pub fn decode_with_spans(data: &[u8]) -> Result<(Value, Spans), BencodeError> {
    let mut p = Parser {
        data,
        pos: 0,
        spans: Vec::new(),
    };
    let v = p.value(0)?;
    if p.pos != data.len() {
        return Err(BencodeError::TrailingData(p.pos));
    }
    Ok((v, p.spans))
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
    spans: Spans,
}

impl Parser<'_> {
    fn peek(&self) -> Result<u8, BencodeError> {
        self.data
            .get(self.pos)
            .copied()
            .ok_or(BencodeError::UnexpectedEof(self.pos))
    }

    fn value(&mut self, depth: usize) -> Result<Value, BencodeError> {
        if depth > MAX_DEPTH {
            return Err(BencodeError::TooDeep(self.pos));
        }
        match self.peek()? {
            b'i' => self.int(),
            b'0'..=b'9' => self.bytes().map(Value::Bytes),
            b'l' => self.list(depth),
            b'd' => self.dict(depth),
            byte => Err(BencodeError::UnexpectedByte {
                pos: self.pos,
                byte,
            }),
        }
    }

    /// Scans digits (and an optional leading minus) up to `terminator`.
    fn number_text(&self, start: usize, terminator: u8) -> Result<(&[u8], usize), BencodeError> {
        let limit = (start + MAX_DIGITS + 2).min(self.data.len());
        let window = &self.data[start..limit];
        match window.iter().position(|&b| b == terminator) {
            Some(n) => Ok((&window[..n], start + n)),
            None if limit == self.data.len() => Err(BencodeError::UnexpectedEof(limit)),
            None => Err(BencodeError::InvalidInteger(start)),
        }
    }

    fn int(&mut self) -> Result<Value, BencodeError> {
        let start = self.pos + 1;
        let (text, end) = self.number_text(start, b'e')?;
        let digits = text.strip_prefix(b"-").unwrap_or(text);
        let negative = digits.len() != text.len();
        let valid = !digits.is_empty()
            && digits.iter().all(u8::is_ascii_digit)
            && !(digits.len() > 1 && digits[0] == b'0')
            && !(negative && digits == b"0");
        if !valid {
            return Err(BencodeError::InvalidInteger(start));
        }
        let n = std::str::from_utf8(text)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or(BencodeError::InvalidInteger(start))?;
        self.pos = end + 1;
        Ok(Value::Int(n))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, BencodeError> {
        let start = self.pos;
        let (text, colon) = self.number_text(start, b':')?;
        let valid = !text.is_empty()
            && text.iter().all(u8::is_ascii_digit)
            && !(text.len() > 1 && text[0] == b'0');
        if !valid {
            return Err(BencodeError::InvalidLength(start));
        }
        let len = std::str::from_utf8(text)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or(BencodeError::InvalidLength(start))?;
        let body = colon + 1;
        let end = body
            .checked_add(len)
            .filter(|&e| e <= self.data.len())
            .ok_or(BencodeError::UnexpectedEof(self.data.len()))?;
        self.pos = end;
        Ok(self.data[body..end].to_vec())
    }

    fn list(&mut self, depth: usize) -> Result<Value, BencodeError> {
        self.pos += 1;
        let mut items = Vec::new();
        while self.peek()? != b'e' {
            items.push(self.value(depth + 1)?);
        }
        self.pos += 1;
        Ok(Value::List(items))
    }

    fn dict(&mut self, depth: usize) -> Result<Value, BencodeError> {
        self.pos += 1;
        let mut map = BTreeMap::new();
        let mut prev: Option<Vec<u8>> = None;
        while self.peek()? != b'e' {
            let key_pos = self.pos;
            if !self.peek()?.is_ascii_digit() {
                return Err(BencodeError::NonStringKey(key_pos));
            }
            let key = self.bytes()?;
            if prev.as_ref().is_some_and(|p| *p >= key) {
                return Err(BencodeError::UnsortedKeys(key_pos));
            }
            let value_start = self.pos;
            let value = self.value(depth + 1)?;
            if depth == 0 {
                self.spans.push((key.clone(), value_start..self.pos));
            }
            prev = Some(key.clone());
            map.insert(key, value);
        }
        self.pos += 1;
        Ok(Value::Dict(map))
    }
}

/// Encodes a value. Dictionary keys come out sorted because `BTreeMap` is.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

pub fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(b) => encode_bytes(b, out),
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(map) => {
            out.push(b'd');
            for (k, v) in map {
                encode_bytes(k, out);
                encode_into(v, out);
            }
            out.push(b'e');
        }
    }
}

fn encode_bytes(b: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(b.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn spec_examples() {
        assert_eq!(decode(b"i42e").unwrap(), Value::Int(42));
        assert_eq!(decode(b"4:spam").unwrap(), Value::str("spam"));
        assert_eq!(
            decode(b"l4:spami42ee").unwrap(),
            Value::List(vec![Value::str("spam"), Value::Int(42)])
        );
        assert_eq!(
            decode(b"d3:foo3:bare").unwrap(),
            Value::dict([("foo", Value::str("bar"))])
        );
    }

    #[test]
    fn nested_values_round_trip() {
        let raw = b"d5:emptyde4:listl1:ai-3eld1:xi0eeee3:raw3:\x00\xff\x01e";
        let v = decode(raw).unwrap();
        assert_eq!(encode(&v), raw.to_vec());
        assert_eq!(v.get("raw").unwrap().as_bytes().unwrap(), &[0, 255, 1]);
    }

    #[test]
    fn malformed_input_is_rejected() {
        let cases: &[&[u8]] = &[
            b"",
            b"i42",
            b"l4:spam",
            b"i-0e",
            b"i03e",
            b"ie",
            b"i-e",
            b"-1:a",
            b"5:spam",
            b"01:a",
            b"d3:foo3:bar3:abc1:xe",
            b"d3:foo3:bar3:foo1:xe",
            b"di1e1:ae",
            b"i1ei2e",
            b"x",
            b"i99999999999999999999e",
        ];
        for case in cases {
            assert!(
                decode(case).is_err(),
                "accepted {:?}",
                String::from_utf8_lossy(case)
            );
        }
    }

    #[test]
    fn depth_is_limited() {
        let mut deep = vec![b'l'; MAX_DEPTH + 1];
        deep.extend(vec![b'e'; MAX_DEPTH + 1]);
        assert!(decode(&deep).is_ok());
        let mut too_deep = vec![b'l'; MAX_DEPTH + 2];
        too_deep.extend(vec![b'e'; MAX_DEPTH + 2]);
        assert!(matches!(decode(&too_deep), Err(BencodeError::TooDeep(_))));
    }

    #[test]
    fn spans_point_at_original_bytes() {
        let raw = b"d1:ai1e4:infod1:xi7eee";
        let (_, spans) = decode_with_spans(raw).unwrap();
        let info = spans.iter().find(|(k, _)| k == b"info").unwrap();
        assert_eq!(&raw[info.1.clone()], b"d1:xi7ee");
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            any::<i64>().prop_map(Value::Int),
            proptest::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
        ];
        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..8).prop_map(Value::List),
                proptest::collection::btree_map(
                    proptest::collection::vec(any::<u8>(), 0..8),
                    inner,
                    0..8
                )
                .prop_map(Value::Dict),
            ]
        })
    }

    proptest! {
        #[test]
        fn decode_of_encode_is_identity(v in arb_value()) {
            prop_assert_eq!(decode(&encode(&v)).unwrap(), v);
        }

        #[test]
        fn random_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode(&data);
        }

        #[test]
        fn bencode_like_bytes_never_panic(data in "[ilde0-9:a-z-]{0,64}") {
            let _ = decode(data.as_bytes());
        }
    }
}
