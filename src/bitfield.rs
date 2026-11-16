//! A row of bits, one per piece. Bit 0 is the high bit of byte 0.

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BitfieldError {
    #[error("bitfield is {got} bytes, expected {expected}")]
    WrongLength { got: usize, expected: usize },
    #[error("bitfield has bits set past the last piece")]
    SpareBitsSet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bitfield {
    bytes: Vec<u8>,
    len: usize,
}

impl Bitfield {
    pub fn new(len: usize) -> Self {
        Bitfield {
            bytes: vec![0; len.div_ceil(8)],
            len,
        }
    }

    pub fn full(len: usize) -> Self {
        let mut b = Bitfield::new(len);
        for i in 0..len {
            b.set(i);
        }
        b
    }

    /// Parses a bitfield message payload, enforcing the exact length and
    /// that the unused bits at the end are zero.
    pub fn from_bytes(bytes: &[u8], len: usize) -> Result<Self, BitfieldError> {
        let expected = len.div_ceil(8);
        if bytes.len() != expected {
            return Err(BitfieldError::WrongLength {
                got: bytes.len(),
                expected,
            });
        }
        let spare = expected * 8 - len;
        if spare > 0 {
            let mask = (1u8 << spare) - 1;
            if bytes[expected - 1] & mask != 0 {
                return Err(BitfieldError::SpareBitsSet);
            }
        }
        Ok(Bitfield {
            bytes: bytes.to_vec(),
            len,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn has(&self, i: usize) -> bool {
        i < self.len && self.bytes[i / 8] & (0x80 >> (i % 8)) != 0
    }

    pub fn set(&mut self, i: usize) {
        if i < self.len {
            self.bytes[i / 8] |= 0x80 >> (i % 8);
        }
    }

    pub fn clear(&mut self, i: usize) {
        if i < self.len {
            self.bytes[i / 8] &= !(0x80 >> (i % 8));
        }
    }

    pub fn count_ones(&self) -> usize {
        self.bytes.iter().map(|b| b.count_ones() as usize).sum()
    }

    pub fn is_full(&self) -> bool {
        self.count_ones() == self.len
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len).filter(|&i| self.has(i))
    }

    /// True if some bit is set here but not in `other` (same length).
    pub fn has_any_not_in(&self, other: &Bitfield) -> bool {
        self.bytes
            .iter()
            .zip(other.bytes.iter())
            .any(|(a, b)| a & !b != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_query() {
        let mut b = Bitfield::new(10);
        b.set(0);
        b.set(9);
        assert_eq!(b.as_bytes(), &[0x80, 0x40]);
        assert!(b.has(0) && b.has(9) && !b.has(5) && !b.has(100));
        assert_eq!(b.iter_ones().collect::<Vec<_>>(), vec![0, 9]);
        b.clear(0);
        assert_eq!(b.count_ones(), 1);
    }

    #[test]
    fn parse_checks_length_and_spare_bits() {
        assert!(Bitfield::from_bytes(&[0xff, 0xc0], 10).is_ok());
        assert_eq!(
            Bitfield::from_bytes(&[0xff, 0xe0], 10),
            Err(BitfieldError::SpareBitsSet)
        );
        assert!(matches!(
            Bitfield::from_bytes(&[0xff], 10),
            Err(BitfieldError::WrongLength { .. })
        ));
        assert!(Bitfield::from_bytes(&[0xff], 8).unwrap().is_full());
    }
}
