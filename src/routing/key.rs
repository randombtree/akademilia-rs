/// Kademlia routing key
use std::fmt::{Display, Formatter, Error as FmtError};
use std::cmp::{Ord, Ordering, PartialOrd};

use rand::{Rng, CryptoRng};


use super::constants::*;

type KadBytes = [u8; KAD_BYTES];


#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct Key(KadBytes);

impl Default for Key {
    fn default() -> Self {
	Key (std::array::from_fn(|_| 0))
    }
}


impl From<KadBytes> for Key {
    fn from(bytes: KadBytes) -> Self {
	Key(bytes)
    }
}


impl Key {
    pub fn from_fn<F: FnMut(usize) -> u8>(initializer: F) -> Self {
	Key::from(std::array::from_fn(initializer))
    }

    #[cfg(test)]
    pub fn from_fn_bits<F: FnMut(usize) -> bool>(mut initializer: F) -> Self {
	Self::from_fn(|i| {
	    (0..8).fold(0u8, |acc, b| {
		acc | u8::from(initializer(8 * i + b)) << (7 - b) // Note reverse bit order
	    })
	})
    }

    /// Generate random key from the supplied random number generator.
    pub fn from_random<R: Rng>(rng: &mut R) -> Key {
	Key (std::array::from_fn(|_| rng.gen()))
    }

    /// Generates random key from cryptographically safe rng.
    pub fn from_secure_random<R: CryptoRng + Rng>(rng: &mut R) -> Key {
	Self::from_random(rng)
    }

    /// Returns true if bit number 'bit' is set in key.
    pub fn is_set(&self, bit: usize) -> bool {
	assert!(bit < KAD_BITS);
	let byte   = bit / 8;
	// NB: Ordering is reversed to keep distance nicely sorted
	let offset = 7 - (bit % 8);
	let mask   = 1 << offset;
	self.0[byte] & mask != 0
    }

    /// Get the XOR distance between the keys, i.e. d(x,y)
    pub fn distance(&self, other: &Key) -> Distance {
	// Good grief! :( - https://github.com/rust-lang/rfcs/issues/2773
	// TODO: Benchmark and optimize with std::simd once it stabilizes?
	let mut bits:u16 = 0;
	let mut end = false;
	let bytes = std::array::from_fn(|i| {
	    let xor = self.0[i] ^ other.0[i];
	    // Calculate leading ones, used in bucket placement
	    if !end {
		if xor == 0 {
		    bits += u16::try_from(u8::BITS).unwrap();
		} else {
		    end = true;
		    // NB: Reversed ordering for better sorting, also see is_set
		    bits += u16::try_from(xor.leading_zeros()).unwrap();
		}
	    }
	    xor
	});

	Distance {
	    key: Key::from(bytes),
	    bits,
	}
    }
}


impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
	write!(f, "Key (")?;
	for b in self.0 {
	    write!(f, "{:02X}", b)?;
	}
	write!(f, ")")?;
	Ok(())
    }
}


impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
	for (a, b) in self.0.iter().zip(other.0.iter()) {
	    let ord = a.cmp(b);
	    if ord != Ordering::Equal {
		return Some(ord)
	    }
	}
	Some(Ordering::Equal)

    }
}


impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
	self.partial_cmp(other).unwrap()
    }
}


#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Distance {
    /// XOR key
    key: Key,
    /// prefix (leading) zero bits; grows when we get closer.
    bits: u16,
}


impl Distance {
    pub fn is_set(&self, bit: usize) -> bool { self.key.is_set(bit) }
    pub fn bits(&self) -> u16 { self.bits }
}


impl Default for Distance {
    fn default() -> Self {
	Distance {
	    key: Key::default(),
	    bits: u16::try_from(KAD_BITS).unwrap(),
	}
    }
}


impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
	(self.bits < other.bits)
	    .then_some(Ordering::Less)
	    .or_else(|| (self.bits > other.bits).then_some(Ordering::Greater))
	    .or_else(|| self.key.partial_cmp(&other.key)
		     .and_then(|ord| Some(ord.reverse()))) // Distance is measured from furthest

    }
}


impl Ord for Distance {
    fn cmp(&self, other: &Self) -> Ordering {
	self.partial_cmp(other).unwrap()
    }
}


impl Display for Distance {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
	write!(f, "Distance {} (", self.bits)?;
	for b in self.key.0 {
	    write!(f, "{:02X}", b)?;
	}
	write!(f, ")")?;
	Ok(())
    }
}


#[cfg(test)]
mod test {
    use rand::{
	SeedableRng,
	rngs::SmallRng,
    };
    use super::*;

    #[test]
    fn test_key_distance_self() {
	let mut rng = SmallRng::from_entropy();
	let key = Key::from_random(&mut rng);
	let distance = key.distance(&key);
	let bits = distance.bits().into();
	assert!(KAD_BITS == bits, "Distance to self should be all bits zero, not {}", bits);
    }

    #[test]
    fn test_key_distances() {
	let key = Key::default();
	for bits in 0..KAD_BITS {
	    let other = Key::from_fn_bits(|b| b >= bits );
	    let distance = key.distance(&other);
	    let dbits = distance.bits().into();
	    assert!(bits == dbits, "Expected {} bits, but got {}", bits, dbits);
	}
    }

    #[test]
    fn test_key_format() {
	let zero = Key::default();
	assert!("Key (0000000000000000000000000000000000000000000000000000000000000000)" == format!("{}", zero));
	let full = Key::from_fn(|_| 0xFFu8);
	assert!("Key (FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)" ==  format!("{}", full));
    }
}
