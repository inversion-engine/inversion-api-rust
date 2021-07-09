//! InvUniq uuid-like identifier, using base58 alphabet.

use once_cell::sync::Lazy;
use std::sync::Arc;

/// InvUniq uuid-like identifier, using base58 alphabet.
#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct InvUniq(Arc<String>);

static INV_UNIQ_EVT: Lazy<InvUniq> =
    Lazy::new(|| InvUniq(Arc::new("111111111111111111111111".to_string())));

impl Default for InvUniq {
    fn default() -> Self {
        Self::raw_new(PrivKind::Rand)
    }
}

impl InvUniq {
    /// Construct a new completely random uniq
    /// The odds of this being an "evt" type are pretty much nill.
    /// The odds of this being "req"/"res" type are 50 / 50.
    pub fn new_rand() -> Self {
        Self::raw_new(PrivKind::Rand)
    }

    /// Construct a new "evt" type uniq
    /// Note, this isn't really uniq, it is all '1's.
    pub fn new_evt() -> Self {
        INV_UNIQ_EVT.clone()
    }

    /// Construct a new "req" type uniq
    pub fn new_req() -> Self {
        Self::raw_new(PrivKind::FlagOn)
    }

    /// Construct a new "res" type uniq
    pub fn new_res() -> Self {
        Self::raw_new(PrivKind::FlagOff)
    }

    /// Returns `true` if this uniq is "evt" type.
    pub fn is_evt(&self) -> bool {
        self == &*INV_UNIQ_EVT
    }

    /// Returns `true` if this uniq is "req" type.
    /// If this uniq was constructed with `new_rand` this will be random.
    pub fn is_req(&self) -> bool {
        !self.is_evt() && self.is_flag()
    }

    /// Returns `true` if this uniq is "req" type.
    /// If this uniq was constructed with `new_rand` this will be random.
    pub fn is_res(&self) -> bool {
        !self.is_evt() && !self.is_flag()
    }
}

impl std::fmt::Debug for InvUniq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InvUniq:{}", &self.0)
    }
}

impl std::fmt::Display for InvUniq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl AsRef<str> for InvUniq {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// -- private -- //

const B58B: &[u8] =
    b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

const B58: [char; 58] = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
    'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

enum PrivKind {
    Rand,
    FlagOn,
    FlagOff,
}

impl InvUniq {
    fn raw_new(k: PrivKind) -> Self {
        let mut out = nanoid::nanoid!(24, &B58).into_bytes();
        match k {
            PrivKind::Rand => (),
            PrivKind::FlagOn => {
                let idx = B58B.iter().position(|&x| x == out[23]).unwrap();
                if idx < 29 {
                    out[23] = B58B[idx + 29];
                }
            }
            PrivKind::FlagOff => {
                let idx = B58B.iter().position(|&x| x == out[23]).unwrap();
                if idx >= 29 {
                    out[23] = B58B[idx - 29];
                }
            }
        }
        Self(Arc::new(String::from_utf8_lossy(&out).to_string()))
    }

    fn is_flag(&self) -> bool {
        B58B.iter()
            .position(|&x| x == self.0.as_bytes()[23])
            .unwrap()
            >= 29
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniq() {
        let u = InvUniq::default();
        let enc = rmp_serde::to_vec_named(&u).unwrap();
        let dec: InvUniq = rmp_serde::from_read_ref(&enc).unwrap();

        println!("{:?} {}", u, u);
        assert_eq!(u, dec);
    }

    #[test]
    fn test_uniq_flag() {
        let mut got_one_on = false;
        let mut got_one_off = false;
        for _ in 0..200 {
            let id = InvUniq::default();
            if id.is_req() {
                got_one_on = true;
            } else {
                got_one_off = true;
            }
        }
        assert!(got_one_on);
        assert!(got_one_off);
        for _ in 0..200 {
            let id = InvUniq::new_req();
            assert!(id.is_req());
        }
        for _ in 0..200 {
            let id = InvUniq::new_res();
            assert!(id.is_res());
        }
    }
}
