//! Uniq uuid-like identifier, using base58 alphabet.

use std::sync::Arc;

/// Uniq uuid-like identifier, using base58 alphabet.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct Uniq(Arc<String>);

impl Default for Uniq {
    fn default() -> Self {
        pub const B58: [char; 58] = [
            '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
            'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S',
            'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z',
        ];
        Self(Arc::new(nanoid::nanoid!(24, &B58)))
    }
}

impl std::fmt::Debug for Uniq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Uniq:{}", &self.0)
    }
}

impl std::fmt::Display for Uniq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniq() {
        let u = Uniq::default();
        let enc = rmp_serde::to_vec_named(&u).unwrap();
        let dec: Uniq = rmp_serde::from_read_ref(&enc).unwrap();

        println!("{:?} {}", u, u);
        assert_eq!(u, dec);
    }
}
