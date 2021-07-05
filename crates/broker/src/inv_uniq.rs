//! InvUniq uuid-like identifier, using base58 alphabet.

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

impl Default for InvUniq {
    fn default() -> Self {
        const B58: [char; 58] = [
            '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
            'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S',
            'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z',
        ];
        Self(Arc::new(nanoid::nanoid!(24, &B58)))
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
}
