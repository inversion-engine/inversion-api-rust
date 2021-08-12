//! inversion id type

use std::sync::Arc;

/// inversion id type
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
pub struct InvId(Arc<str>);

impl InvId {
    /// inversion id constructor
    pub fn new_raw(s: Box<str>) -> Self {
        Self(s.into())
    }

    /// construct a new anonymous InvId
    pub fn new_anon() -> Self {
        Self::new_categorized(
            "anon",
            "anon",
            crate::inv_uniq::InvUniq::default().as_ref(),
        )
    }

    /// construct a new category prefixed InvId
    pub fn new_categorized(
        prefix_1: &str,
        prefix_2: &str,
        identity: &str,
    ) -> Self {
        Self::new_raw(
            format!("{}.{}.{}", prefix_1, prefix_2, identity).into_boxed_str(),
        )
    }
}

impl std::fmt::Debug for InvId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InvId:{}", self.0)
    }
}

impl std::fmt::Display for InvId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for InvId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inv_id() {
        println!("{}", InvId::new_anon());
        println!("{}", InvId::new_categorized("net", "rpc", "json"));
    }
}
