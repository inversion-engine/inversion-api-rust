//! inversion api spec definitions

use crate::inv_uniq::*;

use std::sync::Arc;

/// Status of a particular Feature Definition in an ApiSpec.
#[derive(
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[serde(rename_all = "camelCase")]
pub enum FeatureSpecStatus {
    /// This is a proposed, unstable feature.
    /// It may or may not function, and the api
    /// may change between revisions.
    Unstable,

    /// This is a stable feature.
    /// Implementations claiming this API SPEC REVISION
    /// *MUST* implement this feature as defined.
    Stable,

    /// This previously stable feature is no longer required.
    /// Implementations claiming this API SPEC REVISION
    /// *MAY* report this feature as not implemented.
    Deprecated,
}

/// Feature Definition for ApiSpec.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FeatureDef {
    /// The short reference name for this feature.
    pub feature_name: Box<str>,

    /// The implementation status of this feature.
    pub feature_status: FeatureSpecStatus,
}

impl serde::Serialize for FeatureDef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = (&self.feature_name, &self.feature_status);
        s.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FeatureDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let d: (Box<str>, FeatureSpecStatus) =
            serde::Deserialize::deserialize(deserializer)?;
        Ok(Self {
            feature_name: d.0,
            feature_status: d.1,
        })
    }
}

impl std::fmt::Display for FeatureDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap())
    }
}

impl std::fmt::Debug for FeatureDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(&self).unwrap())
    }
}

/// Inversion Api ApiSpecId type.
pub type ApiSpecId = Arc<str>;

/// Spec representing an Inversion API.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApiSpecInner {
    /// top level categorization.
    pub api_cat1: Box<str>,

    /// sub level categorization.
    pub api_cat2: Box<str>,

    /// api name.
    pub api_name: Box<str>,

    /// api revision.
    pub api_revision: u32,

    /// feature list associated with this spec.
    pub api_features: Box<[FeatureDef]>,
}

impl ApiSpecInner {
    /// Generate the canonical ApiSpecId for this ApiSpec
    pub fn to_api_id(&self) -> ApiSpecId {
        format!(
            "{}.{}.{}.{}",
            self.api_cat1, self.api_cat2, self.api_name, self.api_revision,
        )
        .into_boxed_str()
        .into()
    }
}

impl Default for ApiSpecInner {
    fn default() -> Self {
        Self {
            api_cat1: "anon".into(),
            api_cat2: "anon".into(),
            api_name: format!("{}", InvUniq::new_rand()).into(),
            api_revision: 0,
            api_features: Box::new([]),
        }
    }
}

impl serde::Serialize for ApiSpecInner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = (
            "invApiSpec",
            &self.api_cat1,
            &self.api_cat2,
            &self.api_name,
            &self.api_revision,
            &self.api_features,
        );
        s.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ApiSpecInner {
    #[allow(clippy::type_complexity)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let d: (
            Box<str>,
            Box<str>,
            Box<str>,
            Box<str>,
            u32,
            Box<[FeatureDef]>,
        ) = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self {
            api_cat1: d.1,
            api_cat2: d.2,
            api_name: d.3,
            api_revision: d.4,
            api_features: d.5,
        })
    }
}

impl std::fmt::Display for ApiSpecInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap())
    }
}

impl std::fmt::Debug for ApiSpecInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(&self).unwrap())
    }
}

/// Inversion Api ApiSpec type.
pub type ApiSpec = Arc<ApiSpecInner>;

/// Inversion Api ImplSpecId type.
pub type ImplSpecId = Arc<str>;

/// Spec representing an Inversion API implementation.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImplSpecInner {
    /// Api spec this impl is implementing.
    pub api_spec: ApiSpec,

    /// The name of this implementation.
    pub impl_name: Box<str>,

    /// The revision of this implementation.
    pub impl_revision: u32,

    /// feature impl list associated with this spec.
    pub impl_features: Box<[Box<str>]>,
}

impl ImplSpecInner {
    /// Generate the canonical ImplSpecId for this ImplSpec
    pub fn to_impl_id(&self) -> ImplSpecId {
        format!(
            "{}.{}.{}",
            self.api_spec.to_api_id(),
            self.impl_name,
            self.impl_revision,
        )
        .into_boxed_str()
        .into()
    }
}

impl Default for ImplSpecInner {
    fn default() -> Self {
        Self {
            api_spec: ApiSpec::default(),
            impl_name: format!("{}", InvUniq::new_rand()).into(),
            impl_revision: 0,
            impl_features: Box::new([]),
        }
    }
}

impl serde::Serialize for ImplSpecInner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = (
            "invImplSpec",
            &self.api_spec,
            &self.impl_name,
            &self.impl_revision,
            &self.impl_features,
        );
        s.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ImplSpecInner {
    #[allow(clippy::type_complexity)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let d: (Box<str>, ApiSpec, Box<str>, u32, Box<[Box<str>]>) =
            serde::Deserialize::deserialize(deserializer)?;
        Ok(Self {
            api_spec: d.1,
            impl_name: d.2,
            impl_revision: d.3,
            impl_features: d.4,
        })
    }
}

impl std::fmt::Display for ImplSpecInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap())
    }
}

impl std::fmt::Debug for ImplSpecInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(&self).unwrap())
    }
}

/// Inversion Api ImplSpec type.
pub type ImplSpec = Arc<ImplSpecInner>;
