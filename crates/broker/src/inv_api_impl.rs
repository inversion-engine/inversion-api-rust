//! inversion api implementation

use once_cell::sync::Lazy;
use crate::inv_api_spec::*;

use std::sync::Arc;

/// Inversion Api Spec
/// This is a bit self-referential, but this spec defines the api
/// for actually working with inversions apis.
///
/// ```
/// use inversion_api_broker::inv_api_impl::*;
/// assert_eq!("data.bus.inv-api.0", &*INV_API_SPEC.to_api_id());
/// ```
pub static INV_API_SPEC: Lazy<ApiSpec> = Lazy::new(|| {
    Arc::new(ApiSpecInner {
        api_cat1: "data".into(),
        api_cat2: "bus".into(),
        api_name: "inv-api".into(),
        api_revision: 0,
        api_features: Box::new([
            FeatureDef {
                feature_name: "unstable-triage".into(),
                feature_status: FeatureSpecStatus::Unstable,
            },
        ]),
    })
});

/// Inversion Api Impl Spec
/// This is a bit self-referential, but this spec defines the implementation
/// for actually working with inversion apis.
///
/// ```
/// use inversion_api_broker::inv_api_impl::*;
/// assert_eq!("data.bus.inv-api.0.inv-api.0", &*INV_API_IMPL.to_impl_id());
/// ```
pub static INV_API_IMPL: Lazy<ImplSpec> = Lazy::new(|| {
    Arc::new(ImplSpecInner {
        api_spec: INV_API_SPEC.clone(),
        impl_name: "inv-api".into(),
        impl_revision: 0,
        impl_features: Box::new(["unstable-triage".into()]),
    })
});
