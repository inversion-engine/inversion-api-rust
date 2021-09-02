//! inversion api implementation

use crate::inv_api_spec::*;
use crate::inv_broker::*;
use crate::inv_codec::*;
use crate::inv_error::*;

use once_cell::sync::Lazy;

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
        api_features: Box::new([FeatureDef {
            feature_name: "unstable-triage".into(),
            feature_status: FeatureSpecStatus::Unstable,
        }]),
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

/// A client connected to an Inversion Api Broker
pub struct InvApiClient {
}

type PrivApiSender =
    InvTypedSender<InvApiCodecEvt, InvApiCodecReqRes, InvApiCodecReqRes>;

type PrivApiHandler =
    InvTypedHandler<InvApiCodecEvt, InvApiCodecReqRes, InvApiCodecReqRes>;

impl InvApiClient {
    /// Establish a client connection to a local Inversion Api Broker.
    /// This is probably what you want, but this is just a wrapper for
    /// `new_raw()` if you need to manually establish a client connection.
    pub async fn new(inv_broker: InvBroker) -> InvResult<Self> {
        // TODO safety / version checks
        // let impl_spec = inv_broker.get_inv_api_impl_spec();
        let (raw_sender, raw_handler, raw_close) = inv_broker.bind_to_inv_api_impl_raw().await?;
        Self::new_raw(raw_sender, raw_handler, raw_close).await
    }

    /// Establish a client connection via raw channel handles.
    /// Consider if `new()` was what you actually wanted.
    pub async fn new_raw(
        raw_sender: InvRawSender,
        raw_handler: InvRawHandler,
        raw_close: InvRawClose,
    ) -> InvResult<Self> {
        let (_sender, _handler): (PrivApiSender, PrivApiHandler) =
            upgrade_inv_raw_channel(raw_sender, raw_handler, raw_close);

        unimplemented!()
    }
}
