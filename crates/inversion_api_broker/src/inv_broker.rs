//! inversion broker traits and impl

use crate::inv_id::InvId;
use futures::future::BoxFuture;
use std::future::Future;
use std::sync::Arc;

/// api type -- TODO use inversion-api-spec here, this is a standin
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ApiSpec {
    /// identifier of this api spec
    pub api_id: InvId,

    /// revision of this api spec
    pub api_revision: u32,

    /// title of this api spec
    pub api_title: String,
}

/// inversion broker trait
pub trait AsInvBroker: 'static + Send + Sync {
    /// Register a new api to this broker
    fn register_api(&self, api: ApiSpec) -> BoxFuture<'static, std::io::Result<()>>;
}

/// inversion broker type handle
pub struct InvBroker(pub Arc<dyn AsInvBroker>);

impl AsInvBroker for InvBroker {
    fn register_api(&self, api: ApiSpec) -> BoxFuture<'static, std::io::Result<()>> {
        AsInvBroker::register_api(&*self.0, api)
    }
}

impl InvBroker {
    /// Register a new api to this broker
    pub fn register_api(
        &self,
        api: ApiSpec,
    ) -> impl Future<Output = std::io::Result<()>> + 'static + Send {
        AsInvBroker::register_api(self, api)
    }
}
