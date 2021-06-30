//! inversion broker traits and impl

use crate::inv_any::InvAny;
use crate::inv_id::InvId;
use futures::future::{BoxFuture, FutureExt};
use std::future::Future;
use std::sync::Arc;

/// api type -- TODO use inversion-api-spec here, this is a standin
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ApiSpecInner {
    /// identifier of this api spec
    pub api_id: InvId,

    /// revision of this api spec
    pub api_revision: u32,

    /// title of this api spec
    pub api_title: String,
}

/// api type -- TODO use inversion-api-spec here, this is a standin
pub type ApiSpec = Arc<ApiSpecInner>;

/// impl type -- TODO use inversion-api-spec here, this is a standin
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ApiImplInner {
    /// identifier of this api spec
    pub api_spec: ApiSpec,

    /// identifier of this api impl
    pub impl_id: InvId,

    /// revision of this api impl
    pub impl_revision: u32,

    /// title of this api impl
    pub impl_title: String,
}

/// impl type -- TODO use inversion-api-spec here, this is a standin
pub type ApiImpl = Arc<ApiImplInner>;

/// Publish one event to the remote end of a broker api
pub type BoundApi<Evt> = Arc<
    dyn Fn(Evt) -> BoxFuture<'static, std::io::Result<()>>
        + 'static
        + Send
        + Sync,
>;

/// A broker api factory is like a virtual channel.
/// Events can be emitted or received.
pub type BoundApiFactory<EvtIn, EvtOut> = Arc<
    dyn Fn(
            BoundApi<EvtOut>,
        ) -> BoxFuture<'static, std::io::Result<BoundApi<EvtIn>>>
        + 'static
        + Send
        + Sync,
>;

/// inversion broker trait
pub trait AsInvBroker: 'static + Send + Sync {
    /// Register a new api to this broker
    fn register_api(
        &self,
        api: ApiSpec,
    ) -> BoxFuture<'static, std::io::Result<()>>;

    /// Register a new api impl to this broker
    fn register_impl(
        &self,
        api_impl: ApiImpl,
        factory: BoundApiFactory<InvAny, InvAny>,
    ) -> BoxFuture<'static, std::io::Result<()>>;

    /// Bind to a registered api implementation
    fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvAny>,
    ) -> BoxFuture<'static, std::io::Result<BoundApi<InvAny>>>;
}

/// inversion broker type handle
pub struct InvBroker(pub Arc<dyn AsInvBroker>);

impl AsInvBroker for InvBroker {
    fn register_api(
        &self,
        api: ApiSpec,
    ) -> BoxFuture<'static, std::io::Result<()>> {
        AsInvBroker::register_api(&*self.0, api)
    }

    fn register_impl(
        &self,
        api_impl: ApiImpl,
        factory: BoundApiFactory<InvAny, InvAny>,
    ) -> BoxFuture<'static, std::io::Result<()>> {
        AsInvBroker::register_impl(&*self.0, api_impl, factory)
    }

    fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvAny>,
    ) -> BoxFuture<'static, std::io::Result<BoundApi<InvAny>>> {
        AsInvBroker::bind_to_impl(&*self.0, api_impl, evt_out)
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

    /// Register a new api impl to this broker
    pub fn register_impl(
        &self,
        api_impl: ApiImpl,
        factory: BoundApiFactory<InvAny, InvAny>,
    ) -> impl Future<Output = std::io::Result<()>> + 'static + Send {
        AsInvBroker::register_impl(self, api_impl, factory)
    }

    /// Bind to a registered api implementation
    pub fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvAny>,
    ) -> impl Future<Output = std::io::Result<BoundApi<InvAny>>> {
        AsInvBroker::bind_to_impl(self, api_impl, evt_out)
    }
}

/// construct a new inversion api broker
pub fn new_broker() -> InvBroker {
    InvBroker(Arc::new(PrivBroker::new()))
}

// -- private -- //

use std::collections::HashMap;

struct ImplRegistry {
    map: HashMap<ApiImpl, BoundApiFactory<InvAny, InvAny>>,
}

impl ImplRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn check_add_impl(
        &mut self,
        api_impl: ApiImpl,
        factory: BoundApiFactory<InvAny, InvAny>,
    ) -> std::io::Result<()> {
        match self.map.entry(api_impl) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "error, attepted to duplicate api_impl",
                ))
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(factory);
                Ok(())
            }
        }
    }

    pub fn check_bind_to_impl(
        &self,
        api_impl: ApiImpl,
    ) -> std::io::Result<BoundApiFactory<InvAny, InvAny>> {
        match self.map.get(&api_impl) {
            Some(factory) => Ok(factory.clone()),
            None => Err(std::io::ErrorKind::InvalidInput.into()),
        }
    }
}

struct ApiRegistry {
    map: HashMap<ApiSpec, ImplRegistry>,
}

impl ApiRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn check_add_api(&mut self, api: ApiSpec) -> std::io::Result<()> {
        match self.map.entry(api) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "error, attepted to duplicate api_spec",
                ))
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(ImplRegistry::new());
                Ok(())
            }
        }
    }

    pub fn check_add_impl(
        &mut self,
        api_impl: ApiImpl,
        factory: BoundApiFactory<InvAny, InvAny>,
    ) -> std::io::Result<()> {
        match self.map.get_mut(&api_impl.api_spec) {
            Some(map) => {
                map.check_add_impl(api_impl, factory)?;
                Ok(())
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "invalid api_spec, register it first",
            )),
        }
    }

    pub fn check_bind_to_impl(
        &self,
        api_impl: ApiImpl,
    ) -> std::io::Result<BoundApiFactory<InvAny, InvAny>> {
        match self.map.get(&api_impl.api_spec) {
            Some(map) => map.check_bind_to_impl(api_impl),
            None => Err(std::io::ErrorKind::InvalidInput.into()),
        }
    }
}

struct PrivBrokerInner {
    api_registry: ApiRegistry,
}

impl PrivBrokerInner {
    pub fn new() -> Self {
        Self {
            api_registry: ApiRegistry::new(),
        }
    }
}

struct PrivBroker(Arc<crate::inv_share::InvShare<PrivBrokerInner>>);

impl PrivBroker {
    fn new() -> Self {
        Self(Arc::new(crate::inv_share::InvShare::new(
            PrivBrokerInner::new(),
        )))
    }
}

impl AsInvBroker for PrivBroker {
    fn register_api(
        &self,
        api: ApiSpec,
    ) -> BoxFuture<'static, std::io::Result<()>> {
        let r = self
            .0
            .share_mut(move |i, _| i.api_registry.check_add_api(api));
        async move { r }.boxed()
    }

    fn register_impl(
        &self,
        api_impl: ApiImpl,
        factory: BoundApiFactory<InvAny, InvAny>,
    ) -> BoxFuture<'static, std::io::Result<()>> {
        let r = self.0.share_mut(move |i, _| {
            i.api_registry.check_add_impl(api_impl, factory)
        });
        async move { r }.boxed()
    }

    fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvAny>,
    ) -> BoxFuture<'static, std::io::Result<BoundApi<InvAny>>> {
        let factory = self
            .0
            .share_mut(move |i, _| i.api_registry.check_bind_to_impl(api_impl));
        async move {
            let factory = factory?;
            factory(evt_out).await
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_inv_broker() {
        let api_spec = Arc::new(ApiSpecInner {
            api_id: InvId::new_anon(),
            api_revision: 0,
            api_title: "".to_string(),
        });

        let api_impl = Arc::new(ApiImplInner {
            api_spec: api_spec.clone(),
            impl_id: InvId::new_anon(),
            impl_revision: 0,
            impl_title: "".to_string(),
        });

        let broker = new_broker();

        broker.register_api(api_spec.clone()).await.unwrap();

        let factory: BoundApiFactory<InvAny, InvAny> = Arc::new(|evt_out| {
            let evt_in: BoundApi<InvAny> = Arc::new(move |evt_in| {
                let evt_out = evt_out.clone();
                async move {
                    let input: usize = evt_in.downcast()?;
                    evt_out(InvAny::new(input + 1)).await?;
                    Ok(())
                }
                .boxed()
            });
            async move { Ok(evt_in) }.boxed()
        });

        broker
            .register_impl(api_impl.clone(), factory)
            .await
            .unwrap();

        let res = Arc::new(atomic::AtomicUsize::new(0));
        let res2 = res.clone();
        let print_res: BoundApi<InvAny> = Arc::new(move |evt| {
            let output: usize = evt.downcast().unwrap();
            println!("got: {}", output);
            res2.store(output, atomic::Ordering::SeqCst);
            async move { Ok(()) }.boxed()
        });

        let evt = broker.bind_to_impl(api_impl, print_res).await.unwrap();
        evt(InvAny::new(42)).await.unwrap();
        assert_eq!(43, res.load(atomic::Ordering::SeqCst));
    }
}
