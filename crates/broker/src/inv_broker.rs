//! inversion broker traits and impl

use crate::inv_any::InvAny;
use crate::inv_id::InvId;
use crate::inv_share::InvShare;
use crate::inv_uniq::InvUniq;
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

/// Function signature for creating a new BoundApi instance.
pub type DynBoundApi<Evt> = Arc<
    dyn Fn(Evt) -> BoxFuture<'static, std::io::Result<()>>
        + 'static
        + Send
        + Sync,
>;

/// Handle for publishing an event to some other logical actor.
pub struct BoundApi<Evt: 'static + Send>(InvShare<DynBoundApi<Evt>>);

impl<Evt: 'static + Send> Clone for BoundApi<Evt> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Evt: 'static + Send> BoundApi<Evt> {
    /// Construct a new bound api handle via trait callback
    pub fn new<Fut, F>(f: F) -> Self
    where
        Fut: Future<Output = std::io::Result<()>> + 'static + Send,
        F: Fn(Evt) -> Fut + 'static + Send + Sync,
    {
        let f: DynBoundApi<Evt> = Arc::new(move |evt| f(evt).boxed());
        Self::from_dyn(f)
    }

    /// Construct a new bound api handle via boxed callback
    pub fn from_dyn(f: DynBoundApi<Evt>) -> Self {
        Self(InvShare::new_rw_lock(f))
    }

    /// Has this channel been closed (underlying callback dropped)?
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Explicitly drop the underlying callback handle.
    pub fn close(&self) {
        self.0.close()
    }

    /// Emit an event to the remote logical actor.
    /// If that actor returns a `ConnectionAborted` error, the
    /// underlying callback handle will be dropped.
    pub fn emit(
        &self,
        evt: Evt,
    ) -> impl Future<Output = std::io::Result<()>> + 'static + Send {
        let inner = self.0.clone();
        async move {
            let f = inner.share_ref(|i| Ok(i.clone()))?;
            match f(evt).await {
                Ok(r) => Ok(r),
                Err(e) => {
                    if matches!(e.kind(), std::io::ErrorKind::ConnectionAborted)
                    {
                        inner.close();
                    }
                    Err(e)
                }
            }
        }
    }
}

/// Function signature for creating a new BoundApiFactory instance.
pub type DynBoundApiFactory<EvtIn, EvtOut> = Arc<
    dyn Fn(
            BoundApi<EvtOut>,
        ) -> BoxFuture<'static, std::io::Result<BoundApi<EvtIn>>>
        + 'static
        + Send
        + Sync,
>;

/// A broker api factory is like a virtual channel.
/// Events can be emitted or received.
pub struct BoundApiFactory<EvtIn, EvtOut>(
    InvShare<DynBoundApiFactory<EvtIn, EvtOut>>,
)
where
    EvtIn: 'static + Send,
    EvtOut: 'static + Send;

impl<EvtIn, EvtOut> Clone for BoundApiFactory<EvtIn, EvtOut>
where
    EvtIn: 'static + Send,
    EvtOut: 'static + Send,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<EvtIn, EvtOut> BoundApiFactory<EvtIn, EvtOut>
where
    EvtIn: 'static + Send,
    EvtOut: 'static + Send,
{
    /// Construct a new bound api handle via trait callback
    pub fn new<Fut, F>(f: F) -> Self
    where
        Fut: Future<Output = std::io::Result<BoundApi<EvtIn>>> + 'static + Send,
        F: Fn(BoundApi<EvtOut>) -> Fut + 'static + Send + Sync,
    {
        let f: DynBoundApiFactory<EvtIn, EvtOut> =
            Arc::new(move |evt_out| f(evt_out).boxed());
        Self::from_dyn(f)
    }

    /// Construct a new bound api handle via boxed callback
    pub fn from_dyn(f: DynBoundApiFactory<EvtIn, EvtOut>) -> Self {
        Self(InvShare::new_rw_lock(f))
    }

    /// Has this channel been closed (underlying callback dropped)?
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Explicitly drop the underlying callback handle.
    pub fn close(&self) {
        // TODO - we must also close both sides
        // of all ALL CHILDREN BoundApi instances.
        self.0.close()
    }

    /// Bind an api with this BoundApiFactory instance.
    pub fn bind(
        &self,
        evt_out: BoundApi<EvtOut>,
    ) -> impl Future<Output = std::io::Result<BoundApi<EvtIn>>> + 'static + Send
    {
        let inner = self.0.clone();
        async move {
            let f = inner.share_ref(|i| Ok(i.clone()))?;
            match f(evt_out).await {
                Ok(r) => Ok(r),
                Err(e) => {
                    if matches!(e.kind(), std::io::ErrorKind::ConnectionAborted)
                    {
                        // TODO - we must also close both sides
                        // of all ALL CHILDREN BoundApi instances.
                        inner.close();
                    }
                    Err(e)
                }
            }
        }
    }
}

/// inversion broker message
#[non_exhaustive]
pub struct InvBrokerMsg {
    /// Message Id
    pub id: InvUniq,

    /// Message Content
    pub msg: InvAny,

    // TODO - add capability checking data
}

impl InvBrokerMsg {
    /// construct a new "evt" type message
    pub fn new_evt(msg: InvAny) -> Self {
        Self {
            id: InvUniq::new_evt(),
            msg,
        }
    }

    /// construct a new "req" type message
    pub fn new_req(msg: InvAny) -> Self {
        Self {
            id: InvUniq::new_req(),
            msg,
        }
    }

    /// construct a new "res" type message
    pub fn new_res(msg: InvAny) -> Self {
        Self {
            id: InvUniq::new_res(),
            msg,
        }
    }

    /// `true` if this message is "evt" type.
    pub fn is_evt(&self) -> bool {
        self.id.is_evt()
    }

    /// `true` if this message is "req" type.
    pub fn is_req(&self) -> bool {
        self.id.is_req()
    }

    /// `true` if this message is "res" type.
    pub fn is_res(&self) -> bool {
        self.id.is_res()
    }

    /// extract the message content from this struct.
    pub fn into_msg(self) -> InvAny {
        self.msg
    }
}

/// Typed wrapper around a generic InvBroker api.
pub struct BoundApiHandle<T: 'static + Send>(pub BoundApi<InvBrokerMsg>, std::marker::PhantomData<&'static T>);

impl<T: 'static + Send> Clone for BoundApiHandle<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), std::marker::PhantomData)
    }
}

impl<T: 'static + Send> BoundApiHandle<T> {
    /// Construct a new BoundApiHandle
    pub fn new(inner: BoundApi<InvBrokerMsg>) -> Self {
        Self(inner, std::marker::PhantomData)
    }

    /// Has this channel been closed?
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Explicitly close this channel.
    pub fn close(&self) {
        self.0.close()
    }

    /// Emit an event to the remote logical actor.
    pub fn emit(
        &self,
        evt: T,
    ) -> impl Future<Output = std::io::Result<()>> + 'static + Send
    where
        T: serde::Serialize,
    {
        self.0.emit(InvBrokerMsg::new_evt(InvAny::new(evt)))
    }
}

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
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, std::io::Result<()>>;

    /// Bind to a registered api implementation
    fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> BoxFuture<'static, std::io::Result<BoundApi<InvBrokerMsg>>>;
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
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, std::io::Result<()>> {
        AsInvBroker::register_impl(&*self.0, api_impl, factory)
    }

    fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> BoxFuture<'static, std::io::Result<BoundApi<InvBrokerMsg>>> {
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
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> impl Future<Output = std::io::Result<()>> + 'static + Send {
        AsInvBroker::register_impl(self, api_impl, factory)
    }

    /// Bind to a registered api implementation
    pub fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> impl Future<Output = std::io::Result<BoundApi<InvBrokerMsg>>> {
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
    map: HashMap<ApiImpl, BoundApiFactory<InvBrokerMsg, InvBrokerMsg>>,
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
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
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
    ) -> std::io::Result<BoundApiFactory<InvBrokerMsg, InvBrokerMsg>> {
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
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
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
    ) -> std::io::Result<BoundApiFactory<InvBrokerMsg, InvBrokerMsg>> {
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
        Self(Arc::new(crate::inv_share::InvShare::new_rw_lock(
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
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, std::io::Result<()>> {
        let r = self.0.share_mut(move |i, _| {
            i.api_registry.check_add_impl(api_impl, factory)
        });
        async move { r }.boxed()
    }

    fn bind_to_impl(
        &self,
        api_impl: ApiImpl,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> BoxFuture<'static, std::io::Result<BoundApi<InvBrokerMsg>>> {
        let factory = self
            .0
            .share_mut(move |i, _| i.api_registry.check_bind_to_impl(api_impl));
        async move {
            let factory = factory?;
            factory.bind(evt_out).await
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

        let factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg> =
            BoundApiFactory::new(|evt_out| {
                let evt_in: BoundApi<InvBrokerMsg> =
                    BoundApi::new(move |evt_in: InvBrokerMsg| {
                        let evt_out = evt_out.clone();
                        async move {
                            let input: usize = evt_in.into_msg().downcast()?;
                            let msg =
                                InvBrokerMsg::new_evt(InvAny::new(input + 1));
                            evt_out.emit(msg).await?;
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
        let print_res: BoundApi<InvBrokerMsg> =
            BoundApi::new(move |evt: InvBrokerMsg| {
                let output: usize = evt.into_msg().downcast().unwrap();
                println!("got: {}", output);
                res2.store(output, atomic::Ordering::SeqCst);
                async move { Ok(()) }.boxed()
            });

        let evt = broker.bind_to_impl(api_impl, print_res).await.unwrap();
        let msg = InvBrokerMsg::new_evt(InvAny::new(42));
        evt.emit(msg).await.unwrap();
        assert_eq!(43, res.load(atomic::Ordering::SeqCst));
    }
}
