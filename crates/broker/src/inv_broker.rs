//! inversion broker traits and impl

use crate::inv_any::InvAny;
use crate::inv_error::*;
use crate::inv_share::InvShare;
use crate::inv_uniq::InvUniq;
use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// Status of a particular Feature Definition in an ApiSpec.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureDef {
    /// The short reference name for this feature.
    pub feature_name: Box<str>,

    /// The implementation status of this feature.
    pub feature_status: FeatureSpecStatus,
}

/// Spec representing an Inversion API.
#[non_exhaustive]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiSpec {
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

impl Default for ApiSpec {
    fn default() -> Self {
        Self {
            api_cat1: "anon".to_string().into_boxed_str(),
            api_cat2: "anon".to_string().into_boxed_str(),
            api_name: format!("{}", InvUniq::new_rand()).into_boxed_str(),
            api_revision: 0,
            api_features: Box::new([]),
        }
    }
}

impl std::fmt::Display for ApiSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "inv.{}.{}.{}.{}",
            self.api_cat1, self.api_cat2, self.api_name, self.api_revision,
        )
    }
}

/// Spec representing an Inversion API implementation.
#[non_exhaustive]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImplSpec {
    /// Api spec this impl is implementing.
    pub api_spec: ApiSpec,

    /// The name of this implementation.
    pub impl_name: Box<str>,

    /// The revision of this implementation.
    pub impl_revision: u32,

    /// feature impl list associated with this spec.
    pub impl_features: Box<[Box<str>]>,
}

impl Default for ImplSpec {
    fn default() -> Self {
        Self {
            api_spec: ApiSpec::default(),
            impl_name: format!("{}", InvUniq::new_rand()).into_boxed_str(),
            impl_revision: 0,
            impl_features: Box::new([]),
        }
    }
}

impl std::fmt::Display for ImplSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.api_spec, self.impl_name, self.impl_revision,
        )
    }
}

/// Closure type for logic to be used to handle
/// the receiving end of a raw channel.
type PrivRawSender = Arc<
    dyn Fn(InvUniq, InvAny) -> BoxFuture<'static, InvResult<()>>
        + 'static
        + Send
        + Sync,
>;

struct PrivRawCleanup {
    raw_close: RawClose,
}

impl Drop for PrivRawCleanup {
    fn drop(&mut self) {
        (self.raw_close)();
    }
}

/// The sender side of a raw channel must be able to invoke the
/// receiver side closure. In order to decouple the timing of
/// specifying the receive logic, and to manage closing the channel,
/// we use this `InvShare<Shared<_>>` type.
type PrivRawSenderFut = InvShare<
    futures::future::Shared<
        BoxFuture<'static, InvResult<(PrivRawSender, Arc<PrivRawCleanup>)>>,
    >,
>;

/// A static callback instance that can be used to close this channel.
/// (Generally, this is used on the receiver side, since it is straight-forward
/// to close the channel from the sender side).
pub type RawClose = Arc<dyn Fn() + 'static + Send + Sync>;

fn run_once<F>(f: F) -> RawClose
where
    F: FnOnce() + 'static + Send + Sync,
{
    let inner = InvShare::new_mutex(f);
    Arc::new(move || {
        if let Some(inner) = inner.extract() {
            inner();
        }
    })
}

/// The raw, low-level sender side of a raw_channel.
#[derive(Clone)]
pub struct RawSender(PrivRawSenderFut);

impl RawSender {
    /// Send a message to the remote end of this channel.
    pub fn send(
        &self,
        id: InvUniq,
        data: InvAny,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        let inner = self.0.clone();
        async move {
            // first, get the Shared<_> type, if we have not been closed
            let raw_sender = match inner.share_ref(|i| Ok(i.clone())) {
                Ok(raw_sender) => raw_sender.clone(),
                Err(_) => {
                    return Err(std::io::ErrorKind::ConnectionReset.into())
                }
            };

            match async move {
                // set up a timeout, incase no-one every calls receiver.handle()
                tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    async move {
                        // wait on the receive logic closure receiver
                        // i.e. someone calls receiver.handle()
                        let (raw_sender, _) = raw_sender.await?;

                        // actually invoke the receiver closure
                        raw_sender(id, data).await
                    },
                )
                .await
                .map_err(|_| InvError::from(std::io::ErrorKind::TimedOut))?
            }
            .await
            {
                Ok(r) => Ok(r),
                Err(e) => {
                    // if we get an error at this layer, we want to close
                    // the channel to free up resources...
                    // Application layer errors should be encoded in the
                    // raw type so they don't trigger this.
                    inner.close();
                    Err(e)
                }
            }
        }
    }

    /// Has this channel been closed?
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Close this channel from the sender side.
    pub fn close(&self) {
        self.0.close();
    }
}

/// The raw, low-level receiver side of a raw_channel.
pub struct RawReceiver(tokio::sync::oneshot::Sender<PrivRawSender>);

impl RawReceiver {
    /// Specify the logic that will be applied on receipt of messages.
    pub fn handle<Fut, F>(self, f: F)
    where
        Fut: Future<Output = InvResult<()>> + 'static + Send,
        F: Fn(InvUniq, InvAny) -> Fut + 'static + Send + Sync,
    {
        // box up this logic into heap space so we can pass it around
        let f: PrivRawSender = Arc::new(move |id, data| f(id, data).boxed());

        // forward this logic to the sender side
        let _ = self.0.send(f);
    }
}

/// Create a raw, low-level channel.
pub fn raw_channel() -> (RawSender, RawReceiver, RawClose) {
    // kill notification incase we're closed before the receive logic is given
    let kill_notify = Arc::new(tokio::sync::Notify::new());

    // setup the channel to forward the receive logic to the sender side
    let (fn_send, fn_recv) = tokio::sync::oneshot::channel::<PrivRawSender>();

    let (inner, inner_init) = InvShare::new_rw_lock_delayed();

    // bundle up a callback for closing this channel
    let raw_close = {
        let inner = inner.clone();
        let kill_notify = kill_notify.clone();
        run_once(move || {
            inner.close();
            kill_notify.notify_waiters();
        })
    };

    // wrap up the receive side so it can be held by multiple clones of
    // the sender side
    let priv_raw_cleanup = Arc::new(PrivRawCleanup {
        raw_close: raw_close.clone(),
    });
    inner_init(async move {
        futures::select_biased! {
            res = fn_recv.fuse() => {
                let s = res.map_err(|_| InvError::from(std::io::ErrorKind::ConnectionReset))?;
                Ok((s, priv_raw_cleanup))
            }
            _ = kill_notify.notified().fuse() => {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
        }
    }.boxed().shared());

    // return the components
    (RawSender(inner), RawReceiver(fn_send), raw_close)
}

struct PrivTypedCleanup {
    close_all: RawClose,
}

impl Drop for PrivTypedCleanup {
    fn drop(&mut self) {
        (self.close_all)();
    }
}

type PrivPendingMap =
    InvShare<HashMap<InvUniq, tokio::sync::oneshot::Sender<InvAny>>>;

struct TypedSenderInner {
    raw_sender: RawSender,
    _cleanup: PrivTypedCleanup,
    pending: PrivPendingMap,
}

/// Send typed events, or make typed requests of the remote api.
pub struct TypedSender<EvtSend, ReqSend, ResSend>
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    for<'de> ResSend: serde::Deserialize<'de> + 'static + Send,
{
    inner: InvShare<TypedSenderInner>,
    _phantom: std::marker::PhantomData<&'static (EvtSend, ReqSend, ResSend)>,
}

impl<EvtSend, ReqSend, ResSend> Clone for TypedSender<EvtSend, ReqSend, ResSend>
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    for<'de> ResSend: serde::Deserialize<'de> + 'static + Send,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<EvtSend, ReqSend, ResSend> TypedSender<EvtSend, ReqSend, ResSend>
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    for<'de> ResSend: serde::Deserialize<'de> + 'static + Send,
{
    /// Emit an event to the remote side of this channel.
    pub fn emit(
        &self,
        data: EvtSend,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        let inner = self.inner.clone();
        async move {
            let raw_sender = inner.share_ref(|i| Ok(i.raw_sender.clone()))?;
            match raw_sender.send(InvUniq::new_evt(), InvAny::new(data)).await {
                Ok(r) => Ok(r),
                Err(e) => {
                    inner.close();
                    Err(e)
                }
            }
        }
    }

    /// Make a request of the remote side of this channel.
    pub fn request(
        &self,
        data: ReqSend,
    ) -> impl Future<Output = InvResult<ResSend>> + 'static + Send {
        let inner = self.inner.clone();
        async move {
            // before we build up the pending instances, check if we're open.
            let (raw_sender, pending) = inner
                .share_ref(|i| Ok((i.raw_sender.clone(), i.pending.clone())))?;

            // build up and insert pending info
            let req_id = InvUniq::new_req();
            let res_id = req_id.as_res();
            let (resp_send, resp_recv) = tokio::sync::oneshot::channel();
            let resp_recv = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                resp_recv,
            );
            let res_id2 = res_id.clone();
            pending.share_mut(move |i, _| {
                i.insert(res_id2, resp_send);
                Ok(())
            })?;

            // setup a cleanup raii guard
            struct Cleanup {
                res_id: InvUniq,
                pending: PrivPendingMap,
            }

            impl Drop for Cleanup {
                fn drop(&mut self) {
                    let res_id = self.res_id.clone();
                    let _ = self.pending.share_mut(move |i, _| {
                        i.remove(&res_id);
                        Ok(())
                    });
                }
            }

            let _cleanup = Cleanup { res_id, pending };

            // send the request and await the response
            raw_sender.send(req_id, InvAny::new(data)).await?;

            let data = resp_recv
                .await
                .map_err(|_| InvError::from(std::io::ErrorKind::TimedOut))?
                .map_err(|_| {
                    InvError::from(std::io::ErrorKind::ConnectionReset)
                })?;

            if let Ok(data) = data.downcast::<ResSend>() {
                Ok(data)
            } else {
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }

    /// Has this bi-directional channel been closed?
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Close this bi-directional channel.
    pub fn close(&self) {
        self.inner.close();
    }
}

type TypedRespondCb<ResRecv> =
    Box<dyn FnOnce(ResRecv) -> BoxFuture<'static, ()> + 'static + Send>;

/// Respond to an incoming typed request from the remote api.
/// Drop this instance to generate a ConnectionReset error on the remote.
pub struct TypedRespond<ResRecv: 'static + Send>(TypedRespondCb<ResRecv>);

impl<ResRecv: 'static + Send> TypedRespond<ResRecv> {
    /// Submit the response to the remote end.
    pub fn respond(
        self,
        data: ResRecv,
    ) -> impl Future<Output = ()> + 'static + Send {
        (self.0)(data)
    }
}

/// Incoming events and requests from the remote api.
pub enum TypedIncoming<
    EvtRecv: 'static + Send,
    ReqRecv: 'static + Send,
    ResRecv: 'static + Send,
> {
    /// An incoming message of type event that does not need direct response.
    Event(EvtRecv),

    /// An incoming message of type request that requires a direct response.
    Request(ReqRecv, TypedRespond<ResRecv>),
}

type PrivTypedSender<EvtRecv, ReqRecv, ResRecv> = Arc<
    dyn Fn(
            TypedIncoming<EvtRecv, ReqRecv, ResRecv>,
        ) -> BoxFuture<'static, InvResult<()>>
        + 'static
        + Send
        + Sync,
>;

/// Specify logic for handling incoming events and requests from the remote api.
pub struct TypedReceiver<EvtRecv, ReqRecv, ResRecv>
where
    for<'de> EvtRecv: serde::Deserialize<'de> + 'static + Send,
    for<'de> ReqRecv: serde::Deserialize<'de> + 'static + Send,
    ResRecv: serde::Serialize + 'static + Send,
{
    raw_sender: RawSender,
    raw_receiver: RawReceiver,
    close_all: RawClose,
    pending: PrivPendingMap,
    _phantom: std::marker::PhantomData<&'static (EvtRecv, ReqRecv, ResRecv)>,
}

impl<EvtRecv, ReqRecv, ResRecv> TypedReceiver<EvtRecv, ReqRecv, ResRecv>
where
    for<'de> EvtRecv: serde::Deserialize<'de> + 'static + Send,
    for<'de> ReqRecv: serde::Deserialize<'de> + 'static + Send,
    ResRecv: serde::Serialize + 'static + Send,
{
    /// Specify the logic that will be applied on receipt of messages.
    pub fn handle<Fut, F>(self, f: F)
    where
        Fut: Future<Output = InvResult<()>> + 'static + Send,
        F: Fn(TypedIncoming<EvtRecv, ReqRecv, ResRecv>) -> Fut
            + 'static
            + Send
            + Sync,
    {
        let Self {
            raw_sender,
            raw_receiver,
            close_all,
            pending,
            ..
        } = self;

        let f: PrivTypedSender<EvtRecv, ReqRecv, ResRecv> =
            Arc::new(move |res| f(res).boxed());

        // raii guard to shutdown sender (everything) if receiver is closed
        struct Cleanup {
            close_all: RawClose,
        }

        impl Drop for Cleanup {
            fn drop(&mut self) {
                (self.close_all)();
            }
        }

        let cleanup = Cleanup { close_all };

        raw_receiver.handle(move |id, data| {
            let _cleanup = &cleanup;
            let raw_sender = raw_sender.clone();
            let f = f.clone();
            let pending = pending.clone();
            async move {
                if id.is_evt() {
                    if let Ok(data) = data.downcast::<EvtRecv>() {
                        f(TypedIncoming::Event(data)).await
                    } else {
                        Err(std::io::ErrorKind::InvalidData.into())
                    }
                } else if id.is_req() {
                    if let Ok(data) = data.downcast::<ReqRecv>() {
                        let respond: TypedRespondCb<ResRecv> =
                            Box::new(move |res| {
                                async move {
                                    let _ = raw_sender
                                        .send(id.as_res(), InvAny::new(res))
                                        .await;
                                }
                                .boxed()
                            });
                        let respond = TypedRespond(respond);
                        f(TypedIncoming::Request(data, respond)).await
                    } else {
                        Err(std::io::ErrorKind::InvalidData.into())
                    }
                } else {
                    // must be a response
                    if let Ok(Some(respond)) =
                        pending.share_mut(|i, _| Ok(i.remove(&id)))
                    {
                        let _ = respond.send(data);
                    }
                    Ok(())
                }
            }
        });
    }
}

/// Given bi-directional raw handles to a remote (one sender, one receiver),
/// set up a typed, request/response enabled high-level channel.
pub fn upgrade_raw_channel<
    EvtSend,
    ReqSend,
    ResSend,
    EvtRecv,
    ReqRecv,
    ResRecv,
>(
    raw_sender: RawSender,
    raw_receiver: RawReceiver,
    raw_recv_close: RawClose,
) -> (
    TypedSender<EvtSend, ReqSend, ResSend>,
    TypedReceiver<EvtRecv, ReqRecv, ResRecv>,
)
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    for<'de> ResSend: serde::Deserialize<'de> + 'static + Send,
    for<'de> EvtRecv: serde::Deserialize<'de> + 'static + Send,
    for<'de> ReqRecv: serde::Deserialize<'de> + 'static + Send,
    ResRecv: serde::Serialize + 'static + Send,
{
    let pending: PrivPendingMap = InvShare::new_mutex(HashMap::new());

    let close_all: RawClose = {
        let raw_sender = raw_sender.clone();
        let pending = pending.clone();
        run_once(move || {
            raw_sender.close();
            raw_recv_close();
            pending.close();
        })
    };

    let priv_typed_cleanup = PrivTypedCleanup {
        close_all: close_all.clone(),
    };

    let typed_sender = TypedSender {
        inner: InvShare::new_rw_lock(TypedSenderInner {
            raw_sender: raw_sender.clone(),
            _cleanup: priv_typed_cleanup,
            pending: pending.clone(),
        }),
        _phantom: std::marker::PhantomData,
    };

    let typed_receiver = TypedReceiver {
        raw_sender,
        raw_receiver,
        close_all,
        pending,
        _phantom: std::marker::PhantomData,
    };

    (typed_sender, typed_receiver)
}

/// Typedef for a TypedSender where all the types are the same.
pub type UnitypedSender<T> = TypedSender<T, T, T>;

/// Typedef for a TypedSender where all the types are the same.
pub type UnitypedReceiver<T> = TypedReceiver<T, T, T>;

/// Delegates to `upgrade_raw_channel` but with the same type for all types.
pub fn unitype_upgrade_raw_channel<T>(
    raw_sender: RawSender,
    raw_receiver: RawReceiver,
    raw_recv_close: RawClose,
) -> (UnitypedSender<T>, UnitypedReceiver<T>)
where
    for<'de> T: serde::Serialize + serde::Deserialize<'de> + 'static + Send,
{
    upgrade_raw_channel(raw_sender, raw_receiver, raw_recv_close)
}

/// inversion broker trait
pub trait AsInvBroker: 'static + Send + Sync {
    /*
    /// Register a new api impl to this broker
    fn register_impl(
        &self,
        api_impl: S,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<()>>;
    */

    /// Bind to a registered api implementation
    fn bind_to_impl_raw(
        &self,
        impl_spec: ImplSpec,
    ) -> BoxFuture<'static, InvResult<(RawSender, RawReceiver, RawClose)>>;
}

// --- old version --- //

/*
/// Function signature for creating a new BoundApi instance.
pub type DynBoundApi<Evt> = Arc<
    dyn Fn(Evt) -> BoxFuture<'static, InvResult<()>> + 'static + Send + Sync,
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
        Fut: Future<Output = InvResult<()>> + 'static + Send,
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
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
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
    dyn Fn(BoundApi<EvtOut>) -> BoxFuture<'static, InvResult<BoundApi<EvtIn>>>
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
        Fut: Future<Output = InvResult<BoundApi<EvtIn>>> + 'static + Send,
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
    ) -> impl Future<Output = InvResult<BoundApi<EvtIn>>> + 'static + Send {
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
pub struct BoundApiHandle<T: 'static + Send>(
    pub BoundApi<InvBrokerMsg>,
    std::marker::PhantomData<&'static T>,
);

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
    ) -> impl Future<Output = InvResult<()>> + 'static + Send
    where
        T: serde::Serialize,
    {
        self.0.emit(InvBrokerMsg::new_evt(InvAny::new(evt)))
    }
}

/// inversion broker trait
pub trait AsInvBroker: 'static + Send + Sync {
    /// Register a new api to this broker
    fn register_api(&self, api: Spec) -> BoxFuture<'static, InvResult<()>>;

    /// Register a new api impl to this broker
    fn register_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<()>>;

    /// Bind to a registered api implementation
    fn bind_to_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<BoundApi<InvBrokerMsg>>>;
}

/// inversion broker type handle
pub struct InvBroker(pub Arc<dyn AsInvBroker>);

impl AsInvBroker for InvBroker {
    fn register_api(&self, api: Spec) -> BoxFuture<'static, InvResult<()>> {
        AsInvBroker::register_api(&*self.0, api)
    }

    fn register_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<()>> {
        AsInvBroker::register_impl(&*self.0, api, api_impl, factory)
    }

    fn bind_to_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<BoundApi<InvBrokerMsg>>> {
        AsInvBroker::bind_to_impl(&*self.0, api, api_impl, evt_out)
    }
}

impl InvBroker {
    /// Register a new api to this broker
    pub fn register_api(
        &self,
        api: Spec,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        AsInvBroker::register_api(self, api)
    }

    /// Register a new api impl to this broker
    pub fn register_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        AsInvBroker::register_impl(self, api, api_impl, factory)
    }

    /// Bind to a registered api implementation
    pub fn bind_to_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> impl Future<Output = InvResult<BoundApi<InvBrokerMsg>>> {
        AsInvBroker::bind_to_impl(self, api, api_impl, evt_out)
    }
}

/// construct a new inversion api broker
pub fn new_broker() -> InvBroker {
    InvBroker(Arc::new(PrivBroker::new()))
}

// -- private -- //


struct ImplRegistry {
    map: HashMap<Spec, BoundApiFactory<InvBrokerMsg, InvBrokerMsg>>,
}

impl ImplRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn check_add_impl(
        &mut self,
        api_impl: Spec,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> InvResult<()> {
        match self.map.entry(api_impl) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(InvError::other("error, attepted to duplicate api_impl"))
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(factory);
                Ok(())
            }
        }
    }

    pub fn check_bind_to_impl(
        &self,
        api_impl: Spec,
    ) -> InvResult<BoundApiFactory<InvBrokerMsg, InvBrokerMsg>> {
        match self.map.get(&api_impl) {
            Some(factory) => Ok(factory.clone()),
            None => Err(std::io::ErrorKind::InvalidInput.into()),
        }
    }
}

struct ApiRegistry {
    map: HashMap<Spec, ImplRegistry>,
}

impl ApiRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn check_add_api(&mut self, api: Spec) -> InvResult<()> {
        match self.map.entry(api) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(InvError::other("error, attepted to duplicate api_spec"))
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(ImplRegistry::new());
                Ok(())
            }
        }
    }

    pub fn check_add_impl(
        &mut self,
        api: Spec,
        api_impl: Spec,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> InvResult<()> {
        match self.map.get_mut(&api) {
            Some(map) => {
                map.check_add_impl(api_impl, factory)?;
                Ok(())
            }
            None => Err(InvError::other(format!(
                "invalid api_spec, register it first {:?} not in {:?}",
                api,
                self.map.keys().collect::<Vec<_>>(),
            ))),
        }
    }

    pub fn check_bind_to_impl(
        &self,
        api: Spec,
        api_impl: Spec,
    ) -> InvResult<BoundApiFactory<InvBrokerMsg, InvBrokerMsg>> {
        match self.map.get(&api) {
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
    fn register_api(&self, api: Spec) -> BoxFuture<'static, InvResult<()>> {
        let r = self
            .0
            .share_mut(move |i, _| i.api_registry.check_add_api(api));
        async move { r }.boxed()
    }

    fn register_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        factory: BoundApiFactory<InvBrokerMsg, InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<()>> {
        let r = self.0.share_mut(move |i, _| {
            i.api_registry.check_add_impl(api, api_impl, factory)
        });
        async move { r }.boxed()
    }

    fn bind_to_impl(
        &self,
        api: Spec,
        api_impl: Spec,
        evt_out: BoundApi<InvBrokerMsg>,
    ) -> BoxFuture<'static, InvResult<BoundApi<InvBrokerMsg>>> {
        let factory = self.0.share_mut(move |i, _| {
            i.api_registry.check_bind_to_impl(api, api_impl)
        });
        async move {
            let factory = factory?;
            factory.bind(evt_out).await
        }
        .boxed()
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::inv_id::InvId;
    //use std::sync::atomic;

    #[test]
    fn test_specs() {
        let spec = ImplSpec {
            api_spec: ApiSpec {
                api_features: Box::new([
                    FeatureDef {
                        feature_name: "readRaw".to_string().into_boxed_str(),
                        feature_status: FeatureSpecStatus::Deprecated,
                    },
                    FeatureDef {
                        feature_name: "readBytes".to_string().into_boxed_str(),
                        feature_status: FeatureSpecStatus::Stable,
                    },
                    FeatureDef {
                        feature_name: "writeBytes".to_string().into_boxed_str(),
                        feature_status: FeatureSpecStatus::Unstable,
                    },
                ]),
                ..Default::default()
            },
            impl_features: Box::new(["readBytes".to_string().into_boxed_str()]),
            ..Default::default()
        };
        println!("{}: {:#?}", spec, spec);
        println!("{}", serde_yaml::to_string(&spec).unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_raw_channel() {
        // create the unidirectional channel
        let (send, recv, close) = raw_channel();

        // add the handle logic
        recv.handle(move |id, data| {
            let close = close.clone();
            async move {
                let data = data.downcast::<isize>().unwrap();
                println!("{:?} {:?}", id, data);
                assert_eq!("evt", &format!("{}", id));
                assert_eq!(42, data);
                // close the channel after a single event
                close();
                Ok(())
            }
        });

        // send the event
        send.send(InvUniq::new_evt(), InvAny::new(42_isize))
            .await
            .unwrap();

        // should be closed
        assert!(send.is_closed());

        // now closed, we should error out
        assert!(send
            .send(InvUniq::new_evt(), InvAny::new(42_isize))
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_typed_channel() {
        let (snd1, recv2, c2) = raw_channel();
        let (snd2, recv1, c1) = raw_channel();

        let (snd1, recv1) = unitype_upgrade_raw_channel::<u32>(snd1, recv1, c1);

        recv1.handle(move |incoming| async move {
            match incoming {
                TypedIncoming::Event(evt) => {
                    println!("evt: {:?}", evt);
                    assert_eq!(69, evt);
                }
                TypedIncoming::Request(data, respond) => {
                    respond.respond(data + 1).await;
                }
            }
            Ok(())
        });

        let (snd2, recv2) = unitype_upgrade_raw_channel::<u32>(snd2, recv2, c2);

        recv2.handle(move |incoming| async move {
            match incoming {
                TypedIncoming::Event(evt) => {
                    println!("evt: {:?}", evt);
                    assert_eq!(42, evt);
                }
                TypedIncoming::Request(data, respond) => {
                    respond.respond(data - 1).await;
                }
            }
            Ok(())
        });

        snd1.emit(42).await.unwrap();
        snd2.emit(69).await.unwrap();

        let res = snd1.request(42).await.unwrap();
        println!("res: {:?}", res);
        assert_eq!(41, res);

        let res = snd2.request(42).await.unwrap();
        println!("res: {:?}", res);
        assert_eq!(43, res);
    }

    /*
    #[tokio::test(flavor = "multi_thread")]
    async fn test_inv_broker() {
        let api = Spec::new(InvId::new_anon());

        let api_impl = Spec::new(InvId::new_anon());

        let broker = new_broker();

        broker.register_api(api.clone()).await.unwrap();

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
            .register_impl(api.clone(), api_impl.clone(), factory)
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

        let evt = broker.bind_to_impl(api, api_impl, print_res).await.unwrap();
        let msg = InvBrokerMsg::new_evt(InvAny::new(42));
        evt.emit(msg).await.unwrap();
        assert_eq!(43, res.load(atomic::Ordering::SeqCst));
    }
    */
}
