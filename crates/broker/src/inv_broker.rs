//! inversion broker traits and impl

use crate::inv_any::InvAny;
use crate::inv_error::*;
use crate::inv_share::InvShare;
use crate::inv_uniq::InvUniq;
use futures::future::{BoxFuture, FutureExt};
use std::future::Future;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

trait PrivAsSpec: 'static + Send + Sync {
    fn as_any(&self) -> &(dyn std::any::Any + 'static + Send + Sync);
    fn obj_debug(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
    fn obj_eq(&self, oth: &dyn PrivAsSpec) -> bool;
    fn obj_partial_cmp(
        &self,
        oth: &dyn PrivAsSpec,
    ) -> Option<std::cmp::Ordering>;
    fn obj_cmp(&self, oth: &dyn PrivAsSpec) -> std::cmp::Ordering;
    fn obj_hash(&self, state: &mut dyn std::hash::Hasher);
}

/// Type erased Spec type, to be used both for ApiSpec and ImplSpec.
#[derive(Clone)]
pub struct Spec(Arc<dyn PrivAsSpec>);

impl std::fmt::Debug for Spec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.obj_debug(f)
    }
}

impl PartialEq for Spec {
    fn eq(&self, oth: &Self) -> bool {
        self.0.obj_eq(&*oth.0)
    }
}

impl Eq for Spec {}

impl PartialOrd for Spec {
    fn partial_cmp(&self, oth: &Self) -> Option<std::cmp::Ordering> {
        self.0.obj_partial_cmp(&*oth.0)
    }
}

impl Ord for Spec {
    fn cmp(&self, oth: &Self) -> std::cmp::Ordering {
        self.0.obj_cmp(&*oth.0)
    }
}

impl std::hash::Hash for Spec {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.obj_hash(state);
    }
}

impl Spec {
    /// Generate a new type-erased "Spec" instance from a compatible
    /// concrete type.
    pub fn new<T>(t: T) -> Self
    where
        T: 'static
            + std::fmt::Debug
            + std::hash::Hash
            + PartialEq
            + Eq
            + PartialOrd
            + Ord
            + Send
            + Sync,
    {
        struct X<T>(T)
        where
            T: 'static
                + std::fmt::Debug
                + std::hash::Hash
                + PartialEq
                + Eq
                + PartialOrd
                + Ord
                + Send
                + Sync;
        impl<T> PrivAsSpec for X<T>
        where
            T: 'static
                + std::fmt::Debug
                + std::hash::Hash
                + PartialEq
                + Eq
                + PartialOrd
                + Ord
                + Send
                + Sync,
        {
            fn as_any(&self) -> &(dyn std::any::Any + 'static + Send + Sync) {
                self
            }

            fn obj_debug(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                self.0.fmt(f)
            }

            fn obj_eq(&self, oth: &dyn PrivAsSpec) -> bool {
                if let Some(r) = oth.as_any().downcast_ref::<Self>() {
                    self.0.eq(&r.0)
                } else {
                    false
                }
            }

            fn obj_partial_cmp(
                &self,
                oth: &dyn PrivAsSpec,
            ) -> Option<std::cmp::Ordering> {
                Some(self.obj_cmp(oth))
            }

            fn obj_cmp(&self, oth: &dyn PrivAsSpec) -> std::cmp::Ordering {
                if let Some(r) = oth.as_any().downcast_ref::<Self>() {
                    self.0.cmp(&r.0)
                } else {
                    panic!("Attempted to Ord::cmp() a differing type");
                }
            }

            fn obj_hash(&self, state: &mut dyn std::hash::Hasher) {
                self.0.hash(&mut Box::new(state));
            }
        }
        Self(Arc::new(X(t)))
    }
}

type PrivRawSender = Arc<
    dyn Fn(InvUniq, InvAny) -> BoxFuture<'static, InvResult<()>>
        + 'static
        + Send
        + Sync,
>;

/// The raw, low-level sender side of a raw_channel.
#[derive(Clone)]
pub struct RawSender(Arc<RwLock<Option<PrivRawSender>>>);

impl RawSender {
    /// Send a message to the remote end of this channel.
    pub fn send(
        &self,
        id: InvUniq,
        data: InvAny,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        let outer = self.0.clone();
        let inner = outer.read().clone();
        if let Some(inner) = inner {
            let fut = inner(id, data);
            async move {
                match fut.await {
                    Ok(r) => Ok(r),
                    Err(e) => {
                        *outer.write() = None;
                        Err(e)
                    }
                }
            }
            .boxed()
        } else {
            async move { Err(std::io::ErrorKind::ConnectionReset.into()) }
                .boxed()
        }
    }

    /// Has this channel been closed?
    pub fn is_closed(&self) -> bool {
        self.0.read().is_none()
    }

    /// Close this channel from the sender side.
    pub fn close(&self) {
        *self.0.write() = None;
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
        let f: PrivRawSender = Arc::new(move |id, data| f(id, data).boxed());
        let _ = self.0.send(f);
    }
}

/// A static callback instance that can be used to close this channel.
/// (Generally, this is used on the receiver side, since it is straight-forward
/// to close the channel from the sender side).
pub type RawClose = Arc<dyn Fn() + 'static + Send + Sync>;

/// Create a raw, low-level channel.
pub fn raw_channel() -> (
    impl Future<Output = InvResult<RawSender>> + 'static + Send,
    RawReceiver,
    RawClose,
) {
    let inner = Arc::new(RwLock::new(None));
    let inner2 = inner.clone();
    let raw_close = Arc::new(move || {
        *inner2.write() = None;
    });
    let (s, r) = tokio::sync::oneshot::channel::<PrivRawSender>();
    let sender_fut = async move {
        let box_sender =
            tokio::time::timeout(std::time::Duration::from_secs(30), r)
                .await
                .map_err(|_| InvError::from(std::io::ErrorKind::TimedOut))?
                .map_err(|_| {
                    InvError::from(std::io::ErrorKind::ConnectionReset)
                })?;
        *inner.write() = Some(box_sender);
        Ok(RawSender(inner))
    };
    (sender_fut, RawReceiver(s), raw_close)
}

/// Send typed events, or make typed requests of the remote api.
pub struct TypedSender<EvtSend, ReqSend, ResSend>
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    for<'de> ResSend: serde::Deserialize<'de> + 'static + Send,
{
    raw_sender: RawSender,
    raw_recv_close: RawClose,
    pending: Arc<Mutex<HashMap<InvUniq, tokio::sync::oneshot::Sender<InvAny>>>>,
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
            raw_sender: self.raw_sender.clone(),
            raw_recv_close: self.raw_recv_close.clone(),
            pending: self.pending.clone(),
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
        self.raw_sender.send(InvUniq::new_evt(), InvAny::new(data))
    }

    /// Make a request of the remote side of this channel.
    pub fn request(
        &self,
        data: ReqSend,
    ) -> impl Future<Output = InvResult<ResSend>> + 'static + Send {
        // before we build up the pending instances, check if we're open.
        if self.raw_sender.is_closed() {
            return async move {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }.boxed();
        }

        // build up and insert pending info
        let req_id = InvUniq::new_req();
        let res_id = req_id.as_res();
        let (resp_send, resp_recv) = tokio::sync::oneshot::channel();
        let resp_recv =
            tokio::time::timeout(std::time::Duration::from_secs(30), resp_recv);
        self.pending.lock().insert(res_id.clone(), resp_send);

        // setup a cleanup raii guard
        struct Cleanup {
            res_id: InvUniq,
            pending: Arc<
                Mutex<HashMap<InvUniq, tokio::sync::oneshot::Sender<InvAny>>>,
            >,
        }

        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = self.pending.lock().remove(&self.res_id);
            }
        }

        let cleanup = Cleanup {
            res_id,
            pending: self.pending.clone(),
        };

        // send the request and await the response
        let fut = self.raw_sender.send(req_id, InvAny::new(data));
        async move {
            let _cleanup = cleanup;

            fut.await?;

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
        .boxed()
    }

    /// Close this bi-directional channel.
    pub fn close(&self) {
        self.raw_sender.close();
        (self.raw_recv_close)();
        self.pending.lock().clear();
    }
}

/// Typedef for a TypedSender where all the types are the same.
pub type UnitypedSender<T> = TypedSender<T, T, T>;

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
pub struct TypedReceiver<
    EvtRecv: 'static + Send,
    ReqRecv: 'static + Send,
    ResRecv: 'static + Send,
> {
    fn_send: tokio::sync::oneshot::Sender<
        PrivTypedSender<EvtRecv, ReqRecv, ResRecv>,
    >,
    _phantom: std::marker::PhantomData<&'static (EvtRecv, ReqRecv, ResRecv)>,
}

impl<
        EvtRecv: 'static + Send,
        ReqRecv: 'static + Send,
        ResRecv: 'static + Send,
    > TypedReceiver<EvtRecv, ReqRecv, ResRecv>
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
        let f: PrivTypedSender<EvtRecv, ReqRecv, ResRecv> =
            Arc::new(move |res| f(res).boxed());
        let _ = self.fn_send.send(f);
    }
}

/// Typedef for a TypedSender where all the types are the same.
pub type UnitypedReceiver<T> = TypedReceiver<T, T, T>;

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
    // TODO - make a concrete PendingSender future type
    //        so we can eliminate this impl in argument position
    //        which prevents us from turbo-fishing the types...
    raw_sender: impl Future<Output = InvResult<RawSender>> + 'static + Send,
    raw_receiver: RawReceiver,
    raw_recv_close: RawClose,
) -> (
    impl Future<Output = InvResult<TypedSender<EvtSend, ReqSend, ResSend>>>
        + 'static
        + Send,
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
    let raw_sender = raw_sender.shared();
    let (fn_send, fn_recv) = tokio::sync::oneshot::channel::<
        PrivTypedSender<EvtRecv, ReqRecv, ResRecv>,
    >();
    let high_level_fn = async move {
        fn_recv
            .await
            .map_err(|_| InvError::from(std::io::ErrorKind::ConnectionReset))
    }
    .shared();
    let pending: Arc<
        Mutex<HashMap<InvUniq, tokio::sync::oneshot::Sender<InvAny>>>,
    > = Arc::new(Mutex::new(HashMap::new()));

    {
        // FIRST call raw_receiver.handle()
        // otherwise we might get into a deadlock waiting for the remote

        let raw_sender = raw_sender.clone();
        let high_level_fn = high_level_fn.clone();
        let pending = pending.clone();
        raw_receiver.handle(move |id, data| {
            let raw_sender = raw_sender.clone();
            let high_level_fn = high_level_fn.clone();
            let pending = pending.clone();
            async move {
                let raw_sender = raw_sender.await?;
                let f = high_level_fn.await?;

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
                    if let Some(respond) = pending.lock().remove(&id) {
                        let _ = respond.send(data);
                    }
                    Ok(())
                }
            }
        });
    }

    let send_fut = async move {
        let raw_sender = raw_sender.await?;

        let typed_sender = TypedSender {
            raw_sender,
            raw_recv_close,
            pending,
            _phantom: std::marker::PhantomData,
        };

        Ok(typed_sender)
    };

    let typed_receiver = TypedReceiver {
        fn_send,
        _phantom: std::marker::PhantomData,
    };

    (send_fut, typed_receiver)
}

/// Delegates to `upgrade_raw_channel` but with the same type for all types.
pub fn unitype_upgrade_raw_channel<T>(
    // TODO - make a concrete PendingSender future type
    //        so we can eliminate this impl in argument position
    //        which prevents us from turbo-fishing the types...
    raw_sender: impl Future<Output = InvResult<RawSender>> + 'static + Send,
    raw_receiver: RawReceiver,
    raw_recv_close: RawClose,
) -> (
    impl Future<Output = InvResult<UnitypedSender<T>>> + 'static + Send,
    UnitypedReceiver<T>,
)
where
    for<'de> T: serde::Serialize + serde::Deserialize<'de> + 'static + Send,
{
    upgrade_raw_channel(raw_sender, raw_receiver, raw_recv_close)
}

// --- old version --- //

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

use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inv_id::InvId;
    use std::sync::atomic;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_chan() {
        let (snd1, recv2, c2) = raw_channel();
        let (snd2, recv1, c1) = raw_channel();

        let (snd1, recv1): (_, UnitypedReceiver<u32>) =
            unitype_upgrade_raw_channel(snd1, recv1, c1);

        recv1.handle(move |incoming| async move {
            match incoming {
                TypedIncoming::Event(evt) => {
                    println!("got: {:?}", evt);
                    assert_eq!(69, evt);
                }
                TypedIncoming::Request(data, respond) => {
                    respond.respond(data + 1).await;
                }
            }
            Ok(())
        });

        let (snd2, recv2): (_, UnitypedReceiver<u32>) =
            unitype_upgrade_raw_channel(snd2, recv2, c2);

        recv2.handle(move |incoming| async move {
            match incoming {
                TypedIncoming::Event(evt) => {
                    println!("got: {:?}", evt);
                    assert_eq!(42, evt);
                }
                TypedIncoming::Request(data, respond) => {
                    respond.respond(data - 1).await;
                }
            }
            Ok(())
        });

        let snd1 = snd1.await.unwrap();
        let snd2 = snd2.await.unwrap();

        snd1.emit(42).await.unwrap();
        snd2.emit(69).await.unwrap();

        let res = snd1.request(42).await.unwrap();
        println!("got: {:?}", res);
        assert_eq!(41, res);

        let res = snd2.request(42).await.unwrap();
        println!("got: {:?}", res);
        assert_eq!(43, res);
    }

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
}
