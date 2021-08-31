//! inversion broker traits and impl

use crate::inv_any::InvAny;
use crate::inv_api_spec::*;
use crate::inv_error::*;
use crate::inv_share::InvShare;
use crate::inv_uniq::InvUniq;
use futures::future::{BoxFuture, FutureExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// Helper traits. You should only need these if you're implementing
/// low-level inversion engine coding.
pub mod traits {
    use super::*;

    /// Inversion Api Sender trait.
    pub trait AsInvRawSender: 'static + Send + Sync {
        /// Send data to the remote end of this "channel".
        fn send(
            &self,
            msg_id: InvUniq,
            data: InvAny,
        ) -> BoxFuture<'static, InvResult<()>>;

        /// Has this channel been closed?
        fn is_closed(&self) -> bool;

        /// Close this channel from the sender side.
        fn close(&self);
    }

    /// Typedef for a Dyn Inversion Api Handler Callback.
    pub type InvRawHandlerCbDyn = Arc<
        dyn Fn(InvUniq, InvAny) -> BoxFuture<'static, InvResult<()>>
            + 'static
            + Send
            + Sync,
    >;

    /// Inversion Api Handler trait.
    pub trait AsInvRawHandler: 'static + Send {
        /// Supply the logic that will be invoked on message receipt.
        fn handle(self: Box<Self>, cb: InvRawHandlerCbDyn);
    }

    /// Inversion Api Close trait.
    pub trait AsInvRawClose: 'static + Send + Sync {
        /// Has this channel been closed?
        fn is_closed(&self) -> bool;

        /// Close this channel from the sender side.
        fn close(&self);
    }

    /// Inversion Api Sender trait.
    pub trait AsInvTypedSender: 'static + Send + Sync {
        /// Type to be emitted via the "emit" method.
        type EvtSend: serde::Serialize + 'static + Send;

        /// Type to be sent with the "request" method.
        type ReqSend: serde::Serialize + 'static + Send;

        /// Type to be received with the "request" method.
        type ResSend: for<'de> serde::Deserialize<'de> + 'static + Send;

        /// Emit data as an event to the remote side of this channel.
        fn emit(
            &self,
            data: Self::EvtSend,
        ) -> BoxFuture<'static, InvResult<()>>;

        /// Make a request of the remote side of this channel, expecting a response.
        fn request(
            &self,
            data: Self::ReqSend,
        ) -> BoxFuture<'static, InvResult<Self::ResSend>>;

        /// Has this channel been closed?
        fn is_closed(&self) -> bool;

        /// Close this channel from the sender side.
        fn close(&self);
    }

    /// Typedef for a Dyn Inversion Api Handler Callback.
    pub type InvTypedHandlerCbDyn<EvtRecv, ReqRecv, ResRecv> = Arc<
        dyn Fn(
                InvTypedIncoming<EvtRecv, ReqRecv, ResRecv>,
            ) -> BoxFuture<'static, InvResult<()>>
            + 'static
            + Send
            + Sync,
    >;

    /// Inversion Api Handler trait.
    pub trait AsInvTypedHandler: 'static + Send {
        /// Event type received from the sender side.
        type EvtRecv: for<'de> serde::Deserialize<'de> + 'static + Send;

        /// Request type received from the sender side.
        type ReqRecv: for<'de> serde::Deserialize<'de> + 'static + Send;

        /// Response type returned to the sender side.
        type ResRecv: serde::Serialize + 'static + Send;

        /// Supply the logic that will be invoked on message receipt.
        fn handle(
            self: Box<Self>,
            cb: InvTypedHandlerCbDyn<
                Self::EvtRecv,
                Self::ReqRecv,
                Self::ResRecv,
            >,
        );
    }

    /// Typedef for a Dyn Inversion Api Factory Handler Callback.
    pub type InvFactoryHandlerCbDyn = Arc<
        dyn Fn(InvRawSender, InvRawHandler, InvRawClose) -> InvResult<()>
            + 'static
            + Send
            + Sync,
    >;

    /// Inversion Api FactoryHandler trait.
    pub trait AsInvFactoryHandler: 'static + Send {
        /// Supply the logic that will be invoked on incoming bind request.
        fn handle(self: Box<Self>, cb: InvFactoryHandlerCbDyn);
    }

    /// inversion broker trait
    pub trait AsInvBroker: 'static + Send + Sync {
        /// Get the inv api impl spec that defines this brokers implemented api.
        fn get_inv_api_impl_spec(&self) -> ImplSpec;

        /// Obtain a handle to this broker's raw inv api implementation.
        fn bind_to_inv_api_impl_raw(&self) -> BoxFuture<'static, InvResult<(InvRawSender, InvRawHandler, InvRawClose)>>;

        /// Register a new api impl to this broker
        #[deprecated]
        fn register_impl_raw(
            &self,
            impl_spec: ImplSpec,
        ) -> BoxFuture<'static, InvResult<InvFactoryHandler>>;

        /// Bind to a registered api implementation
        /// TODO - for now we only support this binding to an exact impl,
        ///        someday we can add ApiSpec / feature matching.
        #[deprecated]
        fn bind_to_impl_raw(
            &self,
            impl_spec: ImplSpec,
        ) -> BoxFuture<
            'static,
            InvResult<(InvRawSender, InvRawHandler, InvRawClose)>,
        >;
    }
}
use traits::*;

/// Concrete wrapper for a Dyn Inversion Api Sender.
#[derive(Clone)]
pub struct InvRawSender(pub Arc<dyn AsInvRawSender>);

impl InvRawSender {
    /// Send data to the remote end of this "channel".
    pub fn send(
        &self,
        msg_id: InvUniq,
        data: InvAny,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        AsInvRawSender::send(&*self.0, msg_id, data)
    }

    /// Has this channel been closed?
    pub fn is_closed(&self) -> bool {
        AsInvRawSender::is_closed(&*self.0)
    }

    /// Close this channel from the sender side.
    pub fn close(&self) {
        AsInvRawSender::close(&*self.0)
    }
}

/// Concrete wrapper for a Dyn Inversion Api Handler.
pub struct InvRawHandler(pub Box<dyn AsInvRawHandler>);

impl InvRawHandler {
    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure should be Sync, and return a 'static Future.
    pub fn handle<Fut, Cb>(self, cb: Cb)
    where
        Fut: 'static + Send + Future<Output = InvResult<()>>,
        Cb: 'static + Send + Sync + Fn(InvUniq, InvAny) -> Fut,
    {
        let cb = Arc::new(move |msg_id, data| cb(msg_id, data).boxed());
        AsInvRawHandler::handle(self.0, cb);
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure should be Sync, and return a direct result.
    pub fn handle_sync<Cb>(self, cb: Cb)
    where
        Cb: 'static + Send + Sync + Fn(InvUniq, InvAny) -> InvResult<()>,
    {
        let cb = Arc::new(move |msg_id, data| {
            let res = cb(msg_id, data);
            async move { res }.boxed()
        });
        AsInvRawHandler::handle(self.0, cb);
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure can be FnMut, and return a 'static Future.
    pub fn handle_mut<Fut, Cb>(self, cb: Cb)
    where
        Fut: 'static + Send + Future<Output = InvResult<()>>,
        Cb: 'static + Send + FnMut(InvUniq, InvAny) -> Fut,
    {
        let m = Arc::new(Mutex::new(cb));
        let cb = Arc::new(move |msg_id, data| (m.lock())(msg_id, data).boxed());
        AsInvRawHandler::handle(self.0, cb)
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure can be FnMut, and return a direct result.
    pub fn handle_mut_sync<Cb>(self, cb: Cb)
    where
        Cb: 'static + Send + FnMut(InvUniq, InvAny) -> InvResult<()>,
    {
        let m = Arc::new(Mutex::new(cb));
        let cb = Arc::new(move |msg_id, data| {
            let res = (m.lock())(msg_id, data);
            async move { res }.boxed()
        });
        AsInvRawHandler::handle(self.0, cb);
    }
}

/// Concrete wrapper for a Dyn Inversion Api Handler.
#[derive(Clone)]
pub struct InvRawClose(pub Arc<dyn AsInvRawClose>);

impl InvRawClose {
    /// Has this channel been closed?
    pub fn is_closed(&self) -> bool {
        AsInvRawClose::is_closed(&*self.0)
    }

    /// Close this channel from the sender side.
    pub fn close(&self) {
        AsInvRawClose::close(&*self.0)
    }
}

/// Create a raw, low-level channel.
pub fn inv_raw_channel() -> (InvRawSender, InvRawHandler, InvRawClose) {
    let notify_kill = Arc::new(tokio::sync::Notify::new());
    let (s, r) = tokio::sync::oneshot::channel();

    let notify_kill2 = notify_kill.clone();
    let recv_fn = async move {
        let r = tokio::time::timeout(std::time::Duration::from_secs(30), r);
        futures::select_biased! {
            res = r.fuse() => {
                res
                    .map_err(|_| InvError::from(std::io::ErrorKind::TimedOut))?
                    .map_err(|_| InvError::from(std::io::ErrorKind::ConnectionReset))
            }
            _ = notify_kill2.notified().fuse() => {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
        }
    }.boxed().shared();

    struct I {
        notify_kill: Arc<tokio::sync::Notify>,
        #[allow(clippy::type_complexity)]
        recv_fn: futures::future::Shared<
            BoxFuture<'static, InvResult<InvRawHandlerCbDyn>>,
        >,
    }

    impl Drop for I {
        fn drop(&mut self) {
            self.notify_kill.notify_waiters();
        }
    }

    let inner = InvShare::new_rw_lock(I {
        notify_kill,
        recv_fn,
    });

    struct S(InvShare<I>);

    impl AsInvRawSender for S {
        fn send(
            &self,
            msg_id: InvUniq,
            data: InvAny,
        ) -> BoxFuture<'static, InvResult<()>> {
            let inner = self.0.clone();
            let fut = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                async move {
                    let (notify_kill, recv_fn) = inner.share_ref(|i| {
                        Ok((i.notify_kill.clone(), i.recv_fn.clone()))
                    })?;

                    let sender = recv_fn.await?;

                    futures::select_biased! {
                        res = sender(msg_id, data).fuse() => {
                            res
                        }
                        _ = notify_kill.notified().fuse() => {
                            Err(std::io::ErrorKind::ConnectionReset.into())
                        }
                    }
                },
            );
            async move {
                fut.await
                    .map_err(|_| InvError::from(std::io::ErrorKind::TimedOut))?
            }
            .boxed()
        }

        fn is_closed(&self) -> bool {
            self.0.is_closed()
        }

        fn close(&self) {
            self.0.close();
        }
    }

    impl AsInvRawClose for S {
        fn is_closed(&self) -> bool {
            AsInvRawSender::is_closed(self)
        }

        fn close(&self) {
            AsInvRawSender::close(self)
        }
    }

    struct H(tokio::sync::oneshot::Sender<InvRawHandlerCbDyn>);

    impl AsInvRawHandler for H {
        fn handle(self: Box<Self>, cb: InvRawHandlerCbDyn) {
            let _ = self.0.send(cb);
        }
    }

    let sender = Arc::new(S(inner));
    let handler = Box::new(H(s));

    (
        InvRawSender(sender.clone()),
        InvRawHandler(handler),
        InvRawClose(sender),
    )
}

type TypedRespondCb<ResRecv> =
    Box<dyn FnOnce(ResRecv) -> BoxFuture<'static, ()> + 'static + Send>;

/// Respond to an incoming typed request from the remote api.
/// Drop this instance to generate a ConnectionReset error on the remote.
pub struct InvTypedRespond<ResRecv: 'static + Send>(TypedRespondCb<ResRecv>);

impl<ResRecv: 'static + Send> InvTypedRespond<ResRecv> {
    /// Submit the response to the remote end.
    pub fn respond(
        self,
        data: ResRecv,
    ) -> impl Future<Output = ()> + 'static + Send {
        (self.0)(data)
    }
}

/// Incoming events and requests from the remote api.
pub enum InvTypedIncoming<
    EvtRecv: 'static + Send,
    ReqRecv: 'static + Send,
    ResRecv: 'static + Send,
> {
    /// An incoming message of type event that does not need direct response.
    Event(EvtRecv),

    /// An incoming message of type request that requires a direct response.
    Request(ReqRecv, InvTypedRespond<ResRecv>),
}

/// Concrete wrapper for a Dyn Inversion Api Typed Sender.
#[derive(Clone)]
pub struct InvTypedSender<EvtSend, ReqSend, ResSend>(
    pub  Arc<
        dyn AsInvTypedSender<
            EvtSend = EvtSend,
            ReqSend = ReqSend,
            ResSend = ResSend,
        >,
    >,
)
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    ResSend: for<'de> serde::Deserialize<'de> + 'static + Send;

impl<EvtSend, ReqSend, ResSend> InvTypedSender<EvtSend, ReqSend, ResSend>
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    ResSend: for<'de> serde::Deserialize<'de> + 'static + Send,
{
    /// Emit data as an event to the remote side of this channel.
    pub fn emit(
        &self,
        data: EvtSend,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        AsInvTypedSender::emit(&*self.0, data)
    }

    /// Make a request of the remote side of this channel, expecting a response.
    pub fn request(
        &self,
        data: ReqSend,
    ) -> impl Future<Output = InvResult<ResSend>> {
        AsInvTypedSender::request(&*self.0, data)
    }

    /// Has this channel been closed?
    pub fn is_closed(&self) -> bool {
        AsInvTypedSender::is_closed(&*self.0)
    }

    /// Close this channel from the sender side.
    pub fn close(&self) {
        AsInvTypedSender::close(&*self.0)
    }
}

/// Concrete wrapper for a Dyn Typed Inversion Api Handler.
pub struct InvTypedHandler<EvtRecv, ReqRecv, ResRecv>(
    pub  Box<
        dyn AsInvTypedHandler<
            EvtRecv = EvtRecv,
            ReqRecv = ReqRecv,
            ResRecv = ResRecv,
        >,
    >,
)
where
    EvtRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
    ReqRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
    ResRecv: serde::Serialize + 'static + Send;

impl<EvtRecv, ReqRecv, ResRecv> InvTypedHandler<EvtRecv, ReqRecv, ResRecv>
where
    EvtRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
    ReqRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
    ResRecv: serde::Serialize + 'static + Send,
{
    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure should be Sync, and return a 'static Future.
    pub fn handle<Fut, Cb>(self, cb: Cb)
    where
        Fut: 'static + Send + Future<Output = InvResult<()>>,
        Cb: 'static
            + Send
            + Sync
            + Fn(InvTypedIncoming<EvtRecv, ReqRecv, ResRecv>) -> Fut,
    {
        let cb = Arc::new(move |incoming| cb(incoming).boxed());
        AsInvTypedHandler::handle(self.0, cb);
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure should be Sync, and return a direct result.
    pub fn handle_sync<Cb>(self, cb: Cb)
    where
        Cb: 'static
            + Send
            + Sync
            + Fn(InvTypedIncoming<EvtRecv, ReqRecv, ResRecv>) -> InvResult<()>,
    {
        let cb = Arc::new(move |incoming| {
            let res = cb(incoming);
            async move { res }.boxed()
        });
        AsInvTypedHandler::handle(self.0, cb);
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure can be FnMut, and return a 'static Future.
    pub fn handle_mut<Fut, Cb>(self, cb: Cb)
    where
        Fut: 'static + Send + Future<Output = InvResult<()>>,
        Cb: 'static
            + Send
            + FnMut(InvTypedIncoming<EvtRecv, ReqRecv, ResRecv>) -> Fut,
    {
        let m = Arc::new(Mutex::new(cb));
        let cb = Arc::new(move |incoming| (m.lock())(incoming).boxed());
        AsInvTypedHandler::handle(self.0, cb)
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure can be FnMut, and return a direct result.
    pub fn handle_mut_sync<Cb>(self, cb: Cb)
    where
        Cb: 'static
            + Send
            + FnMut(InvTypedIncoming<EvtRecv, ReqRecv, ResRecv>) -> InvResult<()>,
    {
        let m = Arc::new(Mutex::new(cb));
        let cb = Arc::new(move |incoming| {
            let res = (m.lock())(incoming);
            async move { res }.boxed()
        });
        AsInvTypedHandler::handle(self.0, cb);
    }
}

/// Given bi-directional raw handles to a remote (one sender, one receiver),
/// set up a typed, request/response enabled high-level channel.
pub fn upgrade_inv_raw_channel<
    EvtSend,
    ReqSend,
    ResSend,
    EvtRecv,
    ReqRecv,
    ResRecv,
>(
    raw_sender: InvRawSender,
    raw_handler: InvRawHandler,
    raw_recv_close: InvRawClose,
) -> (
    InvTypedSender<EvtSend, ReqSend, ResSend>,
    InvTypedHandler<EvtRecv, ReqRecv, ResRecv>,
)
where
    EvtSend: serde::Serialize + 'static + Send,
    ReqSend: serde::Serialize + 'static + Send,
    ResSend: for<'de> serde::Deserialize<'de> + 'static + Send,
    EvtRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
    ReqRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
    ResRecv: serde::Serialize + 'static + Send,
{
    struct I {
        raw_sender: InvRawSender,
        raw_recv_close: InvRawClose,
        pending: HashMap<InvUniq, tokio::sync::oneshot::Sender<InvAny>>,
    }

    impl Drop for I {
        fn drop(&mut self) {
            self.raw_sender.close();
            self.raw_recv_close.close();
            self.pending.clear();
        }
    }

    let inner = InvShare::new_rw_lock(I {
        raw_sender,
        raw_recv_close,
        pending: HashMap::new(),
    });

    struct S<EvtSend, ReqSend, ResSend>
    where
        EvtSend: serde::Serialize + 'static + Send,
        ReqSend: serde::Serialize + 'static + Send,
        ResSend: for<'de> serde::Deserialize<'de> + 'static + Send,
    {
        inner: InvShare<I>,
        #[allow(clippy::type_complexity)]
        _phantom: std::marker::PhantomData<fn() -> (EvtSend, ReqSend, ResSend)>,
    }

    impl<EvtSend, ReqSend, ResSend> Drop for S<EvtSend, ReqSend, ResSend>
    where
        EvtSend: serde::Serialize + 'static + Send,
        ReqSend: serde::Serialize + 'static + Send,
        ResSend: for<'de> serde::Deserialize<'de> + 'static + Send,
    {
        fn drop(&mut self) {
            self.inner.close();
        }
    }

    impl<EvtSend, ReqSend, ResSend> AsInvTypedSender
        for S<EvtSend, ReqSend, ResSend>
    where
        EvtSend: serde::Serialize + 'static + Send,
        ReqSend: serde::Serialize + 'static + Send,
        ResSend: for<'de> serde::Deserialize<'de> + 'static + Send,
    {
        type EvtSend = EvtSend;
        type ReqSend = ReqSend;
        type ResSend = ResSend;

        fn emit(
            &self,
            data: Self::EvtSend,
        ) -> BoxFuture<'static, InvResult<()>> {
            let raw_sender = self.inner.share_ref(|i| Ok(i.raw_sender.clone()));
            async move {
                let raw_sender = raw_sender?;
                raw_sender.send(InvUniq::new_evt(), InvAny::new(data)).await
            }
            .boxed()
        }

        fn request(
            &self,
            data: Self::ReqSend,
        ) -> BoxFuture<'static, InvResult<Self::ResSend>> {
            // early check
            if self.inner.is_closed() {
                return async move {
                    Err(std::io::ErrorKind::ConnectionReset.into())
                }
                .boxed();
            }

            let req_id = InvUniq::new_req();
            let res_id = req_id.as_res();
            let (res_send, res_recv) = tokio::sync::oneshot::channel();

            // setup raii guard to remove the pending item
            struct Cleanup(InvUniq, InvShare<I>);
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    let res_id = self.0.clone();
                    let _ = self.1.share_mut(move |i, _| {
                        i.pending.remove(&res_id);
                        Ok(())
                    });
                }
            }
            let cleanup = Cleanup(res_id.clone(), self.inner.clone());

            // insert pending && fetch sender
            let raw_sender = self.inner.share_mut(move |i, _| {
                i.pending.insert(res_id, res_send);
                Ok(i.raw_sender.clone())
            });

            async move {
                let _cleanup = cleanup;

                let raw_sender = raw_sender?;

                // send the request
                raw_sender.send(req_id, InvAny::new(data)).await?;

                // await the response
                let res_recv = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    res_recv,
                );
                let res = res_recv
                    .await
                    .map_err(|_| InvError::from(std::io::ErrorKind::TimedOut))?
                    .map_err(|_| {
                        InvError::from(std::io::ErrorKind::ConnectionReset)
                    })?;
                if let Ok(res) = res.downcast::<Self::ResSend>() {
                    Ok(res)
                } else {
                    Err(std::io::ErrorKind::InvalidData.into())
                }
            }
            .boxed()
        }

        fn is_closed(&self) -> bool {
            self.inner.is_closed()
        }

        fn close(&self) {
            self.inner.close();
        }
    }

    struct H<EvtRecv, ReqRecv, ResRecv>
    where
        EvtRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
        ReqRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
        ResRecv: serde::Serialize + 'static + Send,
    {
        inner: InvShare<I>,
        raw_handler: InvRawHandler,
        #[allow(clippy::type_complexity)]
        _phantom: std::marker::PhantomData<fn() -> (EvtRecv, ReqRecv, ResRecv)>,
    }

    impl<EvtRecv, ReqRecv, ResRecv> AsInvTypedHandler
        for H<EvtRecv, ReqRecv, ResRecv>
    where
        EvtRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
        ReqRecv: for<'de> serde::Deserialize<'de> + 'static + Send,
        ResRecv: serde::Serialize + 'static + Send,
    {
        type EvtRecv = EvtRecv;
        type ReqRecv = ReqRecv;
        type ResRecv = ResRecv;

        fn handle(
            self: Box<Self>,
            cb: InvTypedHandlerCbDyn<
                Self::EvtRecv,
                Self::ReqRecv,
                Self::ResRecv,
            >,
        ) {
            let Self {
                inner, raw_handler, ..
            } = *self;

            // raii guard to close if this receiver logic is dropped
            struct Cleanup(InvShare<I>);
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    self.0.close();
                }
            }
            let cleanup = Cleanup(inner.clone());

            raw_handler.handle(move |id, data| {
                let _cleanup = &cleanup;
                let inner = inner.clone();
                let cb = cb.clone();
                async move {
                    if id.is_evt() {
                        if let Ok(data) = data.downcast::<EvtRecv>() {
                            cb(InvTypedIncoming::Event(data)).await
                        } else {
                            Err(std::io::ErrorKind::InvalidData.into())
                        }
                    } else if id.is_req() {
                        if let Ok(data) = data.downcast::<ReqRecv>() {
                            let respond: TypedRespondCb<ResRecv> =
                                Box::new(move |res| {
                                    if let Ok(raw_sender) = inner
                                        .share_ref(|i| Ok(i.raw_sender.clone()))
                                    {
                                        async move {
                                            let _ = raw_sender
                                                .send(
                                                    id.as_res(),
                                                    InvAny::new(res),
                                                )
                                                .await;
                                        }
                                        .boxed()
                                    } else {
                                        async move {}.boxed()
                                    }
                                });
                            let respond = InvTypedRespond(respond);
                            cb(InvTypedIncoming::Request(data, respond)).await
                        } else {
                            Err(std::io::ErrorKind::InvalidData.into())
                        }
                    } else {
                        // must be a response
                        if let Ok(Some(respond)) =
                            inner.share_mut(|i, _| Ok(i.pending.remove(&id)))
                        {
                            let _ = respond.send(data);
                        }
                        Ok(())
                    }
                }
            });
        }
    }

    let sender = InvTypedSender(Arc::new(S {
        inner: inner.clone(),
        _phantom: std::marker::PhantomData,
    }));

    let handler = InvTypedHandler(Box::new(H {
        inner,
        raw_handler,
        _phantom: std::marker::PhantomData,
    }));

    (sender, handler)
}

/// Typedef for a TypedSender where all the types are the same.
pub type InvUnitypedSender<T> = InvTypedSender<T, T, T>;

/// Typedef for a TypedSender where all the types are the same.
pub type InvUnitypedHandler<T> = InvTypedHandler<T, T, T>;

/// Delegates to `upgrade_inv_raw_channel` but with the same type for all types.
pub fn unitype_upgrade_inv_raw_channel<T>(
    raw_sender: InvRawSender,
    raw_handler: InvRawHandler,
    raw_recv_close: InvRawClose,
) -> (InvUnitypedSender<T>, InvUnitypedHandler<T>)
where
    T: for<'de> serde::Deserialize<'de> + serde::Serialize + 'static + Send,
{
    upgrade_inv_raw_channel(raw_sender, raw_handler, raw_recv_close)
}

/// Concrete wrapper for a Dyn Inversion Api Factory Handler.
pub struct InvFactoryHandler(pub Box<dyn AsInvFactoryHandler>);

impl InvFactoryHandler {
    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure should be Sync, and return a direct result.
    pub fn handle_sync<Cb>(self, cb: Cb)
    where
        Cb: 'static
            + Send
            + Sync
            + Fn(InvRawSender, InvRawHandler, InvRawClose) -> InvResult<()>,
    {
        let cb = Arc::new(cb);
        AsInvFactoryHandler::handle(self.0, cb);
    }

    /// Specify logic to be invoked by this handler on message receipt.
    /// In this form the closure can be FnMut, and return a direct result.
    pub fn handle_mut_sync<Cb>(self, cb: Cb)
    where
        Cb: 'static
            + Send
            + FnMut(InvRawSender, InvRawHandler, InvRawClose) -> InvResult<()>,
    {
        let m = Arc::new(Mutex::new(cb));
        let cb = Arc::new(move |sender, handler, close| {
            (m.lock())(sender, handler, close)
        });
        AsInvFactoryHandler::handle(self.0, cb);
    }
}

/// inversion broker type handle
pub struct InvBroker(pub Arc<dyn AsInvBroker>);

impl std::ops::Deref for InvBroker {
    type Target = dyn AsInvBroker;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl InvBroker {
    /// Get the inv api impl spec that defines this brokers implemented api.
    pub fn get_inv_api_impl_spec(&self) -> ImplSpec {
        self.0.get_inv_api_impl_spec()
    }

    /// Obtain a handle to this broker's raw inv api implementation.
    pub fn bind_to_inv_api_impl_raw(&self) -> BoxFuture<'static, InvResult<(InvRawSender, InvRawHandler, InvRawClose)>> {
        self.0.bind_to_inv_api_impl_raw()
    }

    /// Register a new api impl to this broker
    pub fn register_impl_raw(
        &self,
        impl_spec: ImplSpec,
    ) -> impl Future<Output = InvResult<InvFactoryHandler>> {
        #[allow(deprecated)]
        self.0.register_impl_raw(impl_spec)
    }

    /// Bind to a registered api implementation
    /// TODO - for now we only support this binding to an exact impl,
    ///        someday we can add ApiSpec / feature matching.
    pub fn bind_to_impl_raw(
        &self,
        impl_spec: ImplSpec,
    ) -> impl Future<Output = InvResult<(InvRawSender, InvRawHandler, InvRawClose)>>
    {
        #[allow(deprecated)]
        self.0.bind_to_impl_raw(impl_spec)
    }
}

/// construct a new inversion api broker
pub fn new_inv_broker() -> InvBroker {
    InvBroker(Arc::new(PrivBroker::new()))
}

// -- private -- //

#[derive(Clone)]
enum PrivPendingFactorySender {
    Pending(Arc<tokio::sync::Notify>),
    Ready(InvFactoryHandlerCbDyn),
}

struct PrivBrokerInner {
    map: HashMap<ImplSpec, PrivPendingFactorySender>,
}

struct PrivBroker(InvShare<PrivBrokerInner>);

impl PrivBroker {
    pub fn new() -> Self {
        Self(InvShare::new_rw_lock(PrivBrokerInner {
            map: HashMap::new(),
        }))
    }
}

impl AsInvBroker for PrivBroker {
    fn get_inv_api_impl_spec(&self) -> ImplSpec {
        crate::inv_api_impl::INV_API_IMPL.clone()
    }

    fn bind_to_inv_api_impl_raw(&self) -> BoxFuture<'static, InvResult<(InvRawSender, InvRawHandler, InvRawClose)>> {
        async move {
            unimplemented!()
        }.boxed()
    }

    fn register_impl_raw(
        &self,
        impl_spec: ImplSpec,
    ) -> BoxFuture<'static, InvResult<InvFactoryHandler>> {
        let inner = self.0.clone();
        async move {
            let notify = Arc::new(tokio::sync::Notify::new());

            let impl_spec2 = impl_spec.clone();
            inner.share_mut(move |i, _| {
                if i.map.contains_key(&impl_spec2) {
                    return Err("impl already registered".into());
                }
                i.map.insert(
                    impl_spec2,
                    PrivPendingFactorySender::Pending(notify),
                );
                Ok(())
            })?;

            struct H(ImplSpec, InvShare<PrivBrokerInner>);

            impl AsInvFactoryHandler for H {
                fn handle(self: Box<Self>, cb: InvFactoryHandlerCbDyn) {
                    let Self(impl_spec, inner) = *self;
                    let n = inner
                        .share_mut(move |i, _| match i.map.entry(impl_spec) {
                            std::collections::hash_map::Entry::Occupied(
                                mut e,
                            ) => {
                                match e
                                    .insert(PrivPendingFactorySender::Ready(cb))
                                {
                                    PrivPendingFactorySender::Pending(n) => {
                                        Ok(n)
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            _ => unreachable!(),
                        })
                        .expect("should be impossible to set factory sender");
                    n.notify_waiters();
                }
            }

            let h = InvFactoryHandler(Box::new(H(impl_spec, inner)));

            Ok(h)
        }
        .boxed()
    }

    fn bind_to_impl_raw(
        &self,
        impl_spec: ImplSpec,
    ) -> BoxFuture<'static, InvResult<(InvRawSender, InvRawHandler, InvRawClose)>>
    {
        let inner = self.0.clone();
        async move {
            let sender = match inner
                .share_ref(|i| Ok(i.map.get(&impl_spec).cloned()))?
            {
                None => return Err("no such impl".into()),
                Some(sender) => sender,
            };

            let sender = match sender {
                PrivPendingFactorySender::Pending(notify) => {
                    notify.notified().await;
                    match inner
                        .share_ref(|i| Ok(i.map.get(&impl_spec).cloned()))?
                    {
                        Some(PrivPendingFactorySender::Ready(sender)) => sender,
                        _ => return Err("no such impl".into()),
                    }
                }
                PrivPendingFactorySender::Ready(sender) => sender,
            };

            let (raw_send1, raw_recv2, raw_close2) = inv_raw_channel();
            let (raw_send2, raw_recv1, raw_close1) = inv_raw_channel();

            sender(raw_send1, raw_recv1, raw_close1)?;

            Ok((raw_send2, raw_recv2, raw_close2))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_specs() {
        let spec = Arc::new(ImplSpecInner {
            api_spec: Arc::new(ApiSpecInner {
                api_features: Box::new([
                    FeatureDef {
                        feature_name: "readRaw".into(),
                        feature_status: FeatureSpecStatus::Deprecated,
                    },
                    FeatureDef {
                        feature_name: "readBytes".into(),
                        feature_status: FeatureSpecStatus::Stable,
                    },
                    FeatureDef {
                        feature_name: "writeBytes".into(),
                        feature_status: FeatureSpecStatus::Unstable,
                    },
                ]),
                ..Default::default()
            }),
            impl_features: Box::new(["readBytes".into()]),
            ..Default::default()
        });
        println!("{}: {:#?}", spec, spec);
        println!("{}", serde_json::to_string(&spec).unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_raw_channel() {
        // create the unidirectional channel
        let (send, recv, close) = inv_raw_channel();

        // add the handle logic
        recv.handle(move |id, data| {
            let close = close.clone();
            async move {
                let data = data.downcast::<isize>().unwrap();
                println!("{:?} {:?}", id, data);
                assert_eq!("evt", &format!("{}", id));
                assert_eq!(42, data);
                // close the channel after a single event
                close.close();
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
        let (snd1, recv2, c2) = inv_raw_channel();
        let (snd2, recv1, c1) = inv_raw_channel();

        let (snd1, recv1) =
            unitype_upgrade_inv_raw_channel::<u32>(snd1, recv1, c1);

        recv1.handle(move |incoming| async move {
            match incoming {
                InvTypedIncoming::Event(evt) => {
                    println!("1evt: {:?}", evt);
                    assert_eq!(69, evt);
                }
                InvTypedIncoming::Request(data, respond) => {
                    respond.respond(data + 1).await;
                }
            }
            Ok(())
        });

        let (snd2, recv2) =
            unitype_upgrade_inv_raw_channel::<u32>(snd2, recv2, c2);

        recv2.handle(move |incoming| async move {
            match incoming {
                InvTypedIncoming::Event(evt) => {
                    println!("2evt: {:?}", evt);
                    assert_eq!(42, evt);
                }
                InvTypedIncoming::Request(data, respond) => {
                    respond.respond(data - 1).await;
                }
            }
            Ok(())
        });

        snd1.emit(42).await.unwrap();
        snd2.emit(69).await.unwrap();

        let res = snd1.request(42).await.unwrap();
        println!("1res: {:?}", res);
        assert_eq!(41, res);

        let res = snd2.request(42).await.unwrap();
        println!("2res: {:?}", res);
        assert_eq!(43, res);

        println!("done, about to drop");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_inv_broker() {
        let impl_spec = Arc::new(ImplSpecInner::default());

        let broker = new_inv_broker();

        let f = broker.register_impl_raw(impl_spec.clone()).await.unwrap();
        f.handle_sync(|s, r, c| {
            let (s, r) = unitype_upgrade_inv_raw_channel::<isize>(s, r, c);
            let s2 = s.clone();
            tokio::task::spawn(async move {
                s2.emit(11).await.unwrap();
                println!("from task res: {:?}", s2.request(11).await.unwrap());
            });
            r.handle(move |inc| {
                let s = s.clone();
                async move {
                    match inc {
                        InvTypedIncoming::Event(evt) => {
                            println!("impl evt: {:?}", evt);
                            println!("impl req: {:?}", s.request(42).await?);
                        }
                        InvTypedIncoming::Request(req, resp) => {
                            s.emit(req).await?;
                            resp.respond(req + 1).await;
                        }
                    }
                    Ok(())
                }
            });
            Ok(())
        });

        let (s, r, c) = broker.bind_to_impl_raw(impl_spec).await.unwrap();
        let (s, r) = unitype_upgrade_inv_raw_channel::<isize>(s, r, c);
        r.handle(|inc| async move {
            match inc {
                InvTypedIncoming::Event(evt) => {
                    println!("bind evt: {:?}", evt);
                }
                InvTypedIncoming::Request(req, resp) => {
                    resp.respond(req + 1).await;
                }
            }
            Ok(())
        });
        s.emit(42).await.unwrap();
        println!("bind res: {:?}", s.request(42).await.unwrap());
    }
}
