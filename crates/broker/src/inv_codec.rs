//! inversion engine serialization codec

use crate::inv_any::*;
use crate::inv_api_spec::*;
use crate::inv_error::*;
use crate::inv_uniq::*;

/// Serialization codec for InversionApi Events.
/// Both sides can emit these events, both sides can receive them.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InvApiCodecEvt {
    /// Notify the remote end of a new local ImplSpec
    /// On new binding, both sides should emit events of this
    /// type for all pre-existing local specs.
    NewImplSpec {
        /// the ImplSpec
        impl_spec: ImplSpec,
    },

    /// a bus message from a bound protocol
    BoundMessage {
        /// the uniq id associated with this binding
        binding_id: InvUniq,

        /// the msg_id for this specific message
        msg_id: InvUniq,

        /// the data content of this message
        data: InvAny,
    },
}

/// Serialization codec for InversionApi Requests and Responses.
/// Both sides can send all requests and responses,
/// both sides can receive all requests and responses.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InvApiCodecReqRes {
    /// A process local to the sender of this message is requesting
    /// to bind to an Impl on the receiver of this message.
    NewImplBindingReq {
        /// a uniq id to use for messages related to this binding
        binding_id: InvUniq,

        /// the impl id to bind to
        impl_id: ImplSpecId,
    },

    /// if the binding was successful, return this message
    NewImplBindingRes {
        /// the uniq binding_id sent with the Req
        binding_id: InvUniq,

        /// if None, the result was a success.
        /// if Some, the result of the binding was an error.
        error: Option<InvError>,
    },
}

/*
use futures::future::{BoxFuture, FutureExt};
use futures::sink::SinkExt;

use parking_lot::Mutex;


use std::future::Future;
use std::sync::Arc;

/// Helper traits. You probably don't need this unless you are implementing a
/// custom inversion engine serialization codec.
pub mod traits {
    use super::*;

    /// Establish a message bus between a local InvBroker and a remote.
    /// This is the sending side.
    pub trait AsInvCodecSender: 'static + Send + Sync {
        /// Notify the remote end of a new local ImplSpec
        fn new_impl_spec(
            &self,
            impl_spec: ImplSpec,
        ) -> BoxFuture<'static, InvResult<()>>;
    }

    /// Establish a message bus between a local InvBroker and a remote.
    /// This is the handler side.
    pub trait AsInvCodecHandler: 'static + Send {}
}
use traits::*;

impl InvCodecMessage {
    /// Serialize
    pub fn encode(self) -> Vec<u8> {
        use serde::Serialize;
        let mut se = rmp_serde::encode::Serializer::new(Vec::new())
            .with_struct_map()
            .with_string_variants();
        self.serialize(&mut se).expect("failed serialize");
        se.into_inner()
    }
}

/// Establish a message bus between a local InvBroker and a remote InvBroker.
/// This is the sending side.
#[derive(Clone)]
pub struct InvCodecSender(Arc<dyn AsInvCodecSender>);

impl InvCodecSender {
    /// Notify the remote end of a new local ImplSpec
    pub fn new_impl_spec(
        &self,
        impl_spec: ImplSpec,
    ) -> impl Future<Output = InvResult<()>> + 'static + Send {
        AsInvCodecSender::new_impl_spec(&*self.0, impl_spec)
    }
}

/// Establish a message bus between a local InvBroker and a remote InvBroker.
/// This is the handler side.
pub struct InvCodecHandler(Box<dyn AsInvCodecHandler>);

impl InvCodecHandler {
    /*
    /// Handle incoming codec messages.
    pub fn handle<
        NewImplSpecCb
    >(self: Box<Self>, _new_impl_spec_cb: NewImplSpecCb)
    where
        NewImplSpecCb: Fn(ImplSpec)
    */
}

/// Create a codec message bus between a local InvBroker and a remote InvBroker.
pub fn inv_codec<R, W>(r: R, w: W) -> (InvCodecSender, InvCodecHandler)
where
    R: tokio::io::AsyncRead + 'static + Send,
    W: tokio::io::AsyncWrite + 'static + Send + Unpin,
{
    use tokio_util::codec::*;

    let mut builder = LengthDelimitedCodec::builder();
    builder.little_endian();

    let _r = builder.new_read(r);
    let w = builder.new_write(w);

    struct S<W>
    where
        W: tokio::io::AsyncWrite + 'static + Send + Unpin,
    {
        limit: Arc<tokio::sync::Semaphore>,
        inner: Arc<Mutex<Option<FramedWrite<W, LengthDelimitedCodec>>>>,
    }

    impl<W> AsInvCodecSender for S<W>
    where
        W: tokio::io::AsyncWrite + 'static + Send + Unpin,
    {
        fn new_impl_spec(
            &self,
            impl_spec: ImplSpec,
        ) -> BoxFuture<'static, InvResult<()>> {
            let limit = self.limit.clone();
            let inner = self.inner.clone();
            async move {
                let _permit = limit
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(InvError::other)?;

                // if we have a permit, the sender is available
                let mut raw_send = inner.lock().take().unwrap();

                //let uniq = InvUniq::new_rand();

                // TODO - register this uniq so we can reference later

                let encoded = InvCodecMessage::NewImplSpec {
                    //ref_id: uniq,
                    impl_spec,
                }
                .encode();

                let res = raw_send
                    .send(encoded.into())
                    .await
                    .map_err(InvError::other);

                *(inner.lock()) = Some(raw_send);

                // TODO - on error close the channel
                res
            }
            .boxed()
        }
    }

    let s = InvCodecSender(Arc::new(S {
        limit: Arc::new(tokio::sync::Semaphore::new(1)),
        inner: Arc::new(Mutex::new(Some(w))),
    }));

    struct H;

    impl AsInvCodecHandler for H {}

    let h = InvCodecHandler(Box::new(H));

    (s, h)
}
*/
