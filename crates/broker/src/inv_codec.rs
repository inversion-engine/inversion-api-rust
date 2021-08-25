//! inversion engine serialization codec

use futures::future::{BoxFuture, FutureExt};

use parking_lot::Mutex;

use crate::inv_api_spec::*;
use crate::inv_error::*;

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
}
use traits::*;

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
pub struct InvCodecHandler;

/// Create a codec message bus between a local InvBroker and a remote InvBroker.
pub fn inv_codec<R, W>(r: R, w: W) -> (InvCodecSender, InvCodecHandler)
where
    R: tokio::io::AsyncRead + 'static + Send,
    W: tokio::io::AsyncWrite + 'static + Send,
{
    use tokio_util::codec::*;

    let mut builder = LengthDelimitedCodec::builder();
    builder.little_endian();

    let _r = builder.new_read(r);
    let w = builder.new_write(w);

    struct S<W>
    where
        W: tokio::io::AsyncWrite + 'static + Send,
    {
        limit: Arc<tokio::sync::Semaphore>,
        inner: Arc<Mutex<Option<FramedWrite<W, LengthDelimitedCodec>>>>,
    }

    impl<W> AsInvCodecSender for S<W>
    where
        W: tokio::io::AsyncWrite + 'static + Send,
    {
        fn new_impl_spec(
            &self,
            _impl_spec: ImplSpec,
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
                let raw_send = inner.lock().take().unwrap();

                // TODO - send impl_spec

                *(inner.lock()) = Some(raw_send);
                Ok(())
            }
            .boxed()
        }
    }

    let s = InvCodecSender(Arc::new(S {
        limit: Arc::new(tokio::sync::Semaphore::new(1)),
        inner: Arc::new(Mutex::new(Some(w))),
    }));

    let h = InvCodecHandler;

    (s, h)
}
