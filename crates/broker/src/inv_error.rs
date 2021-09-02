//! inversion api error type

use std::sync::Arc;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
enum KindTx {
    NotFound,
    PermissionDenied,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    AddrInUse,
    AddrNotAvailable,
    BrokenPipe,
    AlreadyExists,
    WouldBlock,
    InvalidInput,
    InvalidData,
    TimedOut,
    WriteZero,
    Interrupted,
    UnexpectedEof,
    Unsupported,
    OutOfMemory,
    #[serde(other)]
    Other,
}

impl From<std::io::ErrorKind> for KindTx {
    fn from(k: std::io::ErrorKind) -> Self {
        match k {
            std::io::ErrorKind::NotFound => Self::NotFound,
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused => Self::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset => Self::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted => Self::ConnectionAborted,
            std::io::ErrorKind::NotConnected => Self::NotConnected,
            std::io::ErrorKind::AddrInUse => Self::AddrInUse,
            std::io::ErrorKind::AddrNotAvailable => Self::AddrNotAvailable,
            std::io::ErrorKind::BrokenPipe => Self::BrokenPipe,
            std::io::ErrorKind::AlreadyExists => Self::AlreadyExists,
            std::io::ErrorKind::WouldBlock => Self::WouldBlock,
            std::io::ErrorKind::InvalidInput => Self::InvalidInput,
            std::io::ErrorKind::InvalidData => Self::InvalidData,
            std::io::ErrorKind::TimedOut => Self::TimedOut,
            std::io::ErrorKind::WriteZero => Self::WriteZero,
            std::io::ErrorKind::Interrupted => Self::Interrupted,
            std::io::ErrorKind::UnexpectedEof => Self::UnexpectedEof,
            std::io::ErrorKind::Unsupported => Self::Unsupported,
            std::io::ErrorKind::OutOfMemory => Self::OutOfMemory,
            _ => Self::Other,
        }
    }
}

impl From<KindTx> for std::io::ErrorKind {
    fn from(k: KindTx) -> Self {
        match k {
            KindTx::NotFound => Self::NotFound,
            KindTx::PermissionDenied => Self::PermissionDenied,
            KindTx::ConnectionRefused => Self::ConnectionRefused,
            KindTx::ConnectionReset => Self::ConnectionReset,
            KindTx::ConnectionAborted => Self::ConnectionAborted,
            KindTx::NotConnected => Self::NotConnected,
            KindTx::AddrInUse => Self::AddrInUse,
            KindTx::AddrNotAvailable => Self::AddrNotAvailable,
            KindTx::BrokenPipe => Self::BrokenPipe,
            KindTx::AlreadyExists => Self::AlreadyExists,
            KindTx::WouldBlock => Self::WouldBlock,
            KindTx::InvalidInput => Self::InvalidInput,
            KindTx::InvalidData => Self::InvalidData,
            KindTx::TimedOut => Self::TimedOut,
            KindTx::WriteZero => Self::WriteZero,
            KindTx::Interrupted => Self::Interrupted,
            KindTx::UnexpectedEof => Self::UnexpectedEof,
            KindTx::Unsupported => Self::Unsupported,
            KindTx::OutOfMemory => Self::OutOfMemory,
            _ => Self::Other,
        }
    }
}

/// Inversion api error type.
#[derive(Clone)]
pub struct InvError(pub Arc<std::io::Error>);

impl serde::Serialize for InvError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let kind: KindTx = self.kind().into();
        let source = self.get_ref().map(|e| format!("{:?}", e));
        let os = self.raw_os_error();
        (kind, source, os).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for InvError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (kind, source, os): (KindTx, Option<String>, Option<i32>) =
            serde::Deserialize::deserialize(deserializer)?;
        let io = if source.is_some() {
            std::io::Error::new(kind.into(), source.unwrap())
        } else if os.is_some() {
            std::io::Error::from_raw_os_error(os.unwrap())
        } else {
            std::io::Error::new(kind.into(), "unknown")
        };
        Ok(Self(Arc::new(io)))
    }
}

impl InvError {
    /// Construct an "other" type InvError.
    pub fn other<E>(e: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        std::io::Error::new(std::io::ErrorKind::Other, e).into()
    }
}

impl AsRef<std::io::Error> for InvError {
    fn as_ref(&self) -> &std::io::Error {
        &self.0
    }
}

impl std::ops::Deref for InvError {
    type Target = std::io::Error;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::borrow::Borrow<std::io::Error> for InvError {
    fn borrow(&self) -> &std::io::Error {
        self.as_ref()
    }
}

impl std::fmt::Debug for InvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for InvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for InvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<std::io::Error> for InvError {
    fn from(e: std::io::Error) -> Self {
        Self(Arc::new(e))
    }
}

impl From<std::io::ErrorKind> for InvError {
    fn from(e: std::io::ErrorKind) -> Self {
        std::io::Error::from(e).into()
    }
}

impl From<String> for InvError {
    fn from(e: String) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e).into()
    }
}

impl From<&String> for InvError {
    fn from(e: &String) -> Self {
        e.to_string().into()
    }
}

impl From<&str> for InvError {
    fn from(e: &str) -> Self {
        e.to_string().into()
    }
}

impl From<()> for InvError {
    fn from(_: ()) -> Self {
        "[unit error]".into()
    }
}

/// Inversion api result type.
pub type InvResult<T> = std::result::Result<T, InvError>;
