//! inversion api error type

use std::sync::Arc;

/// Inversion api error type.
#[derive(Clone)]
pub struct InvError(pub Arc<std::io::Error>);

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
