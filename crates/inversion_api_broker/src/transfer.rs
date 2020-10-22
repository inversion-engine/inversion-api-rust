//! Type erasure helpers for passing data through the inversion api broker.

use std::sync::Arc;
use std::any::Any;

/// message-pack encode
fn rmp_encode<T: serde::Serialize>(t: &T) -> std::io::Result<Vec<u8>> {
    let mut se = rmp_serde::encode::Serializer::new(Vec::new())
        .with_struct_map()
        .with_string_variants();
    t.serialize(&mut se)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    Ok(se.into_inner())
}

/// message-pack decode
fn rmp_decode<T>(r: &[u8]) -> std::io::Result<T>
where
    for<'de> T: Sized + serde::Deserialize<'de>,
{
    let mut de = rmp_serde::decode::Deserializer::new(r);
    T::deserialize(&mut de).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

/// Serializable that can be used as a trait-object
pub trait Serializable {
    /// Serialize this item into a Vec<u8>
    fn serialize(&self) -> std::io::Result<Vec<u8>>;
}

impl<T: serde::Serialize> Serializable for T {
    fn serialize(&self) -> std::io::Result<Vec<u8>> {
        rmp_encode(self)
    }
}

/// A fat pointer to an item that is both Any and Serializable
pub struct SerAny {
    p_any: Arc<dyn Any + 'static + Sync + Send>,
    p_ser: Arc<dyn Serializable + 'static + Sync + Send>,
}

impl SerAny {
    /// constructor
    pub fn new<T>(t: T) -> Self
    where
        T: serde::Serialize + 'static + Sync + Send,
    {
        let p = Arc::new(t);
        Self {
            p_any: p.clone(),
            p_ser: p,
        }
    }

    /// downcast
    pub fn downcast<T>(self) -> T
    where
        for<'de> T: serde::Deserialize<'de> + 'static + Sync + Send,
    {
        let SerAny { p_any, p_ser } = self;
        // first, try downcasting directly
        if let Ok(p) = p_any.downcast() {
            drop(p_ser);
            if let Ok(p) = Arc::try_unwrap(p) {
                return p;
            } else {
                // we managed the arc cloning with private variables
                // this shouldn't happen
                unreachable!();
            }
        }
        // otherwise, fall back to serialize / deserialize
        let p = p_ser.serialize().unwrap();
        rmp_decode(&p).unwrap()
    }
}

/// Marker trait to ensure: 'static + Sync + Send
pub trait Transferable: 'static + Sync + Send {}

/// Data that can be sent through the inversion api broker.
/// As an optimization, if the input/output types exactly match,
/// no serialization / deserialization will be performed.
pub enum Transfer {
    /// This type has already been serialized.
    /// We may have received it from another process.
    Serialized(Vec<u8>),

    /// This type is yet to be serialized.
    /// If it doesn't cross a process barrier, and the output
    /// type exactly matches, we may not need to serialize / deserialize.
    Typed(SerAny),
}

impl Transferable for Transfer {}

impl<T> From<T> for Transfer
where
    T: 'static + serde::Serialize + Sync + Send,
{
    fn from(t: T) -> Self {
        Self::Typed(SerAny::new(t))
    }
}

impl Transfer {
    /// Extract a concrete type from this transfer item.
    /// As an optimization, if the input/output types exactly match,
    /// no serialization / deserialization will be performed.
    pub fn deserialize<T>(self) -> T
    where
        for<'de> T: serde::Deserialize<'de> + 'static + Sync + Send,
    {
        match self {
            Transfer::Serialized(t) => {
                rmp_decode(&t).unwrap()
            }
            Transfer::Typed(t) => {
                t.downcast()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Bob(i32);
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Ned(i32);

        let bob = Bob(42);
        let bob: Transfer = bob.into();
        let bob: Bob = bob.deserialize();
        assert_eq!(Bob(42), bob);

        let bob = Bob(42);
        let bob: Transfer = bob.into();
        let ned: Ned = bob.deserialize();
        assert_eq!(Ned(42), ned);
    }
}
