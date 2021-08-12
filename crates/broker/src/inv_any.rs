//! Type erasure helpers for passing data through the inversion api broker.

use crate::inv_error::*;
use parking_lot::Mutex;
use std::any::Any;
use std::sync::Arc;

/// message-pack encode
fn rmp_encode<T: serde::Serialize>(t: &T) -> InvResult<Vec<u8>> {
    let mut se = rmp_serde::encode::Serializer::new(Vec::new())
        .with_struct_map()
        .with_string_variants();
    t.serialize(&mut se).map_err(InvError::other)?;
    Ok(se.into_inner())
}

/// message-pack decode
fn rmp_decode<T>(r: &[u8]) -> InvResult<T>
where
    for<'de> T: Sized + serde::Deserialize<'de>,
{
    let mut de = rmp_serde::decode::Deserializer::new(r);
    T::deserialize(&mut de).map_err(InvError::other)
}

type InvSerCb = Arc<dyn Fn() -> InvResult<Vec<u8>> + 'static + Send + Sync>;

enum InvAnyInner {
    /// Already serialized item
    Ser(Box<[u8]>),
    /// A fat pointer to an item that is both Any and Serializable
    Ptr {
        p_any: Arc<dyn Any + 'static + Sync + Send>,
        p_ser: InvSerCb,
    },
}

/// transfer item with some optimizations to try to avoid serialization
pub struct InvAny(InvAnyInner);

impl InvAny {
    /// constructor from bytes
    pub fn from_bytes(b: Box<[u8]>) -> Self {
        Self(InvAnyInner::Ser(b))
    }

    /// constructor
    pub fn new<T>(t: T) -> Self
    where
        T: serde::Serialize + 'static + Send,
    {
        let p_any = Arc::new(Mutex::new(t));
        let p_ser = p_any.clone();
        let p_ser = Arc::new(move || rmp_encode(&*p_ser.lock()));
        Self(InvAnyInner::Ptr { p_any, p_ser })
    }

    /// downcast
    pub fn downcast<T>(self) -> InvResult<T>
    where
        for<'de> T: serde::Deserialize<'de> + 'static + Send,
    {
        match self.0 {
            InvAnyInner::Ser(b) => rmp_decode(&b),
            InvAnyInner::Ptr { p_any, p_ser } => {
                // first, try downcasting directly
                if let Ok(p) = p_any.downcast::<Mutex<T>>() {
                    drop(p_ser);
                    if let Ok(p) = Arc::try_unwrap(p) {
                        return Ok(p.into_inner());
                    } else {
                        // we managed the arc cloning with private variables
                        // this shouldn't happen
                        unreachable!();
                    }
                }
                // otherwise, fall back to serialize / deserialize
                let p = p_ser()?;
                rmp_decode(&p)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inv_any() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Bob(i32);
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Ned(i32);

        let bob = Bob(42);
        let bob = InvAny::new(bob);
        let bob: Bob = bob.downcast().unwrap();
        assert_eq!(Bob(42), bob);

        let bob = Bob(42);
        let bob = InvAny::new(bob);
        let ned: Ned = bob.downcast().unwrap();
        assert_eq!(Ned(42), ned);
    }
}
