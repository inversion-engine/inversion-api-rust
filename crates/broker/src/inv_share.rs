//! Synchronized droppable share-lock around internal state data.

use crate::inv_error::*;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic;
use std::sync::Arc;

/// Callback for delayed initialization of InvShare
pub type InvShareInitCb<T> = Box<dyn FnOnce(T) + 'static + Send>;

/// Synchronized droppable share-lock around internal state data.
pub struct InvShare<T: 'static + Send>(
    Arc<dyn AsShareSDyn<T> + 'static + Send + Sync>,
);

impl<T: 'static + Send> Clone for InvShare<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: 'static + Send> PartialEq for InvShare<T> {
    fn eq(&self, oth: &Self) -> bool {
        self.0.dyn_eq(&oth.0)
    }
}

impl<T: 'static + Send> Eq for InvShare<T> {}

impl<T: 'static + Send> std::hash::Hash for InvShare<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.dyn_hash(state);
    }
}

impl<T: 'static + Send + Sync> InvShare<T> {
    /// Create a new share lock, backed by a parking_lot::RwLock.
    pub fn new_rw_lock(t: T) -> Self {
        let (i, cb) = Self::new_rw_lock_delayed();
        cb(t);
        i
    }

    /// Create a new share lock, backed by a parking_lot::RwLock.
    /// Api calls before initializer will Err(ConnectionReset).
    pub fn new_rw_lock_delayed() -> (Self, InvShareInitCb<T>) {
        let (i, cb) = SDynRwLock::new();
        (Self(i), cb)
    }
}

impl<T: 'static + Send> InvShare<T> {
    /// Create a new share lock, backed by a parking_lot::Mutex.
    pub fn new_mutex(t: T) -> Self {
        let (i, cb) = Self::new_mutex_delayed();
        cb(t);
        i
    }

    /// Create a new share lock, backed by a parking_lot::Mutex.
    /// Api calls before initializer will Err(ConnectionReset).
    pub fn new_mutex_delayed() -> (Self, InvShareInitCb<T>) {
        let (i, cb) = SDynMutex::new();
        (Self(i), cb)
    }

    /// Execute code with read-only access to the internal state.
    pub fn share_ref<R, F>(&self, f: F) -> InvResult<R>
    where
        F: FnOnce(&T) -> InvResult<R>,
    {
        let guard = self.0.get_ref();
        if guard.is_none() {
            return Err(std::io::ErrorKind::ConnectionReset.into());
        }
        f((**guard).as_ref().unwrap())
    }

    /// Execute code with mut access to the internal state.
    /// The second param, if set to true, will drop the shared state,
    /// any further access will `Err(ConnectionAborted)`.
    /// E.g. `share.share_mut(|_state, close| *close = true).unwrap();`
    pub fn share_mut<R, F>(&self, f: F) -> InvResult<R>
    where
        F: FnOnce(&mut T, &mut bool) -> InvResult<R>,
    {
        let (r, v) = {
            let mut guard = self.0.get_mut();
            if guard.is_none() {
                return Err(std::io::ErrorKind::ConnectionReset.into());
            }
            let mut close = false;
            let r = f((**guard).as_mut().unwrap(), &mut close);
            if close {
                let v = guard.take();
                (r, v)
            } else {
                (r, None)
            }
        };
        // make sure the lock is released before drop
        drop(v);
        r
    }

    /// Extract the contents of this share, closing it in the process.
    /// If the share was already closed, will return None.
    pub fn extract(&self) -> Option<T> {
        self.0.get_mut().take()
    }

    /// Returns true if the internal state has been dropped.
    pub fn is_closed(&self) -> bool {
        self.0.get_ref().is_none()
    }

    /// Explicity drop the internal state.
    pub fn close(&self) {
        let v = self.0.get_mut().take();
        // make sure the lock is released before drop
        drop(v);
    }
}

// -- private -- //

static UNIQ: atomic::AtomicU64 = atomic::AtomicU64::new(1);

type SDynGuardRef<'lt, T> = Box<dyn std::ops::Deref<Target = Option<T>> + 'lt>;
type SDynGuardMut<'lt, T> =
    Box<dyn std::ops::DerefMut<Target = Option<T>> + 'lt>;

trait AsShareSDyn<T: 'static + Send> {
    fn get_ref(&self) -> SDynGuardRef<'_, T>;
    fn get_mut(&self) -> SDynGuardMut<'_, T>;
    fn dyn_eq(&self, oth: &dyn std::any::Any) -> bool;
    fn dyn_hash(&self, hasher: &mut dyn std::hash::Hasher);
}

struct SDynMutex<T: 'static + Send>(Mutex<Option<T>>, u64);

impl<T: 'static + Send> SDynMutex<T> {
    fn new() -> (Arc<Self>, InvShareInitCb<T>) {
        let this = Arc::new(Self(
            Mutex::new(None),
            UNIQ.fetch_add(1, atomic::Ordering::Relaxed),
        ));
        let this2 = this.clone();
        let cb: InvShareInitCb<T> = Box::new(move |t| {
            *this2.0.lock() = Some(t);
        });
        (this, cb)
    }
}

impl<T: 'static + Send> AsShareSDyn<T> for SDynMutex<T> {
    fn get_ref(&self) -> SDynGuardRef<'_, T> {
        Box::new(self.0.lock())
    }

    fn get_mut(&self) -> SDynGuardMut<'_, T> {
        Box::new(self.0.lock())
    }

    fn dyn_eq(&self, oth: &dyn std::any::Any) -> bool {
        let c: &Self = match <dyn std::any::Any>::downcast_ref(oth) {
            None => return false,
            Some(c) => c,
        };
        self.1 == c.1
    }

    fn dyn_hash(&self, hasher: &mut dyn std::hash::Hasher) {
        std::hash::Hash::hash(&self.1, &mut Box::new(hasher))
    }
}

struct SDynRwLock<T: 'static + Send + Sync>(RwLock<Option<T>>, u64);

impl<T: 'static + Send + Sync> SDynRwLock<T> {
    fn new() -> (Arc<Self>, InvShareInitCb<T>) {
        let this = Arc::new(Self(
            RwLock::new(None),
            UNIQ.fetch_add(1, atomic::Ordering::Relaxed),
        ));
        let this2 = this.clone();
        let cb: InvShareInitCb<T> = Box::new(move |t| {
            *this2.0.write() = Some(t);
        });
        (this, cb)
    }
}

impl<T: 'static + Send + Sync> AsShareSDyn<T> for SDynRwLock<T> {
    fn get_ref(&self) -> SDynGuardRef<'_, T> {
        Box::new(self.0.read())
    }

    fn get_mut(&self) -> SDynGuardMut<'_, T> {
        Box::new(self.0.write())
    }

    fn dyn_eq(&self, oth: &dyn std::any::Any) -> bool {
        let c: &Self = match <dyn std::any::Any>::downcast_ref(oth) {
            None => return false,
            Some(c) => c,
        };
        self.1 == c.1
    }

    fn dyn_hash(&self, hasher: &mut dyn std::hash::Hasher) {
        std::hash::Hash::hash(&self.1, &mut Box::new(hasher))
    }
}
