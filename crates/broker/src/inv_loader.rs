//! inversion engine loaders

use crate::inv_broker::InvBroker;
use crate::inv_error::*;

/// Execute a subprocess and connect the provided InvBroker
/// to the child's InvBroker via IPC over child stdin/stdout.
pub async fn inv_loader_as_parent_subprocess<P: AsRef<std::ffi::OsStr>>(
    _broker: InvBroker,
    _subprocess_path: P,
) -> InvResult<()> {
    unimplemented!()
}

/// Assume we have been executed by a parent process.
/// Connect the provided InvBroker to the parent processes InvBroker
/// via IPC over our stdin/stdout.
pub async fn inv_loader_as_child_subprocess(
    _broker: InvBroker,
) -> InvResult<()> {
    unimplemented!()
}
