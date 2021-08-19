#![deny(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
//! Core crates for loading and communicating with inversion api implementations in the Rust programming language.

pub mod inv_any;
pub mod inv_api_spec;
pub mod inv_broker;
pub mod inv_error;
pub mod inv_id;
pub mod inv_share;
pub mod inv_uniq;

pub use inv_error::*;
