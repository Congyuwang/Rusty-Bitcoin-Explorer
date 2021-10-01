//!
//! This module defines the infrastructure for efficient iteration over blocks
//!

mod fetch_async;
mod fetch_connected_async;
mod iter_block;
mod iter_connected;
mod util;

pub use iter_block::BlockIter;
pub use iter_connected::ConnectedBlockIter;
