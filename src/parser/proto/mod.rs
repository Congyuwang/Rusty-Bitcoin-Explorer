//!
//! ## Basic Block Types
//!
//! There are three variants of basic block types.
//! - Block: imported from rust-bitcoin
//! - FBlock: `full_proto::FBlock`, with extra info pre-computed.
//! - SBlock: `simple_proto::SBlock`, with minimal amount of necessary info.
//!
//! For details, see the struct documentations.
//!
//! ## Connected Blocks
//!
//! Connected blocks are blocks with input replaced by referred outputs.
//! There are two types:
//! - `SConnectedBlock`
//! - `FConnectedBlock`
//! Corresponding to the basic F/S Blocks.
//!

/// connect outpoints of inputs to previous outputs
pub mod connected_proto;

/// add block hash, transaction id, script type, addresses to original `bitcoin::Block`
pub mod full_proto;

/// simplified blockchain objects, for faster python processing
pub mod simple_proto;
