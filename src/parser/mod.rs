//!
//! This module defines how to parse binary data on disk to Block structs defined in proto.
//!

/// read transactions and blocks from blk.dat files
pub mod blk_file;

/// read block index in memory from levelDB
pub mod block_index;

/// define binary file readers
pub mod reader;

/// add multi-sig pattern recognition and decode addresses from multi-sig script
pub mod script;

/// on disk transaction index database
pub mod tx_index;

/// various formats of blockchain data representation
pub mod proto;

/// error handling
pub mod errors;
