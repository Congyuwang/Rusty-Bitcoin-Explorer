//!
//! # Introduction
//!
//! This library is designed for efficient and massive deserialization
//! of the binary Bitcoin Core block files.
//!
//! It decodes all transactions, addresses, script types,
//! connects outpoints of inputs to outputs, to figure out
//! input addresses.
//!
//! This library allows efficient and versatile reading of all
//! bitcoin transaction records. This is good for analysis and research on
//! bitcoin trading behaviour.
//!
//! ## Features
//! By default, when iterating through blocks with inputs addresses attached
//! (db.iter_connected_block()), the unspent transaction outputs (UTXO)
//! are stored on disk using rocksdb. Feature on-disk-utxo is enabled by default.
//! Disabling on-disk-utxo feature automatically enables in-memory UTXO cache,
//! which is very fast but requires 32GB memory or more.
//!
//! *By default, UTXO is stored on disk, which usually requires less than 1GB memory.*
//! ```toml
//! [dependencies.explorer]
//! version = "1.2"
//! ```
//!
//! To enable in-memory UTXO. Set the default features to false:
//!
//! *Notice that with in-memory-utxo, db.iter_connected_block() currently uses 32GB RAM.*
//! ```toml
//! [dependencies.explorer]
//! version = "1.2"
//! default-features = false
//! ```
//!
//! ## Caveat
//!
//! Currently it only decode addresses from standard script types.
//!
//! # Example
//!
//! ```rust
//! use bitcoin_explorer::BitcoinDB;
//! use std::path::Path;
//!
//! let path = Path::new("/Users/me/bitcoin");
//!
//! // launch without reading txindex
//! let db = BitcoinDB::new(path, false).unwrap();
//!
//! // launch attempting to read txindex
//! let db = BitcoinDB::new(path, true).unwrap();
//! ```
//!

pub(crate) mod api;
pub mod iter;
pub mod parser;

#[doc(inline)]
pub use crate::api::*;
