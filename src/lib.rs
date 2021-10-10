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
//! Support optional in-memory unspent transaction cache (UTXO).
//! By default, UTXO is stored on disk (using rocksdb).
//! If you have a large memory (32GB or more),
//! enabling in-memory UTXO could accelerate `db.iter_connected_block()`.
//!
//! To enable in-memory UTXO. Set the default features to `false`:
//!
//! *Notice that with `in-memory-utxo`, `db.iter_connected_block()` currently uses 32GB RAM.*
//! ```toml
//! [dependencies.explorer]
//! version = "1.2"
//! default-features = false
//! ```
//!
//! *By default, UTXO is stored on disk, which usually requires less than 1GB memory.*
//! ```toml
//! [dependencies.explorer]
//! version = "1.2"
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
