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
//! # Features
//!
//! Feature `on-disk-utxo` is enabled by default,
//! which uses an on-disk cache to keep track of unspent transaction
//! for iterator `db.iter_connected_block`.
//!
//! To use in-memory UTXO cache for better performance,
//! use `default-features = false` to Cargo.toml,
//! which requires 32GB+ RAM.
//!

pub(crate) mod api;
pub mod iter;
pub mod parser;

#[doc(inline)]
pub use crate::api::*;
