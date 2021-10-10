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
//! `db.iter_connected_block()` uses in-memory UTXO cache by default,
//! which requires 32GB ore more memory, but is very fast.
//! It can be configured to use on-disk UTXO cache for tracking
//! unspent transactions during iterations,
//! which usually requires less than 1GB memory.
//!
//! **By default, UTXO is stored on RAM, which requires 32GM memory
//! (it needs this much memory only for `db.iter_connected_block()`)**
//! ```toml
//! [dependencies.bitcoin-explorer]
//! version = "1.2"
//! ```
//!
//! Enable on-disk UTXO cache if your memory is limited:
//! ```toml
//! [dependencies.bitcoin-explorer]
//! version = "1.2"
//! features = ["on-disk-utxo"]
//! ```
//!
//! ### Guide to Feature
//! When you have a large memory (>= 32 GB), do not use `on-disk-utxo`.
//! In-memory UTXO cache provides blazing fast speed.
//!
//! Use `on-disk-utxo` only when your disk is limited.
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
