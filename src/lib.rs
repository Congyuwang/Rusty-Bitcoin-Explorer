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
//! ## Caveat
//!
//! Currently it only supports all standard script types.
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
