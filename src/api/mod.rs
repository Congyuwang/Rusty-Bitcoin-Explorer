//!
//! Crates APIs, essential structs, functions, methods are all here!
//!
//! To quickly understand how to use this crate, have a look at the
//! documentation for `bitcoin_explorer::BitcoinDB`!!.
//!
//! # Example
//!
//! ```rust
//! use bitcoin_explorer::BitcoinDB;
//! use std::path::Path;
//!
//! let path = Path::new("/Users/me/bitcoin").unwrap();
//!
//! // launch without reading txindex
//! let db = BitcoinDB::new(path, false).unwrap();
//!
//! // launch attempting to read txindex
//! let db = BitcoinDB::new(path, true).unwrap();
//! ```
//!

pub mod connected;

use crate::parser::blk_file::BlkFile;
use crate::parser::errors::{OpError, OpResult};
use crate::parser::script::{evaluate_script, ScriptInfo};
use crate::parser::tx_index::TxDB;
use std::path::Path;
// re-exports
pub use crate::iter::{BlockIter, ConnectedBlockIter};
pub use crate::parser::block_index::{BlockIndex, BlockIndexRecord};
pub use crate::parser::proto::connected_proto::{
    BlockConnectable, FConnectedBlock, FConnectedTransaction, SConnectedBlock,
    SConnectedTransaction, TxConnectable,
};
pub use crate::parser::proto::full_proto::{FBlock, FBlockHeader, FTransaction, FTxOut};
pub use crate::parser::proto::simple_proto::{SBlock, SBlockHeader, STransaction, STxOut};
pub use bitcoin::hashes::hex::{FromHex, ToHex};
pub use bitcoin::{Address, Block, BlockHash, BlockHeader, Network, Script, Transaction, Txid};

///
/// Extract addresses from a script public key.
///
#[inline]
pub fn parse_script(script_pub_key: &str) -> OpResult<ScriptInfo> {
    let script = Script::from_hex(&script_pub_key)?;
    Ok(evaluate_script(&script, Network::Bitcoin))
}

///
/// This is the main struct of this crate!! Click and read the doc.
///
/// All queries start from initializing `BitcoinDB`.
///
pub struct BitcoinDB {
    pub block_index: BlockIndex,
    pub blk_file: BlkFile,
    pub tx_db: TxDB,
}

impl BitcoinDB {
    ///
    /// This is the main structure for reading Bitcoin blockchain data.
    ///
    /// Instantiating this class by passing the `-datadir` directory of
    /// Bitcoin core to the `new()` method.
    /// `tx_index`: whether to try to open tx_index levelDB.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bitcoin_explorer::BitcoinDB;
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// // launch attempting to read txindex
    /// let db = BitcoinDB::new(path, true).unwrap();
    /// ```
    pub fn new(p: &Path, tx_index: bool) -> OpResult<BitcoinDB> {
        if !p.exists() {
            return Err(OpError::from("data_dir does not exist"));
        }
        let blk_path = p.join("blocks");
        let index_path = blk_path.join("index");
        let block_index = BlockIndex::new(index_path.as_path())?;
        let tx_db = if tx_index {
            let tx_index_path = p.join("indexes").join("txindex");
            TxDB::new(&tx_index_path, &block_index)
        } else {
            TxDB::null()
        };
        Ok(BitcoinDB {
            block_index,
            blk_file: BlkFile::new(blk_path.as_path())?,
            tx_db,
        })
    }

    ///
    /// Get the maximum height found in block index.
    ///
    /// It is not necessarily true that any height smaller than
    /// `get_max_height()` can be used to query `get_block()`.
    ///
    /// This max_height is only the max height of block header
    /// information. The actual block data might not have been
    /// synced for querying. `get_block_header()`, `get_height_from_hash()`
    /// `get_hash_from_height()`, will necessarily return valid
    /// data, as long as height is smaller than `get_max_height()`.
    ///
    pub fn get_max_height(&self) -> usize {
        self.block_index.records.len()
    }

    ///
    /// Get block header information.
    ///
    /// This is an in-memory query, thus very fast.
    /// This method is useful for computing blockchain statistics.
    ///
    /// # Example
    ///
    /// ## Compute total number of transactions
    /// ```rust
    /// use bitcoin_explorer::BitcoinDB;
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// let mut total_number: usize = 0;
    ///
    /// // This computation should finish immediately. No Disk Access.
    /// for i in 0..700000 {
    ///     let header = db.get_header(i).unwrap();
    ///     total_number += header.n_tx as usize;
    /// }
    /// println!("total tx from block 0 to 700000: {}.", total_number);
    /// ```
    ///
    pub fn get_header(&self, height: usize) -> OpResult<&BlockIndexRecord> {
        if let Some(header) = self.block_index.records.get(height) {
            Ok(header)
        } else {
            Err(OpError::from("height not found"))
        }
    }

    ///
    /// Get block hash of a certain height.
    ///
    pub fn get_hash_from_height(&self, height: usize) -> OpResult<BlockHash> {
        match self.block_index.records.get(height) {
            None => Err(OpError::from("height not found")),
            Some(s) => Ok(s.block_header.block_hash()),
        }
    }

    ///
    /// Get block height of certain hash.
    ///
    /// Note that the hash is a hex string of the block hash.
    ///
    pub fn get_height_from_hash(&self, hash: &str) -> OpResult<i32> {
        match self.block_index.hash_to_height.get(hash) {
            None => Err(OpError::from("hash not found")),
            Some(h) => Ok(*h),
        }
    }

    ///
    /// Get a block (in different formats (Block, FBlock, SBlock))
    ///
    /// # Example
    /// ```rust
    /// use bitcoin_explorer::{BitcoinDB, FBlock, SBlock, Block};
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// // get block of height 600000 (in different formats)
    /// let block: Block = db.get_block(600000).unwrap();
    /// let block: FBlock = db.get_block(600000).unwrap();
    /// let block: SBlock = db.get_block(600000).unwrap();
    /// ```
    ///
    pub fn get_block<T: From<Block>>(&self, height: i32) -> OpResult<T> {
        if let Some(index) = self.block_index.records.get(height as usize) {
            let blk = self.blk_file.read_block(index.n_file, index.n_data_pos)?;
            Ok(blk.into())
        } else {
            Err(OpError::from("height not found"))
        }
    }

    /// Get a transaction by providing txid.
    ///
    /// This function requires `txindex` to be set to `true`,
    /// and `txindex=1` when running Bitcoin Core.
    ///
    /// # Example
    /// ```rust
    /// use bitcoin_explorer::{BitcoinDB, Transaction, FTransaction, STransaction, Txid, FromHex};
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // !!must launch with txindex=true!!
    /// let db = BitcoinDB::new(path, true).unwrap();
    ///
    /// // get transaction
    /// // e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
    /// let txid_str = "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468";
    /// let txid = Txid::from_hex(txid_str).unwrap();
    ///
    /// // get transactions in different formats
    /// let tx: Transaction = db.get_transaction(&txid).unwrap();
    /// let tx: FTransaction = db.get_transaction(&txid).unwrap();
    /// let tx: STransaction = db.get_transaction(&txid).unwrap();
    /// ```
    ///
    pub fn get_transaction<T: From<Transaction>>(&self, txid: &Txid) -> OpResult<T> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let record = self.tx_db.get_tx_record(txid)?;
        let tx = self
            .blk_file
            .read_transaction(record.n_file, record.n_pos, record.n_tx_offset)?;
        Ok(tx.into())
    }

    ///
    /// Get the height of the block containing a particular transaction.
    ///
    /// This function requires `txindex` to be set to `true`,
    /// and `txindex=1` when running Bitcoin Core.
    ///
    pub fn get_height_of_transaction(&self, txid: &Txid) -> OpResult<i32> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        self.tx_db.get_block_height_of_tx(txid)
    }

    ///
    /// Iterate through all blocks from `start` to `end` (excluded).
    ///
    /// Formats: `Block` / `FBlock` / `SBlock`.
    ///
    /// # Performance
    ///
    /// This iterator is implemented to read the blocks in concurrency,
    /// but the result is still produced in sequential order.
    /// Results read are stored in a synced queue for `next()`
    /// to get.
    ///
    /// The iterator stops automatically when a block cannot be
    /// read (i.e., when the max height in the database met).
    ///
    /// This is a very efficient implementation.
    /// Iterating from height 600000 to 700000 should take less than
    /// 30 minutes.
    /// The performance bottleneck is likely to be disk IO.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bitcoin_explorer::{BitcoinDB, Block, SBlock, FBlock};
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// // iterate over block from 600000 to 700000
    /// for block in db.iter_block::<Block>(600000, 700000) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    ///
    /// // iterate over block from 600000 to 700000
    /// for block in db.iter_block::<FBlock>(600000, 700000) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    ///
    /// // iterate over block from 600000 to 700000
    /// for block in db.iter_block::<SBlock>(600000, 700000) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    /// ```
    ///
    pub fn iter_block<T>(&self, start: u32, end: u32) -> BlockIter<T>
    where
        T: From<Block> + Send + 'static,
    {
        BlockIter::from_range(self, start, end)
    }

    ///
    /// Iterate through all blocks for a given heights (excluded).
    ///
    /// Formats: `Block` / `FBlock` / `SBlock`.
    ///
    /// # Performance
    /// This iterator is implemented to read the blocks in concurrency,
    /// but the result is still produced in the given order in `heights`.
    /// Results read are stored in a synced queue for `next()`
    /// to get.
    ///
    /// This is a very efficient implementation.
    /// Iterating from height 600000 to 700000 should take less than
    /// 30 minutes.
    /// The performance bottleneck is likely to be disk IO.
    ///
    /// ## Fails Fast
    ///
    /// The iterator stops immediately when a `height` cannot be found.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bitcoin_explorer::{BitcoinDB, Block, FBlock, SBlock};
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// let some_heights = vec![3, 5, 7, 9];
    ///
    /// // iterate over blocks from 600000 to 700000
    /// for block in db.iter_heights::<Block>(some_heights) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    ///
    /// // iterate over simple blocks from 600000 to 700000
    /// for block in db.iter_heights::<SBlock>(some_heights.clone()) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    ///
    /// // iterate over full blocks from 600000 to 700000
    /// for block in db.iter_heights::<FBlock>(some_heights.clone()) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    ///
    ///
    /// ```
    ///
    pub fn iter_heights<T: 'static + From<Block> + Send>(&self, heights: Vec<u32>) -> BlockIter<T> {
        BlockIter::new(self, heights)
    }
}
