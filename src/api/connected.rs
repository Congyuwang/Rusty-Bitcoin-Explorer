//!
//! implementation of methods that retrieve block info with outpoints connected
//!
use crate::api::{BitcoinDB, BlockConnectable, ConnectedBlockIter, TxConnectable, Txid};
use crate::parser::errors::{OpError, OpResult};

impl BitcoinDB {
    ///
    /// Get a block with inputs replaced by connected outputs.
    ///
    /// This function requires `txindex` to be set to `true` for `BitcoinDB`,
    /// and requires that flag `txindex=1` has been enabled when
    /// running Bitcoin Core.
    ///
    /// A transaction cannot be found using this function if it is
    /// not yet indexed using `txindex`.
    ///
    /// # Caveat!!
    ///
    /// ## Performance Warning
    ///
    /// Slow! For massive computation, use `db.iter_connected_block()`.
    ///
    pub fn get_connected_block<T: BlockConnectable>(&self, height: usize) -> OpResult<T> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let tx = self.get_block(height)?;
        T::connect(tx, &self.tx_db, &self.block_index, &self.blk_file)
    }

    ///
    /// Get a transaction with outpoints replaced by outputs.
    ///
    /// This function requires `txindex` to be set to `true` for `BitcoinDB`,
    /// and requires that flag `txindex=1` has been enabled when
    /// running Bitcoin Core.
    ///
    /// A transaction cannot be found using this function if it is
    /// not yet indexed using `txindex`.
    ///
    /// Format: `full (FConnectedTransaction)` / `simple (SConnectedTransaction)`.
    ///
    /// # Caveats
    ///
    /// ## Performance Warning
    ///
    /// Slow! For massive computation, use `db.iter_connected_block()`.
    ///
    pub fn get_connected_transaction<T: TxConnectable>(&self, txid: &Txid) -> OpResult<T> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let tx = self.get_transaction(txid)?;
        T::connect(tx, &self.tx_db, &self.block_index, &self.blk_file)
    }

    ///
    /// Iterate through all blocks for a given heights (excluded).
    ///
    /// Format: `full (FConnectedBlock)` / `simple (SConnectedBlock)`.
    ///
    /// This iterator use `unspent output` to track down the connected
    /// outputs of each outpoints.
    ///
    /// ## Note
    /// This does NOT require `txindex=true`.
    ///
    /// # Performance
    ///
    /// ## Using default feature:
    /// Requires 4 GB memory, finishes in 2.5 hours from 0-70000 block.
    ///
    /// ## Using non-default feature
    /// Requires 32 GB memory, finished in 30 minutes from 0-70000 block.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bitcoin_explorer::{BitcoinDB, FConnectedBlock, SConnectedBlock};
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin");
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// // iterate over block from 0 to 700000, (simple format)
    /// for block in db.iter_connected_block::<SConnectedBlock>(700000) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    /// ```
    ///
    pub fn iter_connected_block<TBlock>(&self, end: usize) -> ConnectedBlockIter<TBlock>
    where
        TBlock: 'static + BlockConnectable + Send,
    {
        ConnectedBlockIter::new(self, end)
    }
}
