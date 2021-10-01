//!
//! implementation of methods that retrieve block info with outpoints connected
//!
use crate::api::{BitcoinDB, BlockConnectable, ConnectedBlockIter, TxConnectable, Txid};
use crate::parser::errors::{OpError, OpResult};

impl BitcoinDB {
    ///
    /// Get a block with inputs replaced by connected outputs.
    ///
    /// This function requires `txindex` to be set to `true`,
    /// and `txindex=1` when running Bitcoin Core.
    ///
    /// # Caveat!!
    ///
    /// This is a *very slow* function!!
    /// For massive processing of `connected` blocks, use `iter`.
    ///
    pub fn get_connected_block<T: BlockConnectable>(&self, height: i32) -> OpResult<T> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let tx = self.get_block(height)?;
        Ok(T::connect(tx, &self.tx_db, &self.blk_file))
    }

    ///
    /// Get a transaction with outpoints replaced by outputs.
    ///
    /// Format: `full (FConnectedTransaction)` / `simple (SConnectedTransaction)`.
    ///
    /// # Caveats
    ///
    /// Slow! Not suitable for massive computation. Use `iter`.
    ///
    pub fn get_connected_transaction<T: TxConnectable>(&self, txid: &Txid) -> OpResult<T> {
        let tx = self.get_transaction(txid)?;
        let tx_parsed = T::connect(tx, &self.tx_db, &self.blk_file);
        Ok(tx_parsed)
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
    /// Iterating through height from 0 to 700000 takes 5 hours.
    /// The performance bottleneck is most likely diskIO.
    ///
    /// This iterator is implemented to read the blocks in concurrency,
    /// but each block connects its outpoints to outputs only after
    /// all previous blocks have finished inserting their outputs in
    /// `unspent cache`.
    /// The result is still produced in the sequential order.
    ///
    /// Because this iterator tracks unspent outputs,
    /// it can use up to 20GB to 30GB memory.
    ///
    /// This iterator can only start from genesis block, because it has to
    /// track unspent transactions.
    ///
    /// TODO: might use txindex to allow for iterating starting from larger heights.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bitcoin_explorer::{BitcoinDB, FConnectedBlock, SConnectedBlock};
    /// use std::path::Path;
    ///
    /// let path = Path::new("/Users/me/bitcoin").unwrap();
    ///
    /// // launch without reading txindex
    /// let db = BitcoinDB::new(path, false).unwrap();
    ///
    /// // iterate over block from 0 to 700000, (full format)
    /// for block in db.iter_connected_block::<FConnectedBlock>(700000) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    ///
    /// // iterate over block from 0 to 700000, (simple format)
    /// for block in db.iter_connected_block::<SConnectedBlock>(700000) {
    ///     for tx in block.txdata {
    ///         println!("do something for this transaction");
    ///     }
    /// }
    /// ```
    ///
    pub fn iter_connected_block<TBlock>(&self, end: u32) -> ConnectedBlockIter<TBlock>
    where
        TBlock: 'static + BlockConnectable + Send,
    {
        ConnectedBlockIter::new(self, end)
    }
}
