use super::par_iter::*;
use crate::parser::blk_file::BlkFile;
use crate::parser::block_index::{BlockIndex, BlockIndexRecord};
use crate::parser::errors::{OpError, OpResult};
use crate::parser::proto::connected_proto::{
    FConnectedBlock, FConnectedTransaction, SConnectedBlock, SConnectedTransaction,
};
use crate::parser::proto::full_proto::{FBlock, FTransaction};
use crate::parser::proto::simple_proto::{SBlock, STransaction};
use crate::parser::script::{evaluate_script, ScriptInfo};
use crate::parser::tx_index::TxDB;
use bitcoin::hashes::hex::FromHex;
use bitcoin::{Block, BlockHash, Network, Script, Transaction, Txid};
use std::path::Path;

#[inline]
pub fn parse_script(script_pub_key: &str) -> OpResult<ScriptInfo> {
    let script = Script::from_hex(&script_pub_key)?;
    Ok(evaluate_script(&script, Network::Bitcoin))
}

pub struct BitcoinDB {
    pub block_index: BlockIndex,
    pub blk_file: BlkFile,
    pub tx_db: TxDB,
}

impl BitcoinDB {
    /// pass the `-datadir` directory of Bitcoin core
    /// `tx_index`: whether to try to open tx_index levelDB.
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

    pub fn get_block_header(&self, height: usize) -> OpResult<&BlockIndexRecord> {
        if let Some(header) = self.block_index.records.get(height) {
            Ok(header)
        } else {
            Err(OpError::from("height not found"))
        }
    }

    pub fn get_hash_from_height(&self, height: usize) -> OpResult<BlockHash> {
        match self.block_index.records.get(height) {
            None => Err(OpError::from("height not found")),
            Some(s) => Ok(s.block_hash),
        }
    }

    pub fn get_height_from_hash(&self, hash: &str) -> OpResult<i32> {
        match self.block_index.hash_to_height.get(hash) {
            None => Err(OpError::from("hash not found")),
            Some(h) => Ok(*h),
        }
    }

    pub fn get_block(&self, height: i32) -> OpResult<Block> {
        if let Some(index) = self.block_index.records.get(height as usize) {
            let blk = self.blk_file.read_block(index.n_file, index.n_data_pos)?;
            Ok(blk)
        } else {
            Err(OpError::from("height not found"))
        }
    }

    pub fn get_block_full(&self, height: i32) -> OpResult<FBlock> {
        let blk = self.get_block(height)?;
        let blk_parsed = FBlock::parse(blk);
        Ok(blk_parsed)
    }

    pub fn get_block_simple(&self, height: i32) -> OpResult<SBlock> {
        let blk = self.get_block(height)?;
        let blk_parsed = SBlock::parse(blk);
        Ok(blk_parsed)
    }

    pub fn get_block_full_connected(&self, height: i32) -> OpResult<FConnectedBlock> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let blk = self.get_block(height)?;
        let blk_parsed = FConnectedBlock::connect(blk, &self.tx_db, &self.blk_file);
        Ok(blk_parsed)
    }

    pub fn get_block_simple_connected(&self, height: i32) -> OpResult<SConnectedBlock> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let blk = self.get_block(height)?;
        let blk_parsed = SConnectedBlock::connect(blk, &self.tx_db, &self.blk_file);
        Ok(blk_parsed)
    }

    pub fn get_block_height_of_transaction(&self, txid: &Txid) -> OpResult<i32> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        self.tx_db.get_block_height_of_tx(txid)
    }

    pub fn get_transaction(&self, txid: &Txid) -> OpResult<Transaction> {
        if !self.tx_db.is_open() {
            return Err(OpError::from("TxDB not open"));
        }
        let record = self.tx_db.get_tx_record(txid)?;
        let tx = self
            .blk_file
            .read_transaction(record.n_file, record.n_pos, record.n_tx_offset)?;
        Ok(tx)
    }

    pub fn get_transaction_full(&self, txid: &Txid) -> OpResult<FTransaction> {
        let tx = self.get_transaction(txid)?;
        let tx_parsed = FTransaction::parse(tx);
        Ok(tx_parsed)
    }

    pub fn get_transaction_simple(&self, txid: &Txid) -> OpResult<STransaction> {
        let tx = self.get_transaction(txid)?;
        let tx_parsed = STransaction::parse(tx);
        Ok(tx_parsed)
    }

    pub fn get_transaction_full_connected(&self, txid: &Txid) -> OpResult<FConnectedTransaction> {
        let tx = self.get_transaction(txid)?;
        let tx_parsed = FConnectedTransaction::connect(tx, &self.tx_db, &self.blk_file);
        Ok(tx_parsed)
    }

    pub fn get_transaction_simple_connected(&self, txid: &Txid) -> OpResult<SConnectedTransaction> {
        let tx = self.get_transaction(txid)?;
        let tx_parsed = SConnectedTransaction::connect(tx, &self.tx_db, &self.blk_file);
        Ok(tx_parsed)
    }

    pub fn get_block_full_iter_seq(
        &self,
        start: u32,
        end: u32,
    ) -> OpResult<FBlockIteratorSequential> {
        FBlockIteratorSequential::new(self, start, end)
    }

    pub fn get_block_simple_iter_seq(
        &self,
        start: u32,
        end: u32,
    ) -> OpResult<SBlockIteratorSequential> {
        SBlockIteratorSequential::new(self, start, end)
    }

    pub fn get_block_full_iter_arr(&self, heights: Vec<u32>) -> FBlockIteratorArray {
        FBlockIteratorArray::new(self, heights)
    }

    pub fn get_block_simple_iter_arr(&self, heights: Vec<u32>) -> SBlockIteratorArray {
        SBlockIteratorArray::new(self, heights)
    }

    pub fn get_block_full_connected_iter(&self, end: u32) -> FConnectedBlockIterator {
        FConnectedBlockIterator::new(self, end)
    }

    pub fn get_block_simple_connected_iter(&self, end: u32) -> SConnectedBlockIterator {
        SConnectedBlockIterator::new(self, end)
    }
}
