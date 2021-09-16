use std::path::Path;

use bitcoin::{Network, Script};

use crate::bitcoinparser::blk_store::BlkFile;
use crate::bitcoinparser::block_index::{BlockIndex, BlockIndexRecord};
use crate::bitcoinparser::errors::{OpError, OpResult};
use crate::bitcoinparser::parsed_proto::{Block, Transaction};
use crate::bitcoinparser::script::{evaluate_script, ScriptInfo};
use crate::bitcoinparser::tx_index::{TransactionIndex, TransactionRecord};
use bitcoin::hashes::hex::FromHex;

#[inline]
pub fn parse_script(script_pub_key: &str) -> OpResult<ScriptInfo> {
    let script = Script::from_hex(&script_pub_key)?;
    Ok(evaluate_script(&script, Network::Bitcoin))
}

pub struct BitcoinDB {
    pub block_index: BlockIndex,
    pub blk_store: BlkFile,
}

impl BitcoinDB {
    /// pass the `-datadir` directory of Bitcoin core
    pub fn new(p: &Path) -> OpResult<BitcoinDB> {
        let blk_path = p.join("blocks");
        let index_path = blk_path.join("index");
        Ok(BitcoinDB {
            block_index: BlockIndex::new(index_path.as_path())?,
            blk_store: BlkFile::new(blk_path.as_path())?,
        })
    }

    pub fn get_block_of_height(&self, height: i32) -> OpResult<Block> {
        if let Some(index) = self.block_index.records.get(height as usize) {
            let blk = self.blk_store.read_block(index.n_file, index.n_data_pos)?;
            let blk_parsed = Block::parse(blk);
            Ok(blk_parsed)
        } else {
            Err(OpError::from("height not found".to_string()))
        }
    }

    pub fn get_block_header(&self, height: usize) -> OpResult<&BlockIndexRecord> {
        if let Some(header) = self.block_index.records.get(height) {
            Ok(header)
        } else {
            Err(OpError::from("height not found".to_string()))
        }
    }
}

pub struct TxDB {
    pub transaction_index: TransactionIndex,
}

impl TxDB {
    /// pass the `-datadir` directory of Bitcoin core
    pub fn new(p: &Path, blk_index: &BlockIndex) -> TxDB {
        let path = p.join("indexes").join("txindex");
        TxDB {
            transaction_index: TransactionIndex::new(&path, blk_index),
        }
    }

    pub fn query_block_height_of_transaction(&mut self, txid: &str) -> OpResult<i32> {
        let record: TransactionRecord = self.transaction_index.query_tx_record(txid)?;
        let file_pos_height = &self.transaction_index.file_pos_to_height;
        match file_pos_height.get(&record.n_file) {
            None => Err(OpError::from("transaction not found".to_string())),
            Some(pos_height) => match pos_height.borrow().get(&record.n_pos) {
                None => Err(OpError::from("transaction not found".to_string())),
                Some(height) => Ok(*height),
            },
        }
    }

    pub fn query_transaction(&mut self, txid: &str, blk_store: &BlkFile) -> OpResult<Transaction> {
        if let Ok(record) = self.transaction_index.query_tx_record(txid) {
            let tx = blk_store.read_transaction(record.n_file, record.n_pos, record.n_tx_offset)?;
            let tx_parsed = Transaction::parse(tx);
            Ok(tx_parsed)
        } else {
            Err(OpError::from("txid not found".to_string()))
        }
    }
}
