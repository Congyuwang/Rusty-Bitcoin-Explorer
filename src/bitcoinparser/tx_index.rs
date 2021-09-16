use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use log::{info, warn};
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use rusty_leveldb::{DB, Options};

use crate::bitcoinparser::block_index::BlockIndex;
use crate::bitcoinparser::errors::{OpError, OpResult};
use crate::bitcoinparser::reader::BlockchainRead;
use bitcoin::hashes::hex::FromHex;

pub struct TransactionIndex {
    db: Option<DB>,
    // used for reverse looking up block
    pub file_pos_to_height: BTreeMap<i32, RefCell<BTreeMap<u32, i32>>>,
}

impl TransactionIndex {
    pub fn new(path: &Path, blk_index: &BlockIndex) -> TransactionIndex {
        let mut file_pos_to_height = BTreeMap::new();
        for b in &blk_index.records {
            let height = b.n_height;
            if !file_pos_to_height.contains_key(&b.n_file) {
                file_pos_to_height.insert(b.n_file, RefCell::new(BTreeMap::new()));
            }
            let mut pos_to_height = file_pos_to_height.get(&b.n_file).unwrap().borrow_mut();
            pos_to_height.insert(b.n_data_pos, height);
        }
        TransactionIndex {
            db: TransactionIndex::try_open_db(path),
            file_pos_to_height,
        }
    }

    #[inline]
    fn try_open_db(path: &Path) -> Option<DB> {
        match DB::open(path, Options::default()) {
            Ok(db) => {
                info!{"Successfully open tx_index DB!"}
                Some(db)
            },
            Err(e) => {
                warn!("Filed to open tx_index DB: {:?}", e);
                None
            }
        }
    }

    pub fn query_tx_record(&mut self, txid: &str) -> OpResult<TransactionRecord> {
        let txid = Txid::from_hex(txid)?;
        if let Some(db) = self.db.as_mut() {
            let inner = txid.as_inner();
            let mut key = Vec::with_capacity(inner.len() + 1);
            key.push(b't');
            key.extend(inner);
            let key = key.as_slice();
            if let Some(value) = db.get(key) {
                Ok(TransactionRecord::from(&key[1..], value.as_slice())?)
            } else {
                Err(OpError::from("value not found".to_string()))
            }
        } else {
            Err(OpError::from("tx_index DB not found,\
            might need to reindex with tx_index=1".to_string()))
        }
    }
}

pub struct TransactionRecord {
    pub txid: Txid,
    pub n_file: i32,
    pub n_pos: u32,
    pub n_tx_offset: u32,
}

impl TransactionRecord {
    fn from(key: &[u8], values: &[u8]) -> OpResult<Self> {
        let mut reader = Cursor::new(values);
        Ok(TransactionRecord {
            txid: Txid::from_slice(key)?,
            n_file: reader.read_varint()? as i32,
            n_pos: reader.read_varint()? as u32,
            n_tx_offset: reader.read_varint()? as u32,
        })
    }
}
