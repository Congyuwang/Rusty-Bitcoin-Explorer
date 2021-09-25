use crate::parser::block_index::BlockIndex;
use crate::parser::errors::{OpError, OpResult};
use crate::parser::reader::BlockchainRead;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::{Options, ReadOptions};
use log::{info, warn};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, RwLock};

struct TxKey {
    key: Vec<u8>,
}

impl db_key::Key for TxKey {
    fn from_u8(key: &[u8]) -> Self {
        TxKey {
            key: Vec::from(key),
        }
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(&self.key)
    }
}

pub struct TxDB {
    db: Option<Database<TxKey>>,
    // used for reverse looking up to block height
    pub file_pos_to_height: BTreeMap<i32, Arc<RwLock<BTreeMap<u32, i32>>>>,
}

impl TxDB {
    /// initialize TxDB for transaction queries
    pub fn new(path: &Path, blk_index: &BlockIndex) -> TxDB {
        let option_db = TxDB::try_open_db(path);
        if let Some(db) = option_db {
            let mut file_pos_to_height = BTreeMap::new();
            for b in &blk_index.records {
                let height = b.n_height;
                if !file_pos_to_height.contains_key(&b.n_file) {
                    file_pos_to_height.insert(b.n_file, Arc::new(RwLock::new(BTreeMap::new())));
                }
                let pos_to_height = file_pos_to_height.get(&b.n_file).unwrap().clone();
                let mut map = pos_to_height.write().unwrap();
                map.insert(b.n_data_pos, height);
            }
            TxDB {
                db: Some(db),
                file_pos_to_height,
            }
        } else {
            TxDB::null()
        }
    }

    pub fn is_open(&self) -> bool {
        self.db.is_some()
    }

    pub fn null() -> TxDB {
        TxDB {
            db: None,
            file_pos_to_height: BTreeMap::new(),
        }
    }

    fn try_open_db(path: &Path) -> Option<Database<TxKey>> {
        let options = Options::new();
        match Database::open(path, options) {
            Ok(db) => {
                info! {"Successfully opened tx_index DB!"}
                Some(db)
            }
            Err(e) => {
                warn!("Filed to open tx_index DB: {:?}", e);
                None
            }
        }
    }

    pub fn get_tx_record(&self, txid: &Txid) -> OpResult<TransactionRecord> {
        if let Some(db) = &self.db {
            let inner = txid.as_inner();
            let mut key = Vec::with_capacity(inner.len() + 1);
            key.push(b't');
            key.extend(inner);
            let key = TxKey { key };
            let read_options = ReadOptions::new();
            if let Ok(value) = db.get(read_options, &key) {
                if let Some(value) = value {
                    Ok(TransactionRecord::from(&key.key[1..], value.as_slice())?)
                } else {
                    Err(OpError::from("null value read"))
                }
            } else {
                Err(OpError::from("value not found"))
            }
        } else {
            Err(OpError::from("TxDB not open"))
        }
    }

    pub fn get_block_height_of_tx(&self, txid: &Txid) -> OpResult<i32> {
        let record: TransactionRecord = self.get_tx_record(txid)?;
        let file_pos_height = &self.file_pos_to_height;
        match file_pos_height.get(&record.n_file) {
            None => Err(OpError::from("transaction not found")),
            Some(pos_height) => {
                let pos_height = Arc::clone(pos_height);
                let pos_height = pos_height.read().unwrap();
                match pos_height.get(&record.n_pos) {
                    None => Err(OpError::from("transaction not found")),
                    Some(height) => Ok(*height),
                }
            }
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
