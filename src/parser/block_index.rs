use crate::parser::errors::OpResult;
use crate::parser::reader::BlockchainRead;
use bitcoin::hashes::hex::ToHex;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, BlockHeader};
use leveldb::database::iterator::LevelDBIterator;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::options::{Options, ReadOptions};
use log::info;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::path::Path;

const BLOCK_VALID_HEADER: u32 = 1;
const BLOCK_VALID_TREE: u32 = 2;
const BLOCK_VALID_TRANSACTIONS: u32 = 3;
const BLOCK_VALID_CHAIN: u32 = 4;
const BLOCK_VALID_SCRIPTS: u32 = 5;
const BLOCK_VALID_MASK: u32 = BLOCK_VALID_HEADER
    | BLOCK_VALID_TREE
    | BLOCK_VALID_TRANSACTIONS
    | BLOCK_VALID_CHAIN
    | BLOCK_VALID_SCRIPTS;
const BLOCK_HAVE_DATA: u32 = 8;
const BLOCK_HAVE_UNDO: u32 = 16;

// BLOCK_INDEX RECORD

#[derive(Serialize, Clone)]
pub struct BlockIndexRecord {
    pub block_hash: BlockHash,
    pub n_version: i32,
    pub n_height: i32,
    pub n_status: u32,
    pub n_tx: u32,
    pub n_file: i32,
    pub n_data_pos: u32,
    pub n_undo_pos: u32,
    pub block_header: BlockHeader,
}

impl BlockIndexRecord {
    fn from(key: &[u8], values: &[u8]) -> OpResult<Self> {
        let mut reader = Cursor::new(values);

        let block_hash: BlockHash = BlockHash::from_slice(key)?;
        let n_version = reader.read_varint()? as i32;
        let n_height = reader.read_varint()? as i32;
        let n_status = reader.read_varint()? as u32;
        let n_tx = reader.read_varint()? as u32;
        let n_file = if n_status & (BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO) > 0 {
            reader.read_varint()? as i32
        } else {
            -1
        };
        let n_data_pos = if n_status & BLOCK_HAVE_DATA > 0 {
            reader.read_varint()? as u32
        } else {
            u32::MAX
        };
        let n_undo_pos = if n_status & BLOCK_HAVE_UNDO > 0 {
            reader.read_varint()? as u32
        } else {
            u32::MAX
        };
        let block_header = reader.read_block_header()?;

        Ok(BlockIndexRecord {
            block_hash,
            n_version,
            n_height,
            n_status,
            n_tx,
            n_file,
            n_data_pos,
            n_undo_pos,
            block_header,
        })
    }
}

impl fmt::Debug for BlockIndexRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockIndexRecord")
            .field("block_hash", &self.block_hash.to_hex())
            .field("version", &self.n_version)
            .field("height", &self.n_height)
            .field("status", &self.n_status)
            .field("n_tx", &self.n_tx)
            .field("n_file", &self.n_file)
            .field("n_data_pos", &self.n_data_pos)
            .field("header", &self.block_header)
            .finish()
    }
}

#[inline]
fn is_block_index_record(data: &[u8]) -> bool {
    *data.get(0).unwrap() == b'b'
}

#[derive(Clone)]
pub struct BlockIndex {
    pub records: Vec<BlockIndexRecord>,
    pub hash_to_height: BTreeMap<String, i32>,
}

impl BlockIndex {
    pub(crate) fn new(p: &Path) -> OpResult<BlockIndex> {
        let records = load_block_index(p)?;
        let mut hash_to_height = BTreeMap::new();
        for b in &records {
            let this_block_hash = BlockHash::from_slice(&b.block_hash)?;
            hash_to_height.insert(this_block_hash.to_hex(), b.n_height);
        }
        Ok(BlockIndex {
            records,
            hash_to_height,
        })
    }
}

struct BlockKey {
    key: Vec<u8>,
}

impl db_key::Key for BlockKey {
    fn from_u8(key: &[u8]) -> Self {
        BlockKey {
            key: Vec::from(key),
        }
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(&self.key)
    }
}

/// load all block index in memory from disk (i.e. `blocks/index` path)
pub fn load_block_index(path: &Path) -> OpResult<Vec<BlockIndexRecord>> {
    let mut block_index = Vec::with_capacity(800000);

    info!("Start loading block_index");
    let mut options = Options::new();
    options.create_if_missing = false;
    let db: Database<BlockKey> = Database::open(path, options)?;
    let options = ReadOptions::new();
    let mut iter = db.iter(options);

    while iter.advance() {
        let k = iter.key();
        let v = iter.value();
        if is_block_index_record(&k.key) {
            let record = BlockIndexRecord::from(&k.key[1..], &v)?;
            if record.n_status & (BLOCK_VALID_MASK | BLOCK_HAVE_DATA) > 0 {
                block_index.push(record);
            }
        }
    }
    block_index.sort_by_key(|b| b.n_height);
    info!("Longest chain: {}", &block_index.len());
    Ok(block_index)
}
