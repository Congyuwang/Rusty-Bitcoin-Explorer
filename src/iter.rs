use crate::api::BitcoinDB;
use crate::bitcoinparser::errors::{OpError, OpResult};
use crate::bitcoinparser::proto::connected_proto::{
    FConnectedBlock, FConnectedTransaction, SConnectedBlock, SConnectedTransaction,
};
use crate::bitcoinparser::proto::full_proto::{FBlock, FTxOut};
use crate::bitcoinparser::proto::simple_proto::{SBlock, STxOut};
use bitcoin::{Txid, Block};
use log::warn;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use crate::bitcoinparser::script::Type;
use crate::bitcoinparser::blk_file::BlkFile;
use crate::bitcoinparser::block_index::BlockIndex;

/// python iterator implementation does not allow lifetime.
/// Thus, we must own the necessary resource for the iterator
/// to work for python.
pub struct DBCopy {
    pub block_index: BlockIndex,
    pub blk_file: BlkFile,
}

impl DBCopy {

    fn from_bitcoin_db(db: &BitcoinDB) -> DBCopy {
        DBCopy {
            block_index: db.block_index.clone(),
            blk_file: db.blk_file.clone()
        }
    }

    fn get_block(&self, height: i32) -> OpResult<Block> {
        if let Some(index) = self.block_index.records.get(height as usize) {
            let blk = self.blk_file.read_block(index.n_file, index.n_data_pos)?;
            Ok(blk)
        } else {
            Err(OpError::from("height not found"))
        }
    }

    fn get_block_full(&self, height: i32) -> OpResult<FBlock> {
        let blk = self.get_block(height)?;
        let blk_parsed = FBlock::parse(blk);
        Ok(blk_parsed)
    }

    fn get_block_simple(&self, height: i32) -> OpResult<SBlock> {
        let blk = self.get_block(height)?;
        let blk_parsed = SBlock::parse(blk);
        Ok(blk_parsed)
    }
}

pub struct FBlockIteratorSequential {
    end: u32,
    db: DBCopy,
    has_next: bool,
    current: u32,
}

impl FBlockIteratorSequential {
    pub fn new(db: &BitcoinDB, start: u32, end: u32) -> OpResult<FBlockIteratorSequential> {
        if end <= start {
            Err(OpError::from("invalid iterator range"))
        } else {
            Ok(FBlockIteratorSequential {
                end,
                db: DBCopy::from_bitcoin_db(db),
                has_next: true,
                current: start,
            })
        }
    }
}

impl Iterator for FBlockIteratorSequential {
    type Item = FBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next || self.current >= self.end {
            None
        } else {
            let obj = self.db.get_block_full(self.current as i32);
            self.current += 1;
            // fail fast
            match obj {
                Ok(obj) => Some(obj),
                Err(e) => {
                    warn!("Error {} in iterator", e);
                    self.has_next = false;
                    None
                }
            }
        }
    }
}

pub struct SBlockIteratorSequential {
    end: u32,
    db: DBCopy,
    has_next: bool,
    current: u32,
}

impl SBlockIteratorSequential {
    pub fn new(db: &BitcoinDB, start: u32, end: u32) -> OpResult<SBlockIteratorSequential> {
        if end <= start {
            Err(OpError::from("invalid iterator range"))
        } else {
            Ok(SBlockIteratorSequential {
                end,
                db: DBCopy::from_bitcoin_db(db),
                has_next: true,
                current: start,
            })
        }
    }
}

impl Iterator for SBlockIteratorSequential {
    type Item = SBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next || self.current >= self.end {
            None
        } else {
            let obj = self.db.get_block_simple(self.current as i32);
            self.current += 1;
            // fail fast
            match obj {
                Ok(obj) => Some(obj),
                Err(e) => {
                    warn!("Error {} in iterator", e);
                    self.has_next = false;
                    None
                }
            }
        }
    }
}

pub struct FBlockIteratorArray {
    heights: Vec<u32>,
    db: DBCopy,
    has_next: bool,
    current: u32,
}

impl FBlockIteratorArray {
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> FBlockIteratorArray {
        FBlockIteratorArray {
            heights,
            db: DBCopy::from_bitcoin_db(db),
            has_next: true,
            current: 0,
        }
    }
}

impl Iterator for FBlockIteratorArray {
    type Item = FBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next || self.current >= self.heights.len() as u32 {
            None
        } else {
            let height = *self.heights.get(self.current as usize).unwrap();
            let obj = self.db.get_block_full(height as i32);
            self.current += 1;
            // fail fast
            match obj {
                Ok(obj) => Some(obj),
                Err(e) => {
                    warn!("Error {} in iterator", e);
                    self.has_next = false;
                    None
                }
            }
        }
    }
}

pub struct SBlockIteratorArray {
    heights: Vec<u32>,
    db: DBCopy,
    has_next: bool,
    current: u32,
}

impl SBlockIteratorArray {
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> SBlockIteratorArray {
        SBlockIteratorArray {
            heights,
            db: DBCopy::from_bitcoin_db(db),
            has_next: true,
            current: 0,
        }
    }
}

impl Iterator for SBlockIteratorArray {
    type Item = SBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next || self.current >= self.heights.len() as u32 {
            None
        } else {
            let height = *self.heights.get(self.current as usize).unwrap();
            let obj = self.db.get_block_simple(height as i32);
            self.current += 1;
            // fail fast
            match obj {
                Ok(obj) => Some(obj),
                Err(e) => {
                    warn!("Error {} in iterator", e);
                    self.has_next = false;
                    None
                }
            }
        }
    }
}

/// these can only iterator sequentially from the genesis block
pub struct FConnectedBlockIterator {
    inner_iter: FBlockIteratorSequential,
    unspent: HashMap<Txid, RefCell<BTreeMap<u16, FTxOut>>>,
    has_next: bool,
}

impl FConnectedBlockIterator {
    pub fn new(db: &BitcoinDB, end: u32) -> OpResult<FConnectedBlockIterator> {
        Ok(FConnectedBlockIterator {
            inner_iter: FBlockIteratorSequential::new(db, 0, end)?,
            unspent: HashMap::new(),
            has_next: true,
        })
    }

    fn try_connect(&mut self, block: FBlock) -> OpResult<FConnectedBlock> {
        let mut output_block = FConnectedBlock {
            header: block.header,
            txdata: Vec::new(),
        };

        for tx in block.txdata {
            let tx_id = tx.txid;
            let output_copy = tx.output.clone();
            let mut output_tx = FConnectedTransaction {
                lock_time: tx.lock_time,
                txid: tx_id.clone(),
                input: Vec::new(),
                output: output_copy,
            };

            // the new transaction should not be in unspent
            if self.unspent.contains_key(&tx_id) {
                warn!("found duplicate key {}", &tx_id);
            }

            // insert new transaction
            let mut new_unspent: BTreeMap<u16, FTxOut> = BTreeMap::new();
            let mut n: u16 = 0;
            for output in tx.output {
                new_unspent.insert(n, output);
                n += 1
            }
            self.unspent
                .insert(tx_id.clone(), RefCell::new(new_unspent));

            // spend new inputs
            for input in tx.input {
                // skip coinbase transaction
                if input.previous_output.is_null() {
                    output_tx.input.push(FTxOut {
                        value: u64::MAX,
                        script_pubkey: Default::default(),
                        script_type: Type::NotRecognised,
                        addresses: vec![]
                    });
                    continue;
                }

                let prev_txid = &input.previous_output.txid;
                let n = *&input.previous_output.vout as u16;

                let prev_tx = self.unspent.get(prev_txid);
                if let Some(prev_tx) = prev_tx {
                    let mut unspent_output = prev_tx.borrow_mut();
                    let unspent: Option<FTxOut> = unspent_output.remove(&n);
                    if let Some(unspent) = unspent {
                        output_tx.input.push(unspent);
                    } else {
                        return Err(OpError::from("cannot find previous outpoint, bad data"));
                    }
                } else {
                    return Err(OpError::from("cannot find previous transactions, bad data"));
                }
            }
            output_block.txdata.push(output_tx);
        }
        // clean up after processing a block
        let mut to_remove: Vec<Txid> = Vec::new();
        for (txid, unspent) in self.unspent.iter() {
            if unspent.borrow().len() == 0 {
                to_remove.push(txid.clone())
            }
        }
        for txid in to_remove {
            self.unspent.remove(&txid);
        }
        Ok(output_block)
    }
}

impl Iterator for FConnectedBlockIterator {
    type Item = FConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next {
            return None;
        }
        let next_block: Option<FBlock> = self.inner_iter.next();
        match next_block {
            None => None,
            Some(block) => match self.try_connect(block) {
                Ok(block) => Some(block),
                Err(e) => {
                    warn!("{} in iterator", e);
                    self.has_next = false;
                    None
                }
            },
        }
    }
}

/// these can only iterator sequentially from the genesis block
pub struct SConnectedBlockIterator {
    inner_iter: SBlockIteratorSequential,
    unspent: HashMap<Txid, RefCell<BTreeMap<u16, STxOut>>>,
    has_next: bool,
}

impl SConnectedBlockIterator {
    pub fn new(db: &BitcoinDB, end: u32) -> OpResult<SConnectedBlockIterator> {
        Ok(SConnectedBlockIterator {
            inner_iter: SBlockIteratorSequential::new(db, 0, end)?,
            unspent: HashMap::new(),
            has_next: true,
        })
    }

    fn try_connect(&mut self, block: SBlock) -> OpResult<SConnectedBlock> {
        let mut output_block = SConnectedBlock {
            header: block.header,
            txdata: Vec::new(),
        };

        for tx in block.txdata {
            let tx_id = tx.txid;
            let mut output_copy: Vec<STxOut> = Vec::with_capacity(tx.output.len());
            for k in tx.output.iter() {
                output_copy.push(k.clone())
            }
            let mut output_tx = SConnectedTransaction {
                lock_time: tx.lock_time,
                txid: tx_id.clone(),
                input: Vec::new(),
                output: output_copy,
            };

            // the new transaction should not be in unspent
            if self.unspent.contains_key(&tx_id) {
                warn!("found duplicate key {}", &tx_id);
            }

            // insert new transaction
            let mut new_unspent: BTreeMap<u16, STxOut> = BTreeMap::new();
            let mut n: u16 = 0;
            for output in tx.output {
                new_unspent.insert(n, output);
                n += 1
            }
            self.unspent
                .insert(tx_id.clone(), RefCell::new(new_unspent));

            // spend new inputs
            for input in tx.input {
                // skip coinbase transaction
                if input.vout == u32::MAX {
                    output_tx.input.push(STxOut {
                        value: u64::MAX,
                        script_type: Type::NotRecognised,
                        addresses: vec![]
                    });
                    continue;
                }

                let prev_txid = &input.txid;
                let n = *&input.vout as u16;

                let prev_tx = self.unspent.get(prev_txid);
                if let Some(prev_tx) = prev_tx {
                    let mut unspent_output = prev_tx.borrow_mut();
                    let unspent: Option<STxOut> = unspent_output.remove(&n);
                    if let Some(unspent) = unspent {
                        output_tx.input.push(unspent);
                    } else {
                        return Err(OpError::from("cannot find previous outpoint, bad data"));
                    }
                } else {
                    return Err(OpError::from("cannot find previous transactions, bad data"));
                }
            }
            output_block.txdata.push(output_tx);
        }
        // clean up after processing a block
        let mut to_remove: Vec<Txid> = Vec::new();
        for (txid, unspent) in self.unspent.iter() {
            if unspent.borrow().len() == 0 {
                to_remove.push(txid.clone())
            }
        }
        for txid in to_remove {
            self.unspent.remove(&txid);
        }
        Ok(output_block)
    }
}

impl Iterator for SConnectedBlockIterator {
    type Item = SConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next {
            return None;
        }
        let next_block: Option<SBlock> = self.inner_iter.next();
        match next_block {
            None => None,
            Some(block) => match self.try_connect(block) {
                Ok(block) => Some(block),
                Err(e) => {
                    warn!("Error {} in iterator", e);
                    self.has_next = false;
                    None
                }
            },
        }
    }
}
