use crate::bitcoinparser::blk_file::BlkFile;
use crate::bitcoinparser::proto::full_proto::{FBlockHeader, FTxOut};
use crate::bitcoinparser::proto::simple_proto::{SBlockHeader, STxOut};
use crate::bitcoinparser::tx_index::TransactionRecord;
use crate::bitcoinparser::tx_index::TxDB;
use bitcoin::{Block, Transaction, TxIn, TxOut, Txid};
use log::warn;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Serialize, Deserialize)]
pub struct SConnectedBlock {
    pub header: SBlockHeader,
    pub txdata: Vec<SConnectedTransaction>,
}

impl SConnectedBlock {
    ///
    /// Replace inputs by previous outputs.
    /// add addresses, block_hash, tx_id to the bitcoin library format,
    /// and also simplify the format.
    ///
    pub fn connect(block: Block, tx_db: &TxDB, blk_file: &BlkFile) -> SConnectedBlock {
        let block_hash = *&block.block_hash();
        SConnectedBlock {
            header: SBlockHeader::parse(block.header, block_hash),
            txdata: connect_output_simple(block.txdata, tx_db, blk_file),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SConnectedTransaction {
    pub lock_time: u32,
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<STxOut>,
    /// List of outputs
    pub output: Vec<STxOut>,
}

impl SConnectedTransaction {
    pub fn connect(tx: Transaction, tx_db: &TxDB, blk_file: &BlkFile) -> SConnectedTransaction {
        SConnectedTransaction {
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: connect_output_tx_in(tx.input, tx_db, blk_file)
                .into_iter()
                .map(STxOut::parse)
                .collect(),
            output: tx.output.into_iter().map(STxOut::parse).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct FConnectedBlock {
    pub header: FBlockHeader,
    pub txdata: Vec<FConnectedTransaction>,
}

impl FConnectedBlock {
    ///
    /// Replace inputs by previous outputs.
    /// add addresses, block_hash, tx_id to the bitcoin library format.
    ///
    pub fn connect(block: Block, tx_db: &TxDB, blk_file: &BlkFile) -> FConnectedBlock {
        let block_hash = *&block.block_hash();
        FConnectedBlock {
            header: FBlockHeader::parse(block.header, block_hash),
            txdata: connect_output_full(block.txdata, tx_db, blk_file),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct FConnectedTransaction {
    pub lock_time: u32,
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<FTxOut>,
    /// List of outputs
    pub output: Vec<FTxOut>,
}

impl FConnectedTransaction {
    pub fn connect(tx: Transaction, tx_db: &TxDB, blk_file: &BlkFile) -> FConnectedTransaction {
        FConnectedTransaction {
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: connect_output_tx_in(tx.input, tx_db, blk_file)
                .into_iter()
                .map(FTxOut::parse)
                .collect(),
            output: tx.output.into_iter().map(FTxOut::parse).collect(),
        }
    }
}

// private data struct for reading transactions
struct TxConnect<'a> {
    record: Option<TransactionRecord>,
    tx_in: &'a TxIn,
}

///
/// simplified version, include less data, faster for python
///
pub fn connect_output_simple(
    transactions: Vec<Transaction>,
    tx_db: &TxDB,
    blk_file: &BlkFile,
) -> Vec<SConnectedTransaction> {
    // query tx_index
    let all_records = query_tx_in_tx_index(&transactions, tx_db);

    // read from blk all transaction outputs
    let mut connected_outputs: VecDeque<Option<TxOut>> = par_input_to_output(all_records, blk_file);

    // reconstruct block
    let mut connected_tx = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let mut outputs = Vec::with_capacity(*&tx.input.len());
        let txid = tx.txid();
        for _ in tx.input {
            let connected_out = connected_outputs.pop_front().unwrap();
            if let Some(out) = connected_out {
                // do not push None, None is warned in log.warn
                outputs.push(out);
            }
        }
        connected_tx.push(SConnectedTransaction {
            lock_time: tx.lock_time,
            txid,
            input: outputs.into_iter().map(STxOut::parse).collect(),
            output: tx.output.into_iter().map(STxOut::parse).collect(),
        })
    }
    connected_tx
}

///
/// full version, include more data, slower for python
///
pub fn connect_output_full(
    transactions: Vec<Transaction>,
    tx_db: &TxDB,
    blk_file: &BlkFile,
) -> Vec<FConnectedTransaction> {
    // query tx_index
    let all_records = query_tx_in_tx_index(&transactions, tx_db);

    // read from blk all transaction outputs
    let mut connected_outputs: VecDeque<Option<TxOut>> = par_input_to_output(all_records, blk_file);

    // reconstruct block
    let mut connected_tx = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let mut outputs = Vec::with_capacity(*&tx.input.len());
        let txid = tx.txid();
        for _ in tx.input {
            let connected_out = connected_outputs.pop_front().unwrap();
            if let Some(out) = connected_out {
                // do not push None, None is warned in log.warn
                outputs.push(out);
            }
        }
        connected_tx.push(FConnectedTransaction {
            lock_time: tx.lock_time,
            txid,
            input: outputs.into_iter().map(FTxOut::parse).collect(),
            output: tx.output.into_iter().map(FTxOut::parse).collect(),
        })
    }
    connected_tx
}

pub fn connect_output_tx_in(tx_in: Vec<TxIn>, tx_db: &TxDB, blk_file: &BlkFile) -> Vec<TxOut> {
    // query tx_index
    let all_records = query_outpoints_in_tx_index(&tx_in, tx_db);
    // read from blk all outputs
    let connected_outputs: VecDeque<Option<TxOut>> = par_input_to_output(all_records, blk_file);
    connected_outputs.into_iter().filter_map(|x| x).collect()
}

///
/// Query all outpoints of a block from tx_index, and store in a single vector
///
fn query_tx_in_tx_index<'a>(
    transactions: &'a Vec<Transaction>,
    tx_db: &TxDB,
) -> Vec<TxConnect<'a>> {
    let mut all_records = Vec::new();
    // query `tx_index`
    for tx in transactions {
        for tx_in in &tx.input {
            let record = tx_db.get_tx_record(&tx_in.previous_output.txid).ok();
            all_records.push(TxConnect { record, tx_in });
        }
    }
    all_records
}

fn query_outpoints_in_tx_index<'a>(tx_in: &'a Vec<TxIn>, tx_db: &TxDB) -> Vec<TxConnect<'a>> {
    let mut all_records = Vec::new();
    for input in tx_in {
        let record = tx_db.get_tx_record(&input.previous_output.txid).ok();
        all_records.push(TxConnect {
            record,
            tx_in: input,
        });
    }
    all_records
}

///
/// Read from `blk` files of all transactions corresponding to outpoints in a block.
/// This is executed in parallel.
///
fn par_input_to_output(all_records: Vec<TxConnect>, blk_file: &BlkFile) -> VecDeque<Option<TxOut>> {
    all_records
        .into_par_iter()
        .map(|tx_connect: TxConnect| {
            // read disk for transactions in parallel
            let record = tx_connect.record;
            let tx_in = tx_connect.tx_in;
            if !is_coin_base(tx_in) {
                match record {
                    Some(record) => {
                        let tx_prev =
                            blk_file.read_transaction(record.n_file, record.n_pos, record.n_tx_offset);
                        match tx_prev {
                            Ok(t) => {
                                let mut output = t.output;
                                let index = tx_in.previous_output.vout as usize;
                                let len = output.len();
                                if index >= len {
                                    warn!("Outpoint {} vout out of range", tx_in.previous_output);
                                    None
                                } else {
                                    Some(output.swap_remove(index))
                                }
                            }
                            Err(_) => {
                                warn!("Outpoint {} not found in blk_file", tx_in.previous_output);
                                None
                            }
                        }
                    }
                    None => {
                        warn!("Outpoint {} not found in tx_index", tx_in.previous_output);
                        None
                    }
                }
            } else {
                // use default to represent coinbase output
                Some(TxOut::default())
            }
        })
        .collect()
}

fn is_coin_base(tx_in: &TxIn) -> bool {
    tx_in.previous_output.is_null()
}
