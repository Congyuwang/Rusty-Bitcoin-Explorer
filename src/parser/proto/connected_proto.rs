use crate::parser::blk_file::BlkFile;
use crate::parser::proto::full_proto::{FBlockHeader, FTxOut};
use crate::parser::proto::simple_proto::{SBlockHeader, STxOut};
use crate::parser::tx_index::TxDB;
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
        let block_hash = block.header.block_hash();
        SConnectedBlock {
            header: SBlockHeader::parse(block.header, block_hash),
            txdata: connect_output_simple(block.txdata, tx_db, blk_file),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SConnectedTransaction {
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<STxOut>,
    /// List of outputs
    pub output: Vec<STxOut>,
}

impl SConnectedTransaction {
    pub fn connect(tx: Transaction, tx_db: &TxDB, blk_file: &BlkFile) -> SConnectedTransaction {
        SConnectedTransaction {
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
        let block_hash = block.header.block_hash();
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

fn connect_output_tx_in(tx_in: Vec<TxIn>, tx_db: &TxDB, blk_file: &BlkFile) -> Vec<TxOut> {
    tx_in
        .par_iter()
        .filter_map(|x| outpoint_connect(x, tx_db, blk_file))
        .collect()
}

///
/// simplified version, include less data, faster for python
///
fn connect_output_simple(
    transactions: Vec<Transaction>,
    tx_db: &TxDB,
    blk_file: &BlkFile,
) -> Vec<SConnectedTransaction> {
    let all_tx_in = get_all_tx_in(&transactions);

    let mut connected_outputs: VecDeque<Option<TxOut>> = all_tx_in
        .par_iter()
        .map(|x| outpoint_connect(x, tx_db, blk_file))
        .collect();

    // reconstruct block
    let mut connected_tx = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let mut outputs = Vec::with_capacity(*&tx.input.len());
        let txid = tx.txid();
        for _ in tx.input {
            let connected_out = connected_outputs.pop_front().unwrap();
            // Do not push None, None is warned in log.warn
            // although None is caused by error.
            if let Some(out) = connected_out {
                // also do not push the null input connected to coinbase transaction
                if out.value != 0xffffffffffffffff {
                    outputs.push(out);
                }
            }
        }
        connected_tx.push(SConnectedTransaction {
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
fn connect_output_full(
    transactions: Vec<Transaction>,
    tx_db: &TxDB,
    blk_file: &BlkFile,
) -> Vec<FConnectedTransaction> {
    let all_tx_in = get_all_tx_in(&transactions);

    let mut connected_outputs: VecDeque<Option<TxOut>> = all_tx_in
        .par_iter()
        .map(|x| outpoint_connect(x, tx_db, blk_file))
        .collect();

    // reconstruct block
    let mut connected_tx = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let mut outputs = Vec::with_capacity(*&tx.input.len());
        let txid = tx.txid();
        for _ in tx.input {
            let connected_out = connected_outputs.pop_front().unwrap();
            // Do not push None, None is warned in log.warn
            // although None is caused by error.
            if let Some(out) = connected_out {
                // also do not push the null input connected to coinbase transaction
                if out.value != 0xffffffffffffffff {
                    outputs.push(out);
                }
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

fn outpoint_connect(tx_in: &TxIn, tx_db: &TxDB, blk_file: &BlkFile) -> Option<TxOut> {
    let outpoint = tx_in.previous_output;
    let tx_id = &outpoint.txid;
    let n = outpoint.vout;
    if !is_coin_base(&tx_in) {
        if let Ok(record) = tx_db.get_tx_record(tx_id) {
            if let Ok(mut tx) =
                blk_file.read_transaction(record.n_file, record.n_pos, record.n_tx_offset)
            {
                let len = tx.output.len();
                if n >= len as u32 {
                    warn!("outpoint {} exceeds range", &outpoint);
                    None
                } else {
                    Some(tx.output.swap_remove(n as usize))
                }
            } else {
                warn!("fail to read transaction {}", &outpoint);
                None
            }
        } else {
            warn!("cannot find outpoint {} in txDB", &outpoint);
            None
        }
    } else {
        // use default value to connect to coinbase transaction.
        Some(TxOut::default())
    }
}

fn get_all_tx_in(transactions: &Vec<Transaction>) -> Vec<&TxIn> {
    let mut all_tx_in = Vec::new();
    for tx in transactions {
        for tx_in in &tx.input {
            all_tx_in.push(tx_in);
        }
    }
    all_tx_in
}

fn is_coin_base(tx_in: &TxIn) -> bool {
    tx_in.previous_output.is_null()
}
