use crate::parser::blk_file::BlkFile;
use crate::parser::proto::full_proto::{FBlockHeader, FTxOut};
use crate::parser::proto::simple_proto::{SBlockHeader, STxOut};
use crate::parser::tx_index::TxDB;
use bitcoin::{Block, BlockHash, BlockHeader, Transaction, TxIn, TxOut, Txid};
use log::warn;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Serialize, Deserialize)]
pub struct SConnectedBlock {
    pub header: SBlockHeader,
    pub txdata: Vec<SConnectedTransaction>,
}

#[derive(Serialize, Deserialize)]
pub struct FConnectedBlock {
    pub header: FBlockHeader,
    pub txdata: Vec<FConnectedTransaction>,
}

#[derive(Serialize, Deserialize)]
pub struct SConnectedTransaction {
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<STxOut>,
    /// List of outputs
    pub output: Vec<STxOut>,
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

fn connect_output_tx_in(tx_in: Vec<TxIn>, tx_db: &TxDB, blk_file: &BlkFile) -> Vec<TxOut> {
    tx_in
        .par_iter()
        .filter_map(|x| outpoint_connect(x, tx_db, blk_file))
        .collect()
}

fn connect_output<Tx>(transactions: Vec<Transaction>, tx_db: &TxDB, blk_file: &BlkFile) -> Vec<Tx>
where
    Tx: TxConnectable,
{
    let all_tx_in = get_all_tx_in(&transactions);

    let mut connected_outputs: VecDeque<Option<TxOut>> = all_tx_in
        .par_iter()
        .map(|x| outpoint_connect(x, tx_db, blk_file))
        .collect();

    // reconstruct block
    let mut connected_tx = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let mut outputs = Vec::with_capacity(*&tx.input.len());
        for _ in 0..tx.input.len() {
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
        let mut tx = Tx::from(&tx);
        for o in outputs {
            tx.add_input(o.into());
        }
        connected_tx.push(tx);
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
    let mut all_tx_in = Vec::with_capacity(transactions.len());
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

pub trait TxConnectable {
    type TOut: 'static + From<TxOut> + Send;

    fn from(tx: &Transaction) -> Self;
    fn add_input(&mut self, input: Self::TOut);
    fn connect(tx: Transaction, tx_db: &TxDB, blk_file: &BlkFile) -> Self;
}

impl TxConnectable for FConnectedTransaction {
    type TOut = FTxOut;

    fn from(tx: &Transaction) -> Self {
        FConnectedTransaction {
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: Vec::new(),
            output: tx.output.clone().into_iter().map(|x| x.into()).collect(),
        }
    }

    fn add_input(&mut self, input: Self::TOut) {
        self.input.push(input);
    }

    fn connect(tx: Transaction, tx_db: &TxDB, blk_file: &BlkFile) -> Self {
        FConnectedTransaction {
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: connect_output_tx_in(tx.input, tx_db, blk_file)
                .into_iter()
                .map(|x| x.into())
                .collect(),
            output: tx.output.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TxConnectable for SConnectedTransaction {
    type TOut = STxOut;

    fn from(tx: &Transaction) -> Self {
        SConnectedTransaction {
            txid: tx.txid(),
            input: Vec::new(),
            output: tx.output.clone().into_iter().map(|x| x.into()).collect(),
        }
    }

    fn add_input(&mut self, input: Self::TOut) {
        self.input.push(input);
    }

    fn connect(tx: Transaction, tx_db: &TxDB, blk_file: &BlkFile) -> Self {
        SConnectedTransaction {
            txid: tx.txid(),
            input: connect_output_tx_in(tx.input, tx_db, blk_file)
                .into_iter()
                .map(|x| x.into())
                .collect(),
            output: tx.output.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub trait BlockConnectable {
    type Tx: TxConnectable + Send;

    fn from(block_header: BlockHeader, block_hash: BlockHash) -> Self;
    fn add_tx(&mut self, tx: Self::Tx);
    fn connect(block: Block, tx_db: &TxDB, blk_file: &BlkFile) -> Self;
}

impl BlockConnectable for FConnectedBlock {
    type Tx = FConnectedTransaction;

    fn from(block_header: BlockHeader, block_hash: BlockHash) -> Self {
        FConnectedBlock {
            header: FBlockHeader::parse(block_header, block_hash),
            txdata: Vec::new(),
        }
    }

    fn add_tx(&mut self, tx: Self::Tx) {
        self.txdata.push(tx);
    }

    fn connect(block: Block, tx_db: &TxDB, blk_file: &BlkFile) -> Self {
        let block_hash = block.header.block_hash();
        FConnectedBlock {
            header: FBlockHeader::parse(block.header, block_hash),
            txdata: connect_output(block.txdata, tx_db, blk_file),
        }
    }
}

impl BlockConnectable for SConnectedBlock {
    type Tx = SConnectedTransaction;

    fn from(block_header: BlockHeader, block_hash: BlockHash) -> Self {
        SConnectedBlock {
            header: SBlockHeader::parse(block_header, block_hash),
            txdata: Vec::new(),
        }
    }

    fn add_tx(&mut self, tx: Self::Tx) {
        self.txdata.push(tx);
    }

    fn connect(block: Block, tx_db: &TxDB, blk_file: &BlkFile) -> Self {
        let block_hash = block.header.block_hash();
        SConnectedBlock {
            header: SBlockHeader::parse(block.header, block_hash),
            txdata: connect_output(block.txdata, tx_db, blk_file),
        }
    }
}
