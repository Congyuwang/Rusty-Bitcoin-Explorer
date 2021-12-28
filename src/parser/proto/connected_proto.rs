use crate::parser::blk_file::BlkFile;
use crate::parser::errors::{OpError, OpResult};
use crate::parser::proto::full_proto::{FBlockHeader, FTxOut};
use crate::parser::proto::simple_proto::{SBlockHeader, STxOut};
use crate::parser::tx_index::TxDB;
use crate::BlockIndex;
use bitcoin::{Block, BlockHash, BlockHeader, Transaction, TxIn, TxOut, Txid};
use log::warn;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

///
/// This type refer to `Block` structs where inputs are
/// replaced by connected outputs.
///
/// ## Implementors:
/// - SConnectedBlock
/// - FConnectedBlock
///
pub trait ConnectedBlock {
    ///
    /// Associated output type.
    ///
    type Tx: ConnectedTx + Send;

    ///
    /// Construct a ConnectedBlock from parts of a block.
    ///
    /// Used in `iter_connected.rs`.
    ///
    fn from(block_header: BlockHeader, block_hash: BlockHash) -> Self;

    ///
    /// Add a new transaction in this block.
    ///
    /// Used in `iter_connected.rs`.
    ///
    fn add_tx(&mut self, tx: Self::Tx);

    ///
    /// Construct a ConnectedBlock and connect the transactions.
    ///
    fn connect(
        block: Block,
        tx_db: &TxDB,
        blk_index: &BlockIndex,
        blk_file: &BlkFile,
    ) -> OpResult<Self>
    where
        Self: Sized;
}

///
/// This type refer to `Transaction` structs where inputs are
/// replaced by connected outputs.
///
/// ## Implementors:
/// - STransaction
/// - FTransaction
///
pub trait ConnectedTx {
    ///
    /// Associated output type.
    ///
    type TOut: 'static + From<TxOut> + Send;

    ///
    /// Construct a ConnectedTx from Transaction without blank inputs.
    ///
    /// This function is used in `iter_connected.rs`.
    ///
    fn from(tx: &Transaction) -> Self;

    ///
    /// Add a input to this ConnectedTx.
    ///
    /// This function is used in `iter_connected.rs`.
    ///
    fn add_input(&mut self, input: Self::TOut);

    ///
    /// Build ConnectedTx from Tx,
    /// and attach inputs to this ConnectedTx using tx-index.
    ///
    fn connect(
        tx: Transaction,
        tx_db: &TxDB,
        blk_index: &BlockIndex,
        blk_file: &BlkFile,
    ) -> OpResult<Self>
    where
        Self: Sized;
}

///
/// Simple format of connected block.
/// See fields for details of this struct.
///
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct SConnectedBlock {
    pub header: SBlockHeader,
    pub txdata: Vec<SConnectedTransaction>,
}

///
/// Full format of connected block.
/// See fields for details of this struct.
///
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct FConnectedBlock {
    pub header: FBlockHeader,
    pub txdata: Vec<FConnectedTransaction>,
}

///
/// Simple format of connected transaction.
/// See fields for details of this struct.
///
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct SConnectedTransaction {
    pub txid: Txid,
    pub input: Vec<STxOut>,
    pub output: Vec<STxOut>,
}

///
/// Full format of connected transaction.
/// See fields for details of this struct.
///
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct FConnectedTransaction {
    pub version: i32,
    pub lock_time: u32,
    pub txid: Txid,
    pub input: Vec<FTxOut>,
    pub output: Vec<FTxOut>,
}

impl ConnectedTx for FConnectedTransaction {
    type TOut = FTxOut;

    fn from(tx: &Transaction) -> Self {
        FConnectedTransaction {
            version: tx.version,
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: Vec::new(),
            output: tx.output.clone().into_iter().map(|x| x.into()).collect(),
        }
    }

    fn add_input(&mut self, input: Self::TOut) {
        self.input.push(input);
    }

    fn connect(
        tx: Transaction,
        tx_db: &TxDB,
        blk_index: &BlockIndex,
        blk_file: &BlkFile,
    ) -> OpResult<Self> {
        let is_coinbase = tx.is_coin_base();
        Ok(FConnectedTransaction {
            version: tx.version,
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: connect_tx_inputs(&tx.input, is_coinbase, tx_db, blk_index, blk_file)?
                .into_iter()
                .map(|x| x.into())
                .collect(),
            output: tx.output.into_iter().map(|x| x.into()).collect(),
        })
    }
}

impl ConnectedTx for SConnectedTransaction {
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

    fn connect(
        tx: Transaction,
        tx_db: &TxDB,
        blk_index: &BlockIndex,
        blk_file: &BlkFile,
    ) -> OpResult<Self> {
        let is_coinbase = tx.is_coin_base();
        Ok(SConnectedTransaction {
            txid: tx.txid(),
            input: connect_tx_inputs(&tx.input, is_coinbase, tx_db, blk_index, blk_file)?
                .into_iter()
                .map(|x| x.into())
                .collect(),
            output: tx.output.into_iter().map(|x| x.into()).collect(),
        })
    }
}

impl ConnectedBlock for FConnectedBlock {
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

    fn connect(
        block: Block,
        tx_db: &TxDB,
        blk_index: &BlockIndex,
        blk_file: &BlkFile,
    ) -> OpResult<Self> {
        let block_hash = block.header.block_hash();
        Ok(FConnectedBlock {
            header: FBlockHeader::parse(block.header, block_hash),
            txdata: connect_block_inputs(block.txdata, tx_db, blk_index, blk_file)?,
        })
    }
}

impl ConnectedBlock for SConnectedBlock {
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

    fn connect(
        block: Block,
        tx_db: &TxDB,
        blk_index: &BlockIndex,
        blk_file: &BlkFile,
    ) -> OpResult<Self> {
        let block_hash = block.header.block_hash();
        Ok(SConnectedBlock {
            header: SBlockHeader::parse(block.header, block_hash),
            txdata: connect_block_inputs(block.txdata, tx_db, blk_index, blk_file)?,
        })
    }
}

///
/// This function is used for connecting transaction inputs for a single block.
///
#[inline]
fn connect_block_inputs<Tx>(
    transactions: Vec<Transaction>,
    tx_db: &TxDB,
    blk_index: &BlockIndex,
    blk_file: &BlkFile,
) -> OpResult<Vec<Tx>>
where
    Tx: ConnectedTx,
{
    // collect all inputs
    let mut all_tx_in = Vec::with_capacity(transactions.len());
    for tx in &transactions {
        for tx_in in &tx.input {
            all_tx_in.push(tx_in);
        }
    }

    // connect transactions inputs in parallel
    let mut connected_outputs: VecDeque<Option<TxOut>> = all_tx_in
        .par_iter()
        .map(|x| connect_input(x, tx_db, blk_index, blk_file))
        .collect();

    // reconstruct block
    let mut connected_tx = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let outpoints_count = if tx.is_coin_base() { 0 } else { tx.input.len() };

        let mut outputs = Vec::with_capacity(outpoints_count);
        for _ in 0..tx.input.len() {
            let connected_out = connected_outputs.pop_front().unwrap();
            if let Some(out) = connected_out {
                // also do not push the null input connected to coinbase transaction
                outputs.push(out);
            }
        }
        // check if any output is missing
        if outputs.len() != outpoints_count {
            return Err(OpError::from(
                "some outpoints aren't found, tx_index is not fully synced",
            ));
        }
        let mut tx = Tx::from(&tx);
        for o in outputs {
            tx.add_input(o.into());
        }
        connected_tx.push(tx);
    }
    Ok(connected_tx)
}

///
/// This function converts multiple Inputs of a single transaction to Outputs in parallel.
///
#[inline]
fn connect_tx_inputs(
    tx_in: &[TxIn],
    is_coinbase: bool,
    tx_db: &TxDB,
    blk_index: &BlockIndex,
    blk_file: &BlkFile,
) -> OpResult<Vec<TxOut>> {
    let connected_outputs: Vec<TxOut> = tx_in
        .par_iter()
        .filter_map(|x| connect_input(x, tx_db, blk_index, blk_file))
        .collect();

    let outpoints_count = if is_coinbase { 0 } else { tx_in.len() };
    let received = connected_outputs.len();

    // some outpoints aren't found
    if received != outpoints_count {
        Err(OpError::from(
            format!("some outpoints aren't found, tx_index is not fully synced, (expected: {}, read: {}, txid)", outpoints_count, received).as_str(),
        ))
    } else {
        Ok(connected_outputs)
    }
}

///
/// This function connect a single TxIn to outputs. It converts:
/// - read failure to `None`
/// - coinbase transaction output to `None`
///
/// It is used in `connect_output_tx_in` and `connect_output`.
///
#[inline]
fn connect_input(
    tx_in: &TxIn,
    tx_db: &TxDB,
    blk_index: &BlockIndex,
    blk_file: &BlkFile,
) -> Option<TxOut> {
    let outpoint = tx_in.previous_output;
    let tx_id = &outpoint.txid;
    let n = outpoint.vout;
    // skip coinbase transaction
    if !is_coin_base(tx_in) {
        // special treatment of genesis tx, which cannot be found in tx-index.
        if tx_db.is_genesis_tx(tx_id) {
            return match blk_index.records.first() {
                None => None,
                Some(pos) => match blk_file.read_block(pos.n_file, pos.n_data_pos) {
                    Ok(mut blk) => {
                        let mut tx = blk.txdata.swap_remove(0);
                        Some(tx.output.swap_remove(0))
                    }
                    Err(_) => None,
                },
            };
        }
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
        // skip coinbase transaction
        None
    }
}

#[inline]
fn is_coin_base(tx_in: &TxIn) -> bool {
    tx_in.previous_output.is_null()
}
