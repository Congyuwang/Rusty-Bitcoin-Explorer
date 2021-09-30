//!
//! Add addresses, block_hash, tx_id to the bitcoin library format
//!
use crate::api::Block;
use crate::parser::script::{evaluate_script, Type};
use bitcoin::{Address, BlockHash, Transaction, TxMerkleNode, TxOut, Txid};
use serde::{Deserialize, Serialize};

///
/// Block in a `full` format.
///
/// A `FBlock` compared to a `Block` has the following more
/// attributes computed:
/// - `block hash`
/// - `transaction id`
/// - `output addresses`
/// - `output script types`
///
#[derive(Serialize, Deserialize)]
pub struct FBlock {
    pub header: FBlockHeader,
    pub txdata: Vec<FTransaction>,
}

impl From<Block> for FBlock {
    /// obtain addresses for each output of each transactions
    fn from(block: bitcoin::Block) -> FBlock {
        let block_hash = block.header.block_hash();
        FBlock {
            header: FBlockHeader::parse(block.header, block_hash),
            txdata: block.txdata.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct FBlockHeader {
    pub version: i32,
    pub block_hash: BlockHash,
    pub prev_blockhash: BlockHash,
    pub merkle_root: TxMerkleNode,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
}

impl FBlockHeader {
    /// obtain addresses for each output
    pub fn parse(b: bitcoin::BlockHeader, block_hash: BlockHash) -> FBlockHeader {
        FBlockHeader {
            version: b.version,
            block_hash,
            prev_blockhash: b.prev_blockhash,
            merkle_root: b.merkle_root,
            time: b.time,
            bits: b.bits,
            nonce: b.nonce,
        }
    }
}

/// `FTransaction` compared to `Transaction` has the following
/// precomputed:
/// - `transaction ID`
/// - `output script type`
/// - `output addresses`
#[derive(Serialize, Deserialize)]
pub struct FTransaction {
    pub version: i32,
    pub lock_time: u32,
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<bitcoin::TxIn>,
    /// List of outputs
    pub output: Vec<FTxOut>,
}

impl From<Transaction> for FTransaction {
    /// obtain addresses for each output
    fn from(tx: Transaction) -> FTransaction {
        FTransaction {
            version: tx.version,
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: tx.input,
            output: tx.output.into_iter().map(FTxOut::from).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FTxOut {
    pub value: u64,
    pub script_pubkey: bitcoin::Script,
    pub script_type: Type,
    pub addresses: Vec<Address>,
}

impl From<TxOut> for FTxOut {
    fn from(out: bitcoin::TxOut) -> FTxOut {
        let eval = evaluate_script(&out.script_pubkey, bitcoin::Network::Bitcoin);
        FTxOut {
            value: out.value,
            script_pubkey: out.script_pubkey,
            script_type: eval.pattern,
            addresses: eval.addresses,
        }
    }
}
