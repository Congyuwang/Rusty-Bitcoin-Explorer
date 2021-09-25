///
/// Add addresses, block_hash, tx_id to the bitcoin library format
///
use crate::parser::script::{evaluate_script, Type};
use bitcoin::{Address, BlockHash, TxMerkleNode, Txid};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FBlock {
    pub header: FBlockHeader,
    pub txdata: Vec<FTransaction>,
}

impl FBlock {
    /// obtain addresses for each output of each transactions
    pub fn parse(block: bitcoin::Block) -> FBlock {
        let block_hash = block.header.block_hash();
        FBlock {
            header: FBlockHeader::parse(block.header, block_hash),
            txdata: block.txdata.into_iter().map(FTransaction::parse).collect(),
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

impl FTransaction {
    /// obtain addresses for each output
    pub fn parse(tx: bitcoin::Transaction) -> FTransaction {
        FTransaction {
            version: tx.version,
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: tx.input,
            output: tx.output.into_iter().map(FTxOut::parse).collect(),
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

impl FTxOut {
    pub fn parse(out: bitcoin::TxOut) -> FTxOut {
        let eval = evaluate_script(&out.script_pubkey, bitcoin::Network::Bitcoin);
        FTxOut {
            value: out.value,
            script_pubkey: out.script_pubkey,
            script_type: eval.pattern,
            addresses: eval.addresses,
        }
    }
}
