use crate::bitcoinparser::script::{evaluate_script, Type};
use bitcoin::{Address, Txid, BlockHash, TxMerkleNode};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub txdata: Vec<Transaction>,
}

impl Block {
    /// obtain addresses for each output of each transactions
    pub fn parse(block: bitcoin::Block) -> Block {
        let block_hash = *&block.block_hash();
        Block {
            header: BlockHeader::parse(block.header, block_hash),
            txdata: block.txdata.into_iter().map(Transaction::parse).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BlockHeader {
    pub version: i32,
    pub block_hash: BlockHash,
    pub prev_blockhash: BlockHash,
    pub merkle_root: TxMerkleNode,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
}

impl BlockHeader {
    /// obtain addresses for each output
    pub fn parse(b: bitcoin::BlockHeader, block_hash: BlockHash) -> BlockHeader {
        BlockHeader {
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
pub struct Transaction {
    pub version: i32,
    pub lock_time: u32,
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<bitcoin::TxIn>,
    /// List of outputs
    pub output: Vec<TxOut>,
}

impl Transaction {
    /// obtain addresses for each output
    pub fn parse(tx: bitcoin::Transaction) -> Transaction {
        Transaction {
            version: tx.version,
            lock_time: tx.lock_time,
            txid: tx.txid(),
            input: tx.input,
            output: tx.output.into_iter().map(TxOut::parse).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxOut {
    pub value: u64,
    pub script_pubkey: bitcoin::Script,
    pub script_type: Type,
    pub addresses: Vec<Address>,
}

impl TxOut {
    fn parse(out: bitcoin::TxOut) -> TxOut {
        let eval = evaluate_script(&out.script_pubkey, bitcoin::Network::Bitcoin);
        TxOut {
            value: out.value,
            script_pubkey: out.script_pubkey,
            script_type: eval.pattern,
            addresses: eval.addresses,
        }
    }
}
