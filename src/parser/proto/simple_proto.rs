use crate::parser::script::{evaluate_script, Type};
use bitcoin::{Address, BlockHash, Txid};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SBlock {
    pub header: SBlockHeader,
    pub txdata: Vec<STransaction>,
}

impl SBlock {
    ///
    /// Add addresses, block_hash, tx_id to the bitcoin library format,
    /// and also simplify the format.
    ///
    pub fn parse(block: bitcoin::Block) -> SBlock {
        let block_hash = block.header.block_hash();
        SBlock {
            header: SBlockHeader::parse(block.header, block_hash),
            txdata: block.txdata.into_iter().map(STransaction::parse).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SBlockHeader {
    pub block_hash: BlockHash,
    pub time: u32,
}

impl SBlockHeader {
    pub fn parse(blk: bitcoin::BlockHeader, block_hash: BlockHash) -> SBlockHeader {
        SBlockHeader {
            block_hash,
            time: blk.time,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct STransaction {
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<STxIn>,
    /// List of outputs
    pub output: Vec<STxOut>,
}

impl STransaction {
    pub fn parse(tx: bitcoin::Transaction) -> STransaction {
        STransaction {
            txid: tx.txid(),
            input: tx.input.into_iter().map(STxIn::parse).collect(),
            output: tx.output.into_iter().map(STxOut::parse).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct STxIn {
    pub txid: Txid,
    pub vout: u32,
}

impl STxIn {
    pub fn parse(tx_in: bitcoin::TxIn) -> STxIn {
        STxIn {
            txid: tx_in.previous_output.txid,
            vout: tx_in.previous_output.vout,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct STxOut {
    pub value: u64,
    pub script_type: Type,
    pub addresses: Vec<Address>,
}

impl STxOut {
    pub fn parse(out: bitcoin::TxOut) -> STxOut {
        let eval = evaluate_script(&out.script_pubkey, bitcoin::Network::Bitcoin);
        STxOut {
            value: out.value,
            script_type: eval.pattern,
            addresses: eval.addresses,
        }
    }
}
