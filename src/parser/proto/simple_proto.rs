use crate::parser::script::{evaluate_script, Type};
use bitcoin::{Address, Block, BlockHash, Transaction, TxIn, TxOut, Txid};
use serde::{Deserialize, Serialize};

///
/// Block in a `simple` format.
///
/// A `SBlock` compared to a `Block` has the following more
/// attributes precomputed:
/// - `block hash`
/// - `transaction id`
/// - `output addresses`
/// - `output script types`
///
/// But is has the following attributes removed:
/// - `nounce`
/// - `previous block hash`
/// - `merkle root`
/// - `bits`
/// - `input witness`
/// - `output public script key hash`
///
/// `SBlock` reduces the amount of data memorized or transferred.
#[derive(Serialize, Deserialize)]
pub struct SBlock {
    pub header: SBlockHeader,
    pub txdata: Vec<STransaction>,
}

impl From<Block> for SBlock {
    ///
    /// Add addresses, block_hash, tx_id to the bitcoin library format,
    /// and also simplify the format.
    ///
    fn from(block: Block) -> SBlock {
        let block_hash = block.header.block_hash();
        SBlock {
            header: SBlockHeader::parse(block.header, block_hash),
            txdata: block.txdata.into_iter().map(|x| x.into()).collect(),
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

/// `STransaction` compared to `Transaction` has the following
/// precomputed:
/// - `transaction ID`
/// - `output script type`
/// - `output addresses`
///
/// It has the following removed:
/// - `input witness`
/// - `output public script key hash`
///
/// It reduces the amount of data memorized or transferred (to python).
#[derive(Serialize, Deserialize)]
pub struct STransaction {
    pub txid: Txid,
    /// List of inputs
    pub input: Vec<STxIn>,
    /// List of outputs
    pub output: Vec<STxOut>,
}

impl From<Transaction> for STransaction {
    fn from(tx: Transaction) -> STransaction {
        STransaction {
            txid: tx.txid(),
            input: tx.input.into_iter().map(|x| x.into()).collect(),
            output: tx.output.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct STxIn {
    pub txid: Txid,
    pub vout: u32,
}

impl From<TxIn> for STxIn {
    fn from(tx_in: TxIn) -> STxIn {
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

impl From<TxOut> for STxOut {
    fn from(out: TxOut) -> STxOut {
        let eval = evaluate_script(&out.script_pubkey, bitcoin::Network::Bitcoin);
        STxOut {
            value: out.value,
            script_type: eval.pattern,
            addresses: eval.addresses,
        }
    }
}
