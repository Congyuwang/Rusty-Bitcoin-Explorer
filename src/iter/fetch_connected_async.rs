use crate::iter::util::{Compress, DBCopy};
use crate::parser::proto::connected_proto::{BlockConnectable, TxConnectable};
use bitcoin::consensus::{Decodable, Encodable};
use bitcoin::{Block, TxOut};
use log::warn;
use rocksdb::{WriteBatch, WriteOptions, DB};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

///
/// read block, update cache
///
pub(crate) fn update_unspent_cache<TBlock>(
    unspent: &Arc<Mutex<DB>>,
    db: &DBCopy,
    height: u32,
    error_state: &Arc<AtomicBool>,
    channel: &Sender<Block>,
) -> bool
where
    TBlock: BlockConnectable,
{
    // stop new tasks from loading when error
    if error_state.load(Ordering::SeqCst) {
        return false;
    }

    let mut sync_write = WriteOptions::default();
    sync_write.set_sync(true);

    if let Some(index) = db.block_index.records.get(height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {
            Ok(block) => {
                let mut batch = WriteBatch::default();

                // insert new transactions
                for tx in block.txdata.iter() {
                    // clone outputs
                    let txid_compressed = tx.txid().compress();

                    let mut n: u32 = 0;
                    for o in tx.output.iter() {
                        let key = txo_key(txid_compressed, n);
                        let value = txo_to_u8(o);
                        batch.put(key, value);
                        n += 1;
                    }
                }
                // quick stopping in error state
                if error_state.load(Ordering::SeqCst) {
                    return false;
                }
                unspent.lock().unwrap().write(batch).expect("failed at writing");
                channel.send(block).unwrap();
                true
            }
            Err(_) => {
                // set error_state to true
                mutate_result_error(error_state);
                false
            }
        }
    } else {
        // set error_state to true
        mutate_result_error(error_state);
        false
    }
}

///
/// fetch_block_connected, thread safe
///
pub(crate) fn connect_outpoints<TBlock>(
    unspent: &Arc<Mutex<DB>>,
    error_state: &Arc<AtomicBool>,
    sender: &Sender<TBlock>,
    block: Block,
) -> bool
where
    TBlock: BlockConnectable,
{
    // stop new tasks from loading when error
    if error_state.load(Ordering::SeqCst) {
        return false;
    }

    let block_hash = block.header.block_hash();
    let mut output_block = TBlock::from(block.header, block_hash);

    // collect rocks db keys
    let mut keys = Vec::new();
    for tx in block.txdata.iter() {
        for input in tx.input.iter() {
            // skip coinbase transaction
            if input.previous_output.is_null() {
                continue;
            }

            keys.push(txo_key(
                input.previous_output.txid.compress(),
                input.previous_output.vout,
            ));
        }
    }

    // get keys
    let tx_outs = unspent.lock().unwrap().multi_get(keys.clone());

    // remove keys
    {
        let lock = unspent.lock().unwrap();
        for key in keys {
            match lock.delete(&key) {
                Ok(_) => {}
                Err(_) => {
                    warn!("failed to remove key {:?}", &key);
                }
            }
        }
    }

    // pointer to record read position in tx_outs
    let mut pos = 0;

    for tx in block.txdata {
        let mut output_tx: TBlock::Tx = TxConnectable::from(&tx);

        // spend new inputs
        for input in tx.input {
            // skip coinbase transaction
            if input.previous_output.is_null() {
                continue;
            }

            let prev_txo = match tx_outs.get(pos).unwrap() {
                Ok(bytes) => match bytes {
                    None => None,
                    Some(bytes) => txo_from_u8(bytes.as_slice()),
                },
                Err(_) => None,
            };

            if let Some(out) = prev_txo {
                output_tx.add_input(out.into());
                pos += 1;
            } else {
                warn!("cannot find previous outpoint, bad data");
                // only increment result lock
                mutate_result_error(error_state);
                return false;
            }
        }
        output_block.add_tx(output_tx);
    }

    if error_state.load(Ordering::SeqCst) {
        return false;
    }
    sender.send(output_block).unwrap();
    true
}

#[inline]
fn mutate_result_error(error_state: &Arc<AtomicBool>) {
    error_state.fetch_or(true, Ordering::SeqCst);
}

#[inline(always)]
fn txo_key(txid_compressed: u128, n: u32) -> Vec<u8> {
    let mut bytes = Vec::from(txid_compressed.to_ne_bytes());
    bytes.extend(n.to_ne_bytes());
    bytes
}

#[inline(always)]
fn txo_to_u8(txo: &TxOut) -> Vec<u8> {
    let mut bytes = Vec::new();
    txo.consensus_encode(&mut bytes).unwrap();
    bytes
}

#[inline(always)]
fn txo_from_u8(bytes: &[u8]) -> Option<TxOut> {
    match TxOut::consensus_decode(bytes) {
        Ok(txo) => Some(txo),
        Err(_) => None,
    }
}
