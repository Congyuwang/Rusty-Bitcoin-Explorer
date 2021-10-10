use crate::iter::util::{Compress, DBCopy};
use crate::parser::proto::connected_proto::{BlockConnectable, TxConnectable};
use bitcoin::Block;
use log::warn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
#[cfg(not(feature = "on-disk-utxo"))] use hash_hasher::HashedMap;
#[cfg(not(feature = "on-disk-utxo"))] use crate::iter::util::VecMap;
#[cfg(feature = "on-disk-utxo")] use bitcoin::consensus::{Decodable, Encodable};
#[cfg(feature = "on-disk-utxo")] use bitcoin::TxOut;
#[cfg(feature = "on-disk-utxo")] use rocksdb::{WriteBatch, DB};

///
/// read block, update cache
///
pub(crate) fn update_unspent_cache<TBlock>(
    #[cfg(not(feature = "on-disk-utxo"))]
    unspent: &Arc<Mutex<HashedMap<u128, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>>,
    #[cfg(feature = "on-disk-utxo")]
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

    if let Some(index) = db.block_index.records.get(height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {

            #[cfg(not(feature = "on-disk-utxo"))]
            Ok(block) => {
                let mut new_unspent_cache = Vec::with_capacity(block.txdata.len());

                // insert new transactions
                for tx in block.txdata.iter() {
                    // clone outputs
                    let txid = tx.txid();
                    let mut outs: Vec<Option<<TBlock::Tx as TxConnectable>::TOut>> =
                        Vec::with_capacity(tx.output.len());
                    for o in tx.output.iter() {
                        outs.push(Some(o.clone().into()));
                    }

                    // update unspent cache
                    let outs: VecMap<<TBlock::Tx as TxConnectable>::TOut> =
                        VecMap::from_vec(outs.into_boxed_slice());
                    let new_unspent = Arc::new(Mutex::new(outs));
                    let txid_compressed = txid.compress();

                    // the new transaction should not be in unspent
                    #[cfg(debug_assertions)]
                    if unspent.lock().unwrap().contains_key(&txid_compressed) {
                        warn!("found duplicate key {}", &txid);
                    }

                    new_unspent_cache.push((txid_compressed, new_unspent));
                }
                // quick stopping in error state
                if error_state.load(Ordering::SeqCst) {
                    return false;
                }
                unspent.lock().unwrap().extend(new_unspent_cache);
                channel.send(block).unwrap();
                true
            }

            #[cfg(feature = "on-disk-utxo")]
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
    #[cfg(not(feature = "on-disk-utxo"))]
    unspent: &Arc<Mutex<HashedMap<u128, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>>,
    #[cfg(feature = "on-disk-utxo")]
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
    #[cfg(feature = "on-disk-utxo")]
    let mut keys = Vec::new();

    #[cfg(feature = "on-disk-utxo")]
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

    // get utxo
    #[cfg(feature = "on-disk-utxo")]
    let tx_outs = unspent.lock().unwrap().multi_get(keys.clone());

    // remove keys
    #[cfg(feature = "on-disk-utxo")]
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
    #[cfg(feature = "on-disk-utxo")]
    let mut pos = 0;

    for tx in block.txdata {
        let mut output_tx: TBlock::Tx = TxConnectable::from(&tx);

        // spend new inputs
        for input in tx.input {
            // skip coinbase transaction
            if input.previous_output.is_null() {
                continue;
            }

            #[cfg(not(feature = "on-disk-utxo"))]
            let prev_txid = &input.previous_output.txid.compress();
            #[cfg(not(feature = "on-disk-utxo"))]
            let n = *&input.previous_output.vout as usize;

            // temporarily lock unspent
            #[cfg(not(feature = "on-disk-utxo"))]
            let prev_tx = {
                let prev_tx = unspent.lock().unwrap();
                match prev_tx.get(prev_txid) {
                    None => None,
                    Some(tx) => Some(tx.clone()),
                }
            };

            #[cfg(feature = "on-disk-utxo")]
            let prev_txo = match tx_outs.get(pos).unwrap() {
                Ok(bytes) => match bytes {
                    None => None,
                    Some(bytes) => txo_from_u8(bytes.as_slice()),
                },
                Err(_) => None,
            };

            #[cfg(not(feature = "on-disk-utxo"))]
            if let Some(prev_tx) = prev_tx {
                // temporarily lock prev_tx
                let (tx_out, is_empty) = {
                    let mut prev_tx_lock = prev_tx.lock().unwrap();
                    let tx_out = prev_tx_lock.remove(n);
                    let is_empty = prev_tx_lock.is_empty();
                    (tx_out, is_empty)
                };
                // remove a key immediately when the key contains no transaction
                if is_empty {
                    unspent.lock().unwrap().remove(prev_txid);
                }
                if let Some(out) = tx_out {
                    output_tx.add_input(out);
                } else {
                    warn!("cannot find previous outpoint, bad data");
                    // only increment result lock
                    mutate_result_error(error_state);
                    return false;
                }
            } else {
                warn!("cannot find previous transactions, bad data");
                // only increment result lock
                mutate_result_error(error_state);
                return false;
            }

            #[cfg(feature = "on-disk-utxo")]
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

#[inline(always)] #[cfg(feature = "on-disk-utxo")]
fn txo_key(txid_compressed: u128, n: u32) -> Vec<u8> {
    let mut bytes = Vec::from(txid_compressed.to_ne_bytes());
    bytes.extend(n.to_ne_bytes());
    bytes
}

#[inline(always)] #[cfg(feature = "on-disk-utxo")]
fn txo_to_u8(txo: &TxOut) -> Vec<u8> {
    let mut bytes = Vec::new();
    txo.consensus_encode(&mut bytes).unwrap();
    bytes
}

#[inline(always)] #[cfg(feature = "on-disk-utxo")]
fn txo_from_u8(bytes: &[u8]) -> Option<TxOut> {
    match TxOut::consensus_decode(bytes) {
        Ok(txo) => Some(txo),
        Err(_) => None,
    }
}
