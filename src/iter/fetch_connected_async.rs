use crate::iter::util::{DBCopy, VecMap};
use crate::parser::proto::connected_proto::{BlockConnectable, TxConnectable};
use bitcoin::Txid;
use log::warn;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};

pub(crate) struct TaskConnected {
    pub(crate) height: u32,
    pub(crate) outputs_insertion_height: Arc<(Mutex<u32>, Condvar)>,
    pub(crate) error_state: Arc<AtomicBool>,
}

///
/// fetch_block_connected, thread safe
///
pub(crate) fn fetch_block_connected<TBlock>(
    mut unspent: &Arc<
        Mutex<HashMap<Txid, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>,
    >,
    db: &DBCopy,
    mut task: TaskConnected,
    sender: &SyncSender<TBlock>,
) -> bool
where
    TBlock: BlockConnectable,
{
    // stop new tasks from loading
    if task.error_state.load(Ordering::SeqCst) {
        // should never increment lock condition here!! Otherwise later tasks might skip
        // over unfinished tasks
        return false;
    }
    let my_height = task.height;

    if let Some(index) = db.block_index.records.get(my_height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {
            Ok(block) => {
                let txdata_copy = block.txdata.clone();
                let block_hash = block.header.block_hash();
                let mut output_block = TBlock::from(block.header, block_hash);

                // insert new transactions
                for tx in block.txdata {
                    let txid = tx.txid();
                    let mut outs: Vec<Option<<TBlock::Tx as TxConnectable>::TOut>> =
                        Vec::with_capacity(tx.output.len());
                    for o in tx.output {
                        outs.push(Some(o.into()));
                    }
                    let outs: VecMap<<TBlock::Tx as TxConnectable>::TOut> =
                        VecMap::from_vec(outs.into_boxed_slice());
                    let new_unspent: Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>> =
                        Arc::new(Mutex::new(outs));

                    // temporary borrow locking of unspent
                    unspent
                        .borrow_mut()
                        .lock()
                        .unwrap()
                        .insert(txid, new_unspent);
                }

                // proceed to output step when precedents finished outputs insertion
                {
                    let (lock, cond) = &*task.outputs_insertion_height;
                    let mut outputs_insertion_height = lock.lock().unwrap();
                    if *outputs_insertion_height != my_height {
                        outputs_insertion_height = cond
                            .wait_while(outputs_insertion_height, |h| *h != my_height)
                            .unwrap();
                    }
                    // this block ends task in waiting in the first period
                    if task.error_state.load(Ordering::SeqCst) {
                        *outputs_insertion_height += 1;
                        cond.notify_all();
                        return false;
                    }
                    *outputs_insertion_height += 1;
                    cond.notify_all();
                }

                for tx in txdata_copy {
                    let mut output_tx: TBlock::Tx = TxConnectable::from(&tx);

                    // spend new inputs
                    for input in tx.input {
                        // skip coinbase transaction
                        if input.previous_output.is_null() {
                            continue;
                        }

                        let prev_txid = &input.previous_output.txid;
                        let n = *&input.previous_output.vout as usize;

                        // temporarily lock unspent
                        let prev_tx = {
                            let prev_tx = unspent.lock().unwrap();
                            match prev_tx.get(prev_txid) {
                                None => None,
                                Some(tx) => Some(tx.clone()),
                            }
                        };
                        if let Some(prev_tx) = prev_tx {
                            // temporarily lock prev_tx
                            let tx_out = {
                                let mut prev_tx_lock = prev_tx.lock().unwrap();
                                let out = prev_tx_lock.remove(n);
                                // remove a key immediately when the key contains no transaction
                                if prev_tx_lock.is_empty() {
                                    unspent.lock().unwrap().remove(prev_txid);
                                }
                                out
                            };
                            if let Some(out) = tx_out {
                                output_tx.add_input(out);
                            } else {
                                warn!("cannot find previous outpoint, bad data");
                                // only increment result lock
                                mutate_result_error(&mut task);
                                return false;
                            }
                        } else {
                            warn!("cannot find previous transactions, bad data");
                            // only increment result lock
                            mutate_result_error(&mut task);
                            return false;
                        }
                    }
                    output_block.add_tx(output_tx);
                }

                if task.error_state.load(Ordering::SeqCst) {
                    return false;
                }
                sender.send(output_block).unwrap();
                true
            }
            Err(_) => {
                // set error_state to true
                mutate_error_inc_lock(&mut task);
                false
            }
        }
    } else {
        // set error_state to true
        mutate_error_inc_lock(&mut task);
        false
    }
}

fn mutate_result_error(task: &mut TaskConnected) {
    let err = task.error_state.borrow_mut();
    err.fetch_or(true, Ordering::SeqCst);
}

/// wait for prior tasks, change error state, move to later tasks
fn mutate_error_inc_lock(task: &mut TaskConnected) {
    let (lock, cond) = &*task.outputs_insertion_height;
    let mut outputs_insertion_height = lock.lock().unwrap();
    if *outputs_insertion_height != task.height {
        outputs_insertion_height = cond
            .wait_while(outputs_insertion_height, |h| *h != task.height)
            .unwrap();
    }
    {
        // now you are holding the lock
        // wait until prior ones to have finished outputs insertion parts
        // change the error state before letting the later tasks go
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
    *outputs_insertion_height += 1;
    cond.notify_all();
}
