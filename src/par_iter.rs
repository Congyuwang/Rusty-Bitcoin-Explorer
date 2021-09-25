///
/// All iterators in this module are parallel, and contain threads.
/// Join the threads to the main after consuming the iterators!
///
use crate::api::BitcoinDB;
use crate::parser::blk_file::BlkFile;
use crate::parser::block_index::BlockIndex;
use crate::parser::errors::{OpError, OpResult};
use crate::parser::proto::connected_proto::{
    FConnectedBlock, FConnectedTransaction, SConnectedBlock, SConnectedTransaction,
};
use crate::parser::proto::full_proto::{FBlock, FBlockHeader, FTxOut};
use crate::parser::proto::simple_proto::{SBlock, SBlockHeader, STxOut};
use bitcoin::Txid;
use log::warn;
use num_cpus;
use std::borrow::BorrowMut;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

struct SVecMap {
    size: u16,
    inner: Box<[Option<STxOut>]>,
}

impl SVecMap {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn from(vec: Vec<STxOut>) -> SVecMap {
        let size = vec.len();
        let inner_vec: Vec<Option<STxOut>> = vec.into_iter().map(|o| Some(o)).collect();
        SVecMap {
            size: size as u16,
            inner: inner_vec.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn remove(&mut self, n: usize) -> Option<STxOut> {
        let element = &mut self.inner[n];
        if let Some(_) = element {
            self.size -= 1;
        };
        element.take()
    }
}

struct FVecMap {
    size: u16,
    inner: Box<[Option<FTxOut>]>,
}

impl FVecMap {
    #[inline]
    fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    fn from(vec: Vec<FTxOut>) -> FVecMap {
        let size = vec.len();
        let inner_vec: Vec<Option<FTxOut>> = vec.into_iter().map(|o| Some(o)).collect();
        FVecMap {
            size: size as u16,
            inner: inner_vec.into_boxed_slice(),
        }
    }

    #[inline]
    fn remove(&mut self, n: usize) -> Option<FTxOut> {
        let element = &mut self.inner[n];
        if let Some(_) = element {
            self.size -= 1;
        };
        element.take()
    }
}

/// python iterator implementation does not allow lifetime.
/// Thus, we must own the necessary resource for the iterator
/// to work for python.
#[derive(Clone)]
struct DBCopy {
    pub block_index: BlockIndex,
    pub blk_file: BlkFile,
}

impl DBCopy {
    fn from_bitcoin_db(db: &BitcoinDB) -> DBCopy {
        DBCopy {
            block_index: db.block_index.clone(),
            blk_file: db.blk_file.clone(),
        }
    }
}

struct STask {
    task_number: u32,
    height: u32,
    output_number: Arc<(Mutex<u32>, Condvar)>,
    sender: SyncSender<SBlock>,
    error_state: Arc<AtomicBool>,
}

struct FTask {
    task_number: u32,
    height: u32,
    output_number: Arc<(Mutex<u32>, Condvar)>,
    sender: SyncSender<FBlock>,
    error_state: Arc<AtomicBool>,
}

struct STask2 {
    height: u32,
    outputs_insertion_height: Arc<(Mutex<u32>, Condvar)>,
    result_height: Arc<(Mutex<u32>, Condvar)>,
    sender: SyncSender<SConnectedBlock>,
    error_state: Arc<AtomicBool>,
}

struct FTask2 {
    height: u32,
    outputs_insertion_height: Arc<(Mutex<u32>, Condvar)>,
    result_height: Arc<(Mutex<u32>, Condvar)>,
    sender: SyncSender<FConnectedBlock>,
    error_state: Arc<AtomicBool>,
}

fn mutate_error_state_s(mut task: STask) {
    // before return, always increase lock condition
    let (lock, cond) = &*task.output_number;
    let mut result_height = lock.lock().unwrap();
    if *result_height != task.task_number {
        result_height = cond
            .wait_while(result_height, |h| *h != task.task_number)
            .unwrap();
    }
    // wait until the prior ones have finished before changing error state
    // change error state before releasing result_height
    {
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
    *result_height += 1;
    cond.notify_all();
}

fn mutate_error_state_f(mut task: FTask) {
    // before return, always increase lock condition
    {
        let (lock, cond) = &*task.output_number;
        let mut result_height = lock.lock().unwrap();
        if *result_height != task.task_number {
            result_height = cond
                .wait_while(result_height, |h| *h != task.task_number)
                .unwrap();
        }
        // wait until the prior ones have finished before changing error state
        // change error state before releasing result_height
        {
            let err = task.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        *result_height += 1;
        cond.notify_all();
    }
}

fn increment_result_lock_s(task: &mut STask2) {
    let (lock, cond) = &*task.result_height;
    let mut result_height = lock.lock().unwrap();
    {
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
    *result_height += 1;
    cond.notify_all();
}

fn increment_result_lock_f(task: &mut FTask2) {
    let (lock, cond) = &*task.result_height;
    let mut result_height = lock.lock().unwrap();
    {
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
    *result_height += 1;
    cond.notify_all();
}

fn mutate_error_both_lock_s(task: &mut STask2) {
    let (lock, cond) = &*task.outputs_insertion_height;
    let mut outputs_insertion_height = lock.lock().unwrap();
    if *outputs_insertion_height != task.height {
        outputs_insertion_height = cond
            .wait_while(outputs_insertion_height, |h| *h != task.height)
            .unwrap();
    }
    *outputs_insertion_height += 1;
    cond.notify_all();
    let (lock, cond) = &*task.result_height;
    let mut result_height = lock.lock().unwrap();
    if *result_height != task.height {
        result_height = cond
            .wait_while(result_height, |h| *h != task.height)
            .unwrap();
    }
    // now you are holding two locks
    // wait until prior ones to have finished both parts
    // change the error state before letting the later tasks go
    {
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
    *result_height += 1;
    cond.notify_all();
}

fn mutate_error_both_lock_f(task: &mut FTask2) {
    let (lock, cond) = &*task.outputs_insertion_height;
    let mut outputs_insertion_height = lock.lock().unwrap();
    if *outputs_insertion_height != task.height {
        outputs_insertion_height = cond
            .wait_while(outputs_insertion_height, |h| *h != task.height)
            .unwrap();
    }
    *outputs_insertion_height += 1;
    cond.notify_all();
    let (lock, cond) = &*task.result_height;
    let mut result_height = lock.lock().unwrap();
    if *result_height != task.height {
        result_height = cond
            .wait_while(result_height, |h| *h != task.height)
            .unwrap();
    }
    // now you are holding two locks
    // wait until prior ones to have finished both parts
    // change the error state before letting the later tasks go
    {
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
    *result_height += 1;
    cond.notify_all();
}

fn fetch_block_s(db: &DBCopy, task: STask) -> bool {
    // check error state at beginning, preventing new works generated
    // which might result in deadlock
    if task.error_state.load(Ordering::SeqCst) {
        // should never increment lock condition here!! Otherwise later tasks might skip
        // over unfinished tasks
        return false;
    }
    let task_number = task.task_number;

    if let Some(index) = db.block_index.records.get(task.height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {
            Ok(blk) => {
                // send data when it is my turn to send
                {
                    let (lock, cond) = &*task.output_number;
                    let mut output_number = lock.lock().unwrap();
                    if *output_number != task_number {
                        output_number = cond
                            .wait_while(output_number, |h| *h != task_number)
                            .unwrap();
                    }
                    // this block kills waiting threads after error
                    if task.error_state.load(Ordering::SeqCst) {
                        *output_number += 1;
                        cond.notify_all();
                        // before return, always increase lock condition
                        return false;
                    }
                    task.sender.send(SBlock::parse(blk)).unwrap();
                    *output_number += 1;
                    cond.notify_all();
                }
                true
            }
            Err(_) => {
                mutate_error_state_s(task);
                return false;
            }
        }
    } else {
        // set error_state to true
        mutate_error_state_s(task);
        return false;
    }
}

fn fetch_block_f(db: &DBCopy, task: FTask) -> bool {
    // check error state at beginning, preventing new works generated
    // which might result in deadlock
    if task.error_state.load(Ordering::SeqCst) {
        // should never increment lock condition here!! Otherwise later tasks might skip
        // over unfinished tasks
        return false;
    }
    let task_number = task.task_number;

    if let Some(index) = db.block_index.records.get(task.height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {
            Ok(blk) => {
                // send data when it is my turn to send
                {
                    let (lock, cond) = &*task.output_number;
                    let mut output_number = lock.lock().unwrap();
                    if *output_number != task_number {
                        output_number = cond
                            .wait_while(output_number, |h| *h != task_number)
                            .unwrap();
                    }
                    // this block kills waiting threads after error
                    if task.error_state.load(Ordering::SeqCst) {
                        *output_number += 1;
                        cond.notify_all();
                        // before return, always increase lock condition
                        return false;
                    }
                    task.sender.send(FBlock::parse(blk)).unwrap();
                    *output_number += 1;
                    cond.notify_all();
                }
                true
            }
            Err(_) => {
                mutate_error_state_f(task);
                return false;
            }
        }
    } else {
        // set error_state to true
        mutate_error_state_f(task);
        return false;
    }
}

fn fetch_fblock_connected(
    mut unspent: &Arc<Mutex<HashMap<Txid, Arc<Mutex<FVecMap>>>>>,
    db: &DBCopy,
    mut task: FTask2,
) -> bool {
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
                let mut lock_times = Vec::new();
                let mut tx_ids = Vec::new();
                let mut outputs = Vec::new();
                let mut inputs = Vec::new();
                for tx in block.txdata {
                    lock_times.push(tx.lock_time);
                    tx_ids.push(tx.txid());
                    let outs: Vec<FTxOut> = tx.output.into_iter().map(FTxOut::parse).collect();
                    outputs.push(outs);
                    inputs.push(tx.input);
                }
                let outputs_copy = outputs.clone();
                let block_hash = block.header.block_hash();
                let mut output_block = FConnectedBlock {
                    header: FBlockHeader::parse(block.header, block_hash),
                    txdata: Vec::new(),
                };

                // insert new transactions
                for (txid, outs) in tx_ids.iter().zip(outputs) {
                    let new_unspent: Arc<Mutex<FVecMap>> =
                        Arc::new(Mutex::new(FVecMap::from(outs)));

                    // the new transaction should not be in unspent
                    if unspent.lock().unwrap().contains_key(txid) {
                        warn!("found duplicate key {}", txid);
                    }

                    // temporary borrow locking of unspent
                    unspent
                        .borrow_mut()
                        .lock()
                        .unwrap()
                        .insert(txid.clone(), new_unspent);
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
                        let (lock, cond2) = &*task.result_height;
                        let mut result_height = lock.lock().unwrap();
                        *result_height += 1;
                        cond2.notify_all();
                        return false;
                    }
                    *outputs_insertion_height += 1;
                    cond.notify_all();
                }

                for (((txid, ins), outs), lock_time) in
                    tx_ids.iter().zip(inputs).zip(outputs_copy).zip(lock_times)
                {
                    let mut output_tx = FConnectedTransaction {
                        lock_time,
                        txid: txid.clone(),
                        input: Vec::new(),
                        output: outs,
                    };

                    // spend new inputs
                    for input in ins {
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
                                output_tx.input.push(out);
                            } else {
                                warn!("cannot find previous outpoint, bad data");
                                // only increment result lock
                                increment_result_lock_f(&mut task);
                                return false;
                            }
                        } else {
                            warn!("cannot find previous transactions, bad data");
                            // only increment result lock
                            increment_result_lock_f(&mut task);
                            return false;
                        }
                    }
                    output_block.txdata.push(output_tx);
                }

                // send when it is my turn
                {
                    let (lock, cond) = &*task.result_height;
                    let mut result_height = lock.lock().unwrap();
                    if *result_height != my_height {
                        result_height =
                            cond.wait_while(result_height, |h| *h != my_height).unwrap();
                    }
                    // end tasks waiting in the second part
                    if task.error_state.load(Ordering::SeqCst) {
                        *result_height += 1;
                        cond.notify_all();
                        return false;
                    }
                    task.sender.send(output_block).unwrap();
                    *result_height += 1;
                    cond.notify_all();
                }
                true
            }
            Err(_) => {
                // set error_state to true
                mutate_error_both_lock_f(&mut task);
                false
            }
        }
    } else {
        // set error_state to true
        mutate_error_both_lock_f(&mut task);
        false
    }
}

fn fetch_sblock_connected(
    mut unspent: &Arc<Mutex<HashMap<Txid, Arc<Mutex<SVecMap>>>>>,
    db: &DBCopy,
    mut task: STask2,
) -> bool {
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
                let mut tx_ids = Vec::new();
                let mut outputs = Vec::new();
                let mut inputs = Vec::new();
                for tx in block.txdata {
                    tx_ids.push(tx.txid());
                    let outs: Vec<STxOut> = tx.output.into_iter().map(STxOut::parse).collect();
                    outputs.push(outs);
                    inputs.push(tx.input);
                }
                let outputs_copy = outputs.clone();
                let block_hash = block.header.block_hash();
                let mut output_block = SConnectedBlock {
                    header: SBlockHeader::parse(block.header, block_hash),
                    txdata: Vec::new(),
                };

                // insert new transactions
                for (txid, outs) in tx_ids.iter().zip(outputs) {
                    let new_unspent: Arc<Mutex<SVecMap>> =
                        Arc::new(Mutex::new(SVecMap::from(outs)));

                    // the new transaction should not be in unspent
                    if unspent.lock().unwrap().contains_key(txid) {
                        warn!("found duplicate key {}", txid);
                    }
                    // temporary borrow locking of unspent
                    unspent
                        .borrow_mut()
                        .lock()
                        .unwrap()
                        .insert(txid.clone(), new_unspent);
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
                        // may be the later tasks are waiting in the second lock, increment lock
                        *outputs_insertion_height += 1;
                        cond.notify_all();
                        let (lock, cond2) = &*task.result_height;
                        let mut result_height = lock.lock().unwrap();
                        *result_height += 1;
                        cond2.notify_all();
                        return false;
                    }
                    *outputs_insertion_height += 1;
                    cond.notify_all();
                }

                for ((txid, ins), outs) in tx_ids.iter().zip(inputs).zip(outputs_copy) {
                    let mut output_tx = SConnectedTransaction {
                        txid: txid.clone(),
                        input: Vec::new(),
                        output: outs,
                    };

                    // spend new inputs
                    for input in ins {
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
                                output_tx.input.push(out);
                            } else {
                                warn!("cannot find previous outpoint, bad data");
                                {
                                    let err = task.error_state.borrow_mut();
                                    err.fetch_or(true, Ordering::SeqCst);
                                }
                                // only increment result lock
                                increment_result_lock_s(&mut task);
                                return false;
                            }
                        } else {
                            warn!("cannot find previous transactions, bad data");
                            // only increment result lock
                            increment_result_lock_s(&mut task);
                            return false;
                        }
                    }
                    output_block.txdata.push(output_tx);
                }

                // send when it is my turn
                {
                    let (lock, cond) = &*task.result_height;
                    let mut result_height = lock.lock().unwrap();
                    if *result_height != my_height {
                        result_height =
                            cond.wait_while(result_height, |h| *h != my_height).unwrap();
                    }
                    // end tasks waiting in the second part
                    if task.error_state.load(Ordering::SeqCst) {
                        *result_height += 1;
                        cond.notify_all();
                        return false;
                    }
                    task.sender.send(output_block).unwrap();
                    *result_height += 1;
                    cond.notify_all();
                }
                true
            }
            Err(_) => {
                // set error_state to true
                mutate_error_both_lock_s(&mut task);
                false
            }
        }
    } else {
        // set error_state to true
        mutate_error_both_lock_s(&mut task);
        false
    }
}
pub struct SBlockIteratorArray {
    receiver: Receiver<SBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl Drop for SBlockIteratorArray {
    // attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl SBlockIteratorArray {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> SBlockIteratorArray {
        let cursor: Vec<u32> = (0..heights.len() as u32).collect();
        let cpus = num_cpus::get();
        let output_number = Arc::new((Mutex::new(*cursor.get(0).unwrap()), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = sync_channel(cpus * 10);
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let error_state_copy = error_state.clone();
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<STask> = VecDeque::with_capacity(cursor.len());
            for task_number in cursor {
                tasks.push_back(STask {
                    task_number,
                    height: *heights.get(task_number as usize).unwrap(),
                    output_number: output_number.clone(),
                    sender: sender.clone(),
                    error_state: error_state_copy.clone(),
                })
            }

            let tasks = Arc::new(Mutex::new(tasks));
            let mut handles = Vec::with_capacity(cpus);

            for _ in 0..cpus {
                let task = tasks.clone();
                let db_copy = db.clone();
                // actual worker
                let handle = thread::spawn(move || {
                    loop {
                        let task = {
                            // drop mutex immediately
                            let mut task = task.lock().unwrap();
                            task.pop_front()
                        };
                        match task {
                            // finish
                            None => break,
                            Some(task) => {
                                if !fetch_block_s(&db_copy, task) {
                                    break;
                                }
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
        SBlockIteratorArray {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap();
    }
}

impl Iterator for SBlockIteratorArray {
    type Item = SBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}

pub struct SBlockIteratorSequential {
    inner: SBlockIteratorArray,
}

impl SBlockIteratorSequential {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, start: u32, end: u32) -> OpResult<SBlockIteratorSequential> {
        if end <= start {
            Err(OpError::from("invalid iterator range"))
        } else {
            let heights: Vec<u32> = (start..end).collect();
            Ok(SBlockIteratorSequential {
                inner: SBlockIteratorArray::new(db, heights),
            })
        }
    }
}

impl Iterator for SBlockIteratorSequential {
    type Item = SBlock;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct FBlockIteratorArray {
    receiver: Receiver<FBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl Drop for FBlockIteratorArray {
    // attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl FBlockIteratorArray {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> FBlockIteratorArray {
        let cursor: Vec<u32> = (0..heights.len() as u32).collect();
        let cpus = num_cpus::get();
        let output_number = Arc::new((Mutex::new(*cursor.get(0).unwrap()), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = sync_channel(cpus * 10);
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let error_state_copy = error_state.clone();
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<FTask> = VecDeque::with_capacity(cursor.len());
            for task_number in cursor {
                tasks.push_back(FTask {
                    task_number,
                    height: *heights.get(task_number as usize).unwrap(),
                    output_number: output_number.clone(),
                    sender: sender.clone(),
                    error_state: error_state_copy.clone(),
                })
            }

            let tasks = Arc::new(Mutex::new(tasks));
            let mut handles = Vec::with_capacity(cpus);

            for _ in 0..cpus {
                let task = tasks.clone();
                let db_copy = db.clone();
                // actual worker
                let handle = thread::spawn(move || {
                    loop {
                        let task = {
                            // drop mutex immediately
                            let mut task = task.lock().unwrap();
                            task.pop_front()
                        };
                        match task {
                            // finish
                            None => break,
                            Some(task) => {
                                if !fetch_block_f(&db_copy, task) {
                                    break;
                                }
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
        FBlockIteratorArray {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap();
    }
}

impl Iterator for FBlockIteratorArray {
    type Item = FBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}

pub struct FBlockIteratorSequential {
    inner: FBlockIteratorArray,
}

impl FBlockIteratorSequential {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, start: u32, end: u32) -> OpResult<FBlockIteratorSequential> {
        if end <= start {
            Err(OpError::from("invalid iterator range"))
        } else {
            let heights: Vec<u32> = (start..end).collect();
            Ok(FBlockIteratorSequential {
                inner: FBlockIteratorArray::new(db, heights),
            })
        }
    }
}

impl Iterator for FBlockIteratorSequential {
    type Item = FBlock;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct SConnectedBlockIterator {
    receiver: Receiver<SConnectedBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl Drop for SConnectedBlockIterator {
    /// attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl SConnectedBlockIterator {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, end: u32) -> SConnectedBlockIterator {
        let cpus = num_cpus::get();
        let outputs_insertion_height = Arc::new((Mutex::new(0), Condvar::new()));
        let result_height = Arc::new((Mutex::new(0), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let error_state_copy = error_state.clone();
        let (sender, receiver) = sync_channel(cpus * 10);
        let unspent = Arc::new(Mutex::new(HashMap::new()));
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<STask2> = VecDeque::with_capacity(end as usize);
            for height in 0..end {
                tasks.push_back(STask2 {
                    height,
                    outputs_insertion_height: outputs_insertion_height.clone(),
                    result_height: result_height.clone(),
                    sender: sender.clone(),
                    error_state: error_state_copy.clone(),
                })
            }

            let tasks = Arc::new(Mutex::new(tasks));
            let mut handles = Vec::with_capacity(cpus);

            for _ in 0..cpus {
                let task = tasks.clone();
                let db_copy = db.clone();
                let unspent_copy = unspent.clone();
                // actual worker
                let handle = thread::spawn(move || {
                    loop {
                        let task = {
                            // drop mutex immediately
                            let mut task = task.lock().unwrap();
                            task.pop_front()
                        };
                        match task {
                            // finish
                            None => break,
                            Some(task) => {
                                if !fetch_sblock_connected(&unspent_copy, &db_copy, task) {
                                    break;
                                }
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
        SConnectedBlockIterator {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap()
    }
}

impl Iterator for SConnectedBlockIterator {
    type Item = SConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}

pub struct FConnectedBlockIterator {
    receiver: Receiver<FConnectedBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl Drop for FConnectedBlockIterator {
    /// attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl FConnectedBlockIterator {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, end: u32) -> FConnectedBlockIterator {
        let cpus = num_cpus::get();
        let outputs_insertion_height = Arc::new((Mutex::new(0), Condvar::new()));
        let result_height = Arc::new((Mutex::new(0), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let error_state_copy = error_state.clone();
        let (sender, receiver) = sync_channel(cpus * 10);
        let unspent = Arc::new(Mutex::new(HashMap::new()));
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<FTask2> = VecDeque::with_capacity(end as usize);
            for height in 0..end {
                tasks.push_back(FTask2 {
                    height,
                    outputs_insertion_height: outputs_insertion_height.clone(),
                    result_height: result_height.clone(),
                    sender: sender.clone(),
                    error_state: error_state_copy.clone(),
                })
            }

            let tasks = Arc::new(Mutex::new(tasks));
            let mut handles = Vec::with_capacity(cpus);

            for _ in 0..cpus {
                let task = tasks.clone();
                let db_copy = db.clone();
                let unspent_copy = unspent.clone();
                // actual worker
                let handle = thread::spawn(move || {
                    loop {
                        let task = {
                            // drop mutex immediately
                            let mut task = task.lock().unwrap();
                            task.pop_front()
                        };
                        match task {
                            // finish
                            None => break,
                            Some(task) => {
                                if !fetch_fblock_connected(&unspent_copy, &db_copy, task) {
                                    break;
                                }
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
        FConnectedBlockIterator {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap()
    }
}

impl Iterator for FConnectedBlockIterator {
    type Item = FConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod test_vec_map {
    use crate::parser::proto::simple_proto::STxOut;
    use crate::par_iter::SVecMap;
    use bitcoin::TxOut;

    #[test]
    fn test_vec_map() {
        let mut vec: SVecMap = SVecMap::from(vec![
            STxOut::parse(TxOut::default()),
            STxOut::parse(TxOut::default()),
            STxOut::parse(TxOut::default()),
        ]);
        assert_eq!(vec.size, 3);
        assert!(vec.remove(1).is_some());
        assert_eq!(vec.size, 2);
        assert!(vec.remove(1).is_none());
        assert_eq!(vec.size, 2);
        assert!(vec.remove(0).is_some());
        assert_eq!(vec.size, 1);
        assert!(vec.remove(0).is_none());
        assert_eq!(vec.size, 1);
        assert!(!vec.is_empty());
        assert!(vec.remove(2).is_some());
        assert!(vec.is_empty());
    }
}
