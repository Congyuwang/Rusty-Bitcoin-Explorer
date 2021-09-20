///
/// All iterators in this module are parallel, and contain threads.
/// Join the threads to the main after consuming the iterators!
///
use crate::api::BitcoinDB;
use crate::bitcoinparser::blk_file::BlkFile;
use crate::bitcoinparser::block_index::BlockIndex;
use crate::bitcoinparser::errors::{OpError, OpResult};
use crate::bitcoinparser::proto::connected_proto::{
    ConnectedBlock, ConnectedTransaction, FConnectedBlock, SConnectedBlock,
};
use crate::bitcoinparser::proto::full_proto::FBlock;
use crate::bitcoinparser::proto::simple_proto::SBlock;
use bitcoin::{Block, TxOut, Txid};
use log::warn;
use num_cpus;
use std::borrow::BorrowMut;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

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

#[derive(Debug)]
struct Task {
    height: u32,
    result_height: Arc<(Mutex<u32>, Condvar)>,
    sender: SyncSender<Block>,
    error_state: Arc<AtomicBool>,
}

struct Task2 {
    height: u32,
    outputs_insertion_height: Arc<(Mutex<u32>, Condvar)>,
    result_height: Arc<(Mutex<u32>, Condvar)>,
    sender: SyncSender<ConnectedBlock>,
    error_state: Arc<AtomicBool>,
}

fn mutate_error_state(mut task: Task) {
    // before return, always increase lock condition
    {
        let (lock, cond) = &*task.result_height;
        let mut result_height = lock.lock().unwrap();
        if *result_height != task.height {
            result_height =
                cond.wait_while(result_height, |h| *h != task.height).unwrap();
        }
        // wait until the prior ones have finished before changing error state
        {
            let err = task.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        *result_height += 1;
        cond.notify_all();
    }
}

fn increment_result_lock(task: &Task2) {
    let (lock, cond) = &*task.result_height;
    let mut result_height = lock.lock().unwrap();
    *result_height += 1;
    cond.notify_all();
}

fn mutate_error_both_lock(task: &mut Task2) {
    let (lock, cond) = &*task.outputs_insertion_height;
    let mut outputs_insertion_height = lock.lock().unwrap();
    if *outputs_insertion_height != task.height {
        outputs_insertion_height =
            cond.wait_while(outputs_insertion_height, |h| *h != task.height).unwrap();
    }
    *outputs_insertion_height += 1;
    cond.notify_all();
    let (lock, cond) = &*task.result_height;
    let mut result_height = lock.lock().unwrap();
    if *result_height != task.height {
        result_height =
            cond.wait_while(result_height, |h| *h != task.height).unwrap();
    }
    *result_height += 1;
    cond.notify_all();
    // wait until prior ones to have finished both parts
    {
        let err = task.error_state.borrow_mut();
        err.fetch_or(true, Ordering::SeqCst);
    }
}

fn fetch_block(db: &DBCopy, task: Task) {
    // check error state at beginning, preventing new works generated
    // which might result in deadlock
    if task.error_state.load(Ordering::SeqCst) {
        // should never increment lock condition here!! Otherwise later tasks might skip
        // over unfinished tasks
        return;
    }
    let my_height = task.height;

    if let Some(index) = db.block_index.records.get(my_height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {
            Ok(blk) => {
                // send data when it is my turn to send
                {
                    let (lock, cond) = &*task.result_height;
                    let mut result_height = lock.lock().unwrap();
                    if *result_height != my_height {
                        result_height =
                            cond.wait_while(result_height, |h| *h != my_height).unwrap();
                    }
                    // this block kills waiting threads after error
                    if task.error_state.load(Ordering::SeqCst) {
                        *result_height += 1;
                        cond.notify_all();
                        // before return, always increase lock condition
                        return;
                    }
                    task.sender.send(blk).unwrap();
                    *result_height += 1;
                    cond.notify_all();
                }
            }
            Err(_) => {
                mutate_error_state(task);
                return
            },
        }
    } else {
        // set error_state to true
        mutate_error_state(task);
        return
    }
}

fn fetch_block_connected(
    mut unspent: &Arc<Mutex<HashMap<Txid, Arc<Mutex<BTreeMap<u16, TxOut>>>>>>,
    db: &DBCopy,
    mut task: Task2,
) {
    // stop new tasks from loading
    if task.error_state.load(Ordering::SeqCst) {
        // should never increment lock condition here!! Otherwise later tasks might skip
        // over unfinished tasks
        return;
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
                    outputs.push(tx.output);
                    inputs.push(tx.input);
                }
                let outputs_copy = outputs.clone();
                let mut output_block = ConnectedBlock {
                    header: block.header,
                    txdata: Vec::new(),
                };

                // insert new transactions
                for (txid, outs) in tx_ids.iter().zip(outputs) {
                    let new_unspent: Arc<Mutex<BTreeMap<u16, TxOut>>> =
                        Arc::new(Mutex::new(BTreeMap::new()));
                    let mut n: u16 = 0;

                    // the new transaction should not be in unspent
                    if unspent.lock().unwrap().contains_key(txid) {
                        warn!("found duplicate key {}", txid);
                    }

                    // long locking of new_unspent_inner, no query anyway
                    {
                        let mut new_unspent_inner = new_unspent.lock().unwrap();
                        for output in outs {
                            new_unspent_inner.insert(n, output);
                            n += 1;
                        }
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
                        // may be the later tasks are waiting in the second lock, increment lock
                        increment_result_lock(&task);
                        return;
                    }
                    *outputs_insertion_height += 1;
                    cond.notify_all();
                }

                for (((txid, ins), outs), lock_time) in
                    tx_ids.iter().zip(inputs).zip(outputs_copy).zip(lock_times)
                {
                    let mut output_tx = ConnectedTransaction {
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
                        let n = *&input.previous_output.vout as u16;

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
                            let unspent: Option<TxOut> = prev_tx.lock().unwrap().remove(&n);
                            if let Some(unspent) = unspent {
                                output_tx.input.push(unspent);
                            } else {
                                warn!("cannot find previous outpoint, bad data");
                                {
                                    let err = task.error_state.borrow_mut();
                                    err.fetch_or(true, Ordering::SeqCst);
                                }
                                // only increment result lock
                                increment_result_lock(&task);
                                return;
                            }
                        } else {
                            warn!("cannot find previous transactions, bad data");
                            {
                                let err = task.error_state.borrow_mut();
                                err.fetch_or(true, Ordering::SeqCst);
                            }
                            // only increment result lock
                            increment_result_lock(&task);
                            return;
                        }
                    }
                    output_block.txdata.push(output_tx);
                }

                // clean up after processing a block
                let mut to_remove: Vec<Txid> = Vec::new();
                // might lock for a relatively long time
                for (txid, unspent) in unspent.lock().unwrap().iter() {
                    if unspent.lock().unwrap().len() == 0 {
                        to_remove.push(txid.clone())
                    }
                }
                {
                    let mut unspent_lock = unspent.lock().unwrap();
                    for txid in to_remove {
                        unspent_lock.remove(&txid);
                    }
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
                        return;
                    }
                    task.sender.send(output_block).unwrap();
                    *result_height += 1;
                    cond.notify_all();
                }
            }
            Err(_) => {
                // set error_state to true
                mutate_error_both_lock(&mut task);
            }
        }
    } else {
        // set error_state to true
        mutate_error_both_lock(&mut task);
    }
}

struct BlockIteratorArray {
    receiver: Receiver<Block>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl Drop for BlockIteratorArray {
    // attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl BlockIteratorArray {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> BlockIteratorArray {
        let cpus = num_cpus::get();
        let output_height = Arc::new((Mutex::new(*heights.get(0).unwrap()), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = sync_channel(cpus * 10);
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let error_state_copy = error_state.clone();
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<Task> = VecDeque::with_capacity(heights.len());
            for height in heights {
                tasks.push_back(Task {
                    height,
                    result_height: output_height.clone(),
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
                            Some(task) => fetch_block(&db_copy, task),
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
        BlockIteratorArray {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap();
    }
}

impl Iterator for BlockIteratorArray {
    type Item = Block;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}

pub struct SBlockIteratorSequential {
    inner: BlockIteratorArray,
}

impl SBlockIteratorSequential {
    pub fn new(db: &BitcoinDB, start: u32, end: u32) -> OpResult<SBlockIteratorSequential> {
        if end <= start {
            Err(OpError::from("invalid iterator range"))
        } else {
            let heights: Vec<u32> = (start..end).collect();
            Ok(SBlockIteratorSequential {
                inner: BlockIteratorArray::new(db, heights),
            })
        }
    }
}

impl Iterator for SBlockIteratorSequential {
    type Item = SBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(block) => Some(SBlock::parse(block)),
        }
    }
}

pub struct FBlockIteratorSequential {
    inner: BlockIteratorArray,
}

impl FBlockIteratorSequential {
    pub fn new(db: &BitcoinDB, start: u32, end: u32) -> OpResult<FBlockIteratorSequential> {
        if end <= start {
            Err(OpError::from("invalid iterator range"))
        } else {
            let heights: Vec<u32> = (start..end).collect();
            Ok(FBlockIteratorSequential {
                inner: BlockIteratorArray::new(db, heights),
            })
        }
    }
}

impl Iterator for FBlockIteratorSequential {
    type Item = FBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(block) => Some(FBlock::parse(block)),
        }
    }
}


pub struct ConnectedBlockIterator {
    receiver: Receiver<ConnectedBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl Drop for ConnectedBlockIterator {
    /// attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl ConnectedBlockIterator {
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, end: u32) -> ConnectedBlockIterator {
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
            let mut tasks: VecDeque<Task2> = VecDeque::with_capacity(end as usize);
            for height in 0..end {
                tasks.push_back(Task2 {
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
                            Some(task) => fetch_block_connected(&unspent_copy, &db_copy, task),
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
        ConnectedBlockIterator {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap()
    }
}

impl Iterator for ConnectedBlockIterator {
    type Item = ConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}

/// these can only iterator sequentially from the genesis block
pub struct FConnectedBlockIterator {
    inner: ConnectedBlockIterator,
}

impl FConnectedBlockIterator {
    pub fn new(db: &BitcoinDB, end: u32) -> FConnectedBlockIterator {
        FConnectedBlockIterator {
            inner: ConnectedBlockIterator::new(db, end),
        }
    }
}

impl Iterator for FConnectedBlockIterator {
    type Item = FConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(block) => Some(FConnectedBlock::from_connected(block)),
        }
    }
}

/// these can only iterator sequentially from the genesis block
pub struct SConnectedBlockIterator {
    inner: ConnectedBlockIterator,
}

impl SConnectedBlockIterator {
    pub fn new(db: &BitcoinDB, end: u32) -> SConnectedBlockIterator {
        SConnectedBlockIterator {
            inner: ConnectedBlockIterator::new(db, end),
        }
    }
}

impl Iterator for SConnectedBlockIterator {
    type Item = SConnectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(block) => Some(SConnectedBlock::from_connected(block)),
        }
    }
}
