use crate::api::BitcoinDB;
use crate::iter::fetch_connected_async::{insert_outputs, consume_outputs};
use crate::iter::util::{DBCopy, VecMap};
use crate::parser::proto::connected_proto::{BlockConnectable, TxConnectable};
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use hash_hasher::HashedMap;

/// iterate through blocks, and connecting outpoints.
pub struct ConnectedBlockIter<TBlock> {
    result_receivers: Vec<Receiver<TBlock>>,
    result_order: Receiver<usize>,
    worker_thread: Option<Vec<JoinHandle<()>>>,
    error_state: Arc<AtomicBool>,
}

impl<TBlock> ConnectedBlockIter<TBlock>
where
    TBlock: 'static + BlockConnectable + Send,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, end: u32) -> Self {
        let cpus = num_cpus::get();
        let db = DBCopy::from_bitcoin_db(db);
        let mut handles = Vec::with_capacity(cpus * 2);

        // shared error state for stopping threads early
        let error_state = Arc::new(AtomicBool::new(false));

        // in-memory UTXO cache
        let unspent: Arc<
            Mutex<HashedMap<u128, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>,
        > = Arc::new(Mutex::new(HashedMap::default()));

        // all tasks
        let heights = Arc::new(Mutex::new((0..end).collect::<VecDeque<u32>>()));

        // the channel for synchronizing cache update
        let (block_receivers_sender, block_receivers) = sync_channel(cpus * 10);
        let block_receivers = Arc::new(Mutex::new(block_receivers));

        // output insertion
        for _ in 0..cpus {

            // block streams
            let (block_sender, block_receiver) = channel();
            let block_receiver = Arc::new(Mutex::new(block_receiver));

            let unspent = unspent.clone();
            let error_state = error_state.clone();
            let heights = heights.clone();
            let db = db.clone();
            let block_receivers_sender = block_receivers_sender.clone();

            // output cache insertion workers
            let handle = thread::spawn(move || {
                loop {
                    let height = {
                        let mut height = heights.lock().unwrap();
                        let next_height = height.pop_front();
                        if next_height.is_some() {
                            block_receivers_sender.send(block_receiver.clone()).unwrap();
                        }
                        next_height
                        // drop mutex immediately
                    };
                    match height {
                        // finish
                        None => break,
                        Some(height) => {
                            if !insert_outputs::<TBlock>(&unspent, &db, height, &error_state, &block_sender) {
                                break;
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // the channel for synchronizing output order
        let (result_register, result_order) = sync_channel(10 * cpus);

        // block_streams
        let mut result_receivers = Vec::with_capacity(cpus);

        // consume UTXO cache and produce output
        for thread_number in 0..cpus {

            // result streams
            let (result_sender, result_receiver) = channel();

            let register = result_register.clone();
            let unspent = unspent.clone();
            let error_state = error_state.clone();
            let block_receivers = block_receivers.clone();

            let handle = thread::spawn(move || {
                loop {
                    let blk = {
                        let receivers_locked = block_receivers.lock().unwrap();
                        if let Ok(receiver) = receivers_locked.recv() {
                            let lock = receiver.lock();
                            register.send(thread_number).unwrap();
                            match lock.unwrap().recv() {
                                Ok(blk) => blk,
                                Err(_) => break
                            }
                        } else {
                            break
                        }
                        // release receivers lock
                    };

                    if !consume_outputs(&unspent, &error_state, &result_sender, blk) {
                        break;
                    }
                }
            });

            result_receivers.push(result_receiver);
            handles.push(handle);
        }

        ConnectedBlockIter {
            result_receivers,
            result_order,
            worker_thread: Some(handles),
            error_state,
        }
    }
}

impl<TBlock> Iterator for ConnectedBlockIter<TBlock> {
    type Item = TBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.result_order.recv() {
            Ok(thread_number) => match self.result_receivers.get(thread_number).unwrap().recv() {
                Ok(block) => Some(block),
                Err(_) => None,
            },
            Err(_) => None,
        }
    }
}

impl<T> ConnectedBlockIter<T> {
    fn join(&mut self) {
        for handle in self.worker_thread.take().unwrap() {
            handle.join().unwrap()
        }
    }
}

impl<T> Drop for ConnectedBlockIter<T> {
    /// attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}
