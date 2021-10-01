use crate::api::{BitcoinDB, Txid};
use crate::iter::fetch_connected_async::{fetch_block_connected, TaskConnected};
use crate::iter::util::{DBCopy, VecMap};
use crate::parser::proto::connected_proto::{BlockConnectable, TxConnectable};
use std::borrow::BorrowMut;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// iterate through blocks, and connecting outpoints.
pub struct ConnectedBlockIter<TBlock> {
    receiver: Receiver<TBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl<T> ConnectedBlockIter<T> {
    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap()
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

impl<TBlock> ConnectedBlockIter<TBlock>
where
    TBlock: 'static + BlockConnectable + Send,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, end: u32) -> Self {
        let cpus = num_cpus::get();
        let outputs_insertion_height = Arc::new((Mutex::new(0), Condvar::new()));
        let result_height = Arc::new((Mutex::new(0), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let error_state_copy = error_state.clone();
        let (sender, receiver) = sync_channel(cpus * 10);
        let unspent: Arc<
            Mutex<BTreeMap<Txid, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>,
        > = Arc::new(Mutex::new(BTreeMap::new()));
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<TaskConnected<TBlock>> = VecDeque::with_capacity(end as usize);
            for height in 0..end {
                tasks.push_back(TaskConnected {
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
                                if !fetch_block_connected(&unspent_copy, &db_copy, task) {
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
        ConnectedBlockIter {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }
}

impl<TBlock> Iterator for ConnectedBlockIter<TBlock> {
    type Item = TBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}
