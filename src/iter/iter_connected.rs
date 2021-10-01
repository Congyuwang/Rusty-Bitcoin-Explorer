use crate::api::{BitcoinDB, Txid};
use crate::iter::fetch_connected_async::{fetch_block_connected, TaskConnected};
use crate::iter::util::{DBCopy, VecMap};
use crate::parser::proto::connected_proto::{BlockConnectable, TxConnectable};
use std::borrow::BorrowMut;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// iterate through blocks, and connecting outpoints.
pub struct ConnectedBlockIter<TBlock> {
    receivers: Vec<Receiver<TBlock>>,
    task_order: Receiver<usize>,
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
        let outputs_insertion_height = Arc::new((Mutex::new(0), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let (task_register, task_order) = channel();
        let unspent: Arc<
            Mutex<HashMap<Txid, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>,
        > = Arc::new(Mutex::new(HashMap::new()));
        // worker master
        let mut tasks: VecDeque<TaskConnected> = VecDeque::with_capacity(end as usize);
        for height in 0..end {
            tasks.push_back(TaskConnected {
                height,
                outputs_insertion_height: outputs_insertion_height.clone(),
                error_state: error_state.clone(),
            })
        }

        let tasks = Arc::new(Mutex::new(tasks));
        let mut handles = Vec::with_capacity(cpus);
        let mut receivers = Vec::with_capacity(cpus);

        // workers
        for thread_number in 0..cpus {
            let (sender, receiver) = sync_channel(10);
            let task = tasks.clone();
            let register = task_register.clone();
            let db_copy = DBCopy::from_bitcoin_db(db);
            let unspent_copy = unspent.clone();
            // actual worker
            let handle = thread::spawn(move || {
                loop {
                    let task = {
                        let mut task = task.lock().unwrap();
                        if task.front().is_some() {
                            // when task queue is locked, register thread order
                            register.send(thread_number).unwrap();
                        }
                        task.pop_front()
                        // drop mutex immediately
                    };
                    match task {
                        // finish
                        None => break,
                        Some(task) => {
                            if !fetch_block_connected(&unspent_copy, &db_copy, task, &sender) {
                                break;
                            }
                        }
                    }
                }
            });
            receivers.push(receiver);
            handles.push(handle);
        }
        ConnectedBlockIter {
            receivers,
            task_order,
            worker_thread: Some(handles),
            error_state,
        }
    }
}

impl<TBlock> Iterator for ConnectedBlockIter<TBlock> {
    type Item = TBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.task_order.recv() {
            Ok(thread_number) => match self.receivers.get(thread_number).unwrap().recv() {
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
