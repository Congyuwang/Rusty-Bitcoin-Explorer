use crate::api::BitcoinDB;
use crate::iter::fetch_async::{fetch_block, Task};
use crate::iter::util::DBCopy;
use bitcoin::Block;
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// iterate through blocks according to array index.
pub struct BlockIterator<TBlock> {
    receiver: Receiver<TBlock>,
    worker_thread: Option<JoinHandle<()>>,
    error_state: Arc<AtomicBool>,
}

impl<T> BlockIterator<T> {
    fn join(&mut self) {
        self.worker_thread.take().unwrap().join().unwrap();
    }
}

impl<T> Drop for BlockIterator<T> {
    // attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl<TBlock> BlockIterator<TBlock>
where
    TBlock: From<Block> + Send + 'static,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> Self {
        let cursor: Vec<u32> = (0..heights.len() as u32).collect();
        let cpus = num_cpus::get();
        let output_number = Arc::new((Mutex::new(*cursor.get(0).unwrap()), Condvar::new()));
        let error_state = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = sync_channel(cpus * 10);
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let error_state_copy = error_state.clone();
        let worker_thread = thread::spawn(move || {
            let mut tasks: VecDeque<Task<TBlock>> = VecDeque::with_capacity(cursor.len());
            for task_number in cursor {
                tasks.push_back(Task {
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
                                if !fetch_block(&db_copy, task) {
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
        BlockIterator {
            receiver,
            worker_thread: Some(worker_thread),
            error_state,
        }
    }

    /// the worker threads are dispatched in this `new` constructor!
    pub fn from_range(db: &BitcoinDB, start: u32, end: u32) -> Self {
        if end <= start {
            BlockIterator::new(db, Vec::new())
        } else {
            let heights: Vec<u32> = (start..end).collect();
            BlockIterator::new(db, heights)
        }
    }
}

impl<TBlock> Iterator for BlockIterator<TBlock> {
    type Item = TBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(block) => Some(block),
            Err(_) => None,
        }
    }
}
