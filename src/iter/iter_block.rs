use crate::api::BitcoinDB;
use crate::iter::fetch_async::{fetch_block, Task};
use crate::iter::util::DBCopy;
use bitcoin::Block;
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// iterate through blocks according to array index.
pub struct BlockIter<TBlock> {
    receivers: Vec<Receiver<TBlock>>,
    task_order: Receiver<usize>,
    worker_thread: Option<Vec<JoinHandle<()>>>,
    error_state: Arc<AtomicBool>,
}

impl<T> BlockIter<T> {
    fn join(&mut self) {
        for handle in self.worker_thread.take().unwrap() {
            handle.join().unwrap()
        }
    }
}

impl<T> Drop for BlockIter<T> {
    // attempt to stop the worker threads
    fn drop(&mut self) {
        {
            let err = self.error_state.borrow_mut();
            err.fetch_or(true, Ordering::SeqCst);
        }
        self.join();
    }
}

impl<TBlock> BlockIter<TBlock>
where
    TBlock: From<Block> + Send + 'static,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> Self {
        let cpus = num_cpus::get();
        let error_state = Arc::new(AtomicBool::new(false));
        let db = DBCopy::from_bitcoin_db(db);
        // worker master
        let error_state_copy = error_state.clone();
        let (task_register, task_order) = channel();
        let mut tasks: VecDeque<Task> = VecDeque::with_capacity(heights.len());
        for height in heights {
            tasks.push_back(Task {
                height,
                error_state: error_state_copy.clone(),
            })
        }

        let tasks = Arc::new(Mutex::new(tasks));
        let mut handles = Vec::with_capacity(cpus);
        let mut receivers = Vec::with_capacity(cpus);
        for thread_number in 0..cpus {
            let (sender, receiver) = sync_channel(10);
            receivers.push(receiver);
            let task = tasks.clone();
            let db_copy = db.clone();
            let register = task_register.clone();
            // actual worker
            let handle = thread::spawn(move || {
                loop {
                    let task = {
                        let mut task = task.lock().unwrap();
                        if task.front().is_some() {
                            register.send(thread_number).unwrap();
                        }
                        task.pop_front()
                        // drop mutex immediately
                    };
                    match task {
                        // finish
                        None => break,
                        Some(task) => {
                            if !fetch_block(&db_copy, task, sender.clone()) {
                                // on error
                                break;
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        BlockIter {
            receivers,
            task_order,
            worker_thread: Some(handles),
            error_state,
        }
    }

    /// the worker threads are dispatched in this `new` constructor!
    pub fn from_range(db: &BitcoinDB, start: u32, end: u32) -> Self {
        if end <= start {
            BlockIter::new(db, Vec::new())
        } else {
            let heights: Vec<u32> = (start..end).collect();
            BlockIter::new(db, heights)
        }
    }
}

impl<TBlock> Iterator for BlockIter<TBlock> {
    type Item = TBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.task_order.recv() {
            Ok(thread_number) => {
                match self.receivers.get(thread_number).unwrap().recv() {
                    Ok(block) => Some(block),
                    Err(_) => None
                }
            },
            Err(_) => None,
        }
    }
}
