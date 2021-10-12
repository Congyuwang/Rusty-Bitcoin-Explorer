use crate::api::BitcoinDB;
use crate::iter::util::get_task;
use bitcoin::Block;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

const MAX_SIZE_FOR_THREAD: usize = 10;

/// iterate through blocks according to array index.
pub struct BlockIter<TBlock> {
    receivers: Vec<Receiver<TBlock>>,
    task_order: Receiver<usize>,
    worker_thread: Option<Vec<JoinHandle<()>>>,
    iterator_stopper: Arc<AtomicBool>,
}

impl<TBlock> BlockIter<TBlock>
where
    TBlock: From<Block> + Send + 'static,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, heights: Vec<u32>) -> Self {
        let cpus = num_cpus::get();
        let iterator_stopper = Arc::new(AtomicBool::new(false));
        // worker master
        let (task_register, task_order) = channel();
        let tasks: VecDeque<u32> = heights.into_iter().collect();
        let tasks = Arc::new(Mutex::new(tasks));
        let mut handles = Vec::with_capacity(cpus);
        let mut receivers = Vec::with_capacity(cpus);
        for thread_number in 0..cpus {
            let (sender, receiver) = sync_channel(MAX_SIZE_FOR_THREAD);
            let task = tasks.clone();
            let register = task_register.clone();
            let iterator_stopper = iterator_stopper.clone();
            let db = db.clone();

            // workers
            let handle = thread::spawn(move || {
                loop {
                    if iterator_stopper.load(Ordering::SeqCst) {
                        break;
                    }
                    match get_task(&task, &register, thread_number) {
                        // finish
                        None => break,
                        Some(task) => {
                            if !fetch_block(&db, task, &sender) {
                                // on error
                                break;
                            }
                        }
                    }
                }
            });
            receivers.push(receiver);
            handles.push(handle);
        }

        BlockIter {
            receivers,
            task_order,
            worker_thread: Some(handles),
            iterator_stopper,
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
            Ok(thread_number) => match self.receivers.get(thread_number).unwrap().recv() {
                Ok(block) => Some(block),
                Err(_) => None,
            },
            Err(_) => None,
        }
    }
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
        // stop threads from getting new tasks
        self.iterator_stopper.fetch_or(true, Ordering::SeqCst);
        // clear the remaining tasks in the channel
        loop {
            if self.next().is_none() {
                break;
            }
        }
        self.join();
    }
}

///
/// fetch_block, thread safe
///
#[inline]
pub(crate) fn fetch_block<T>(db: &BitcoinDB, height: u32, sender: &SyncSender<T>) -> bool
where
    T: From<Block>,
{
    match db.get_block::<T>(height as i32) {
        Ok(blk) => {
            sender.send(blk).unwrap();
            true
        }
        Err(_) => {
            return false;
        }
    }
}
