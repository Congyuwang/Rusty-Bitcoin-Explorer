use crate::iter::util::DBCopy;
use bitcoin::Block;
use std::borrow::BorrowMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};

pub(crate) struct Task<T> {
    pub(crate) task_number: u32,
    pub(crate) height: u32,
    pub(crate) output_number: Arc<(Mutex<u32>, Condvar)>,
    pub(crate) sender: SyncSender<T>,
    pub(crate) error_state: Arc<AtomicBool>,
}

///
/// fetch_block, thread safe
///
pub(crate) fn fetch_block<T>(db: &DBCopy, task: Task<T>) -> bool
where
    T: From<Block>,
{
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
                    task.sender.send(T::from(blk)).unwrap();
                    *output_number += 1;
                    cond.notify_all();
                }
                true
            }
            Err(_) => {
                mutate_error_state(task);
                return false;
            }
        }
    } else {
        // set error_state to true
        mutate_error_state(task);
        return false;
    }
}

/// wait for prior tasks, change error state, move to later tasks
fn mutate_error_state<T>(mut task: Task<T>) {
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
