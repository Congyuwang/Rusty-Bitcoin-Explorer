use crate::iter::util::DBCopy;
use bitcoin::Block;
use std::borrow::BorrowMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

pub(crate) struct Task {
    pub(crate) height: u32,
    pub(crate) error_state: Arc<AtomicBool>,
}

///
/// fetch_block, thread safe
///
pub(crate) fn fetch_block<T>(db: &DBCopy, task: Task, sender: &SyncSender<T>) -> bool
where
    T: From<Block>,
{
    if let Some(index) = db.block_index.records.get(task.height as usize) {
        match db.blk_file.read_block(index.n_file, index.n_data_pos) {
            Ok(blk) => {
                if task.error_state.load(Ordering::SeqCst) {
                    return false;
                }
                sender.send(T::from(blk)).unwrap();
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
fn mutate_error_state(mut task: Task) {
    let err = task.error_state.borrow_mut();
    err.fetch_or(true, Ordering::SeqCst);
}
