//! ## Synchronization
//! - When each thread fetch a task, it registers its thread ID (thread_num)
//!   in a mpsc channel. When consumer consumes, it fetch from this mpsc
//!   channel to see which thread data stream to fetch from. This ensures
//!   the output are in right order.
//! - An additional task number (current, or current_height) is updated
//!   when output is received, it is compared to the output's task number
//!   to ensure that output are received in the right order.
//! - If order is incorrect, some one of the threads have stopped due
//!   to exception. This will stop iterator output, and stop all producers
//!   from fetching tasks, and attempt to flush output until all workers
//!   have stopped.
//!
//! ## Error handling
//! - When any exception occurs, stop producers from fetching new task.
//! - Stop consumers only after all producers have stopped
//!   (otherwise producers might block consumers from sending)
//! - Before dropping the structure, stop all producers from fetching tasks,
//!   and flush all remaining tasks.
//!
use std::iter::Enumerate;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

const MAX_SIZE_FOR_THREAD: usize = 10;

/// iterate through blocks according to array index.
pub struct ParIter<R> {
    receivers: Vec<Receiver<R>>,
    // Receiver<(task_number, thread)>
    task_order: Receiver<(usize, usize)>,
    current: usize,
    worker_thread: Option<Vec<JoinHandle<()>>>,
    iterator_stopper: Arc<AtomicBool>,
    is_killed: bool,
}

impl<R> ParIter<R>
where
    R: Send + 'static,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new<T, TL, F>(tasks: TL, task_executor: F) -> Self
    where
        F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
        T: Send,
        TL: Send + IntoIterator<Item = T>,
        <TL as IntoIterator>::IntoIter: Send + 'static,
    {
        let cpus = num_cpus::get();
        let iterator_stopper = Arc::new(AtomicBool::new(false));
        // worker master
        let (task_register, task_order) = channel();
        let tasks = Arc::new(Mutex::new(tasks.into_iter().enumerate()));
        let mut handles = Vec::with_capacity(cpus);
        let mut receivers = Vec::with_capacity(cpus);
        for thread_number in 0..cpus {
            let (sender, receiver) = sync_channel(MAX_SIZE_FOR_THREAD);
            let task = tasks.clone();
            let register = task_register.clone();
            let iterator_stopper = iterator_stopper.clone();
            let task_executor = task_executor.clone();

            // workers
            let handle = thread::spawn(move || {
                loop {
                    if iterator_stopper.load(Ordering::SeqCst) {
                        break;
                    }
                    match get_task(&task, &register, thread_number) {
                        // finish
                        None => break,
                        Some(task) => match task_executor(task) {
                            Ok(blk) => {
                                sender.send(blk).unwrap();
                            }
                            Err(_) => {
                                iterator_stopper.fetch_or(true, Ordering::SeqCst);
                                break;
                            }
                        },
                    }
                }
            });
            receivers.push(receiver);
            handles.push(handle);
        }

        ParIter {
            receivers,
            task_order,
            current: 0,
            worker_thread: Some(handles),
            iterator_stopper,
            is_killed: false,
        }
    }
}

impl<R> ParIter<R> {
    /// stop workers, flush tasks
    pub fn kill(&mut self) {
        if !self.is_killed {
            // stop threads from getting new tasks
            self.iterator_stopper.fetch_or(true, Ordering::SeqCst);
            // flush the remaining tasks in the channel
            loop {
                let _ = match self.task_order.recv() {
                    Ok((_, thread_number)) => self.receivers.get(thread_number).unwrap().recv(),
                    // all workers have stopped
                    Err(_) => break,
                };
            }
            self.is_killed = true;
        }
    }
}

fn get_task<T, TL>(
    tasks: &Arc<Mutex<Enumerate<TL>>>,
    register: &Sender<(usize, usize)>,
    thread_number: usize,
) -> Option<T>
where
    T: Send,
    TL: Iterator<Item = T>,
{
    // lock task list
    let mut task = tasks.lock().unwrap();
    let next_task = task.next();
    // register task stealing
    match next_task {
        Some((task_number, task)) => {
            register.send((task_number, thread_number)).unwrap();
            Some(task)
        }
        None => None,
    }
}

impl<R> Iterator for ParIter<R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_killed {
            return None;
        }
        match self.task_order.recv() {
            Ok((task_number, thread_number)) => {
                // Some threads might have stopped first.
                // while the remaining working threads produces wrong order.
                if task_number != self.current {
                    self.kill();
                    return None;
                }

                match self.receivers.get(thread_number).unwrap().recv() {
                    Ok(block) => {
                        self.current += 1;
                        Some(block)
                    }
                    // some worker have stopped
                    Err(_) => {
                        self.kill();
                        None
                    }
                }
            }
            // all workers have stopped
            Err(_) => None,
        }
    }
}

impl<R> ParIter<R> {
    fn join(&mut self) {
        for handle in self.worker_thread.take().unwrap() {
            handle.join().unwrap()
        }
    }
}

impl<R> Drop for ParIter<R> {
    // attempt to stop the worker threads
    fn drop(&mut self) {
        self.kill();
        self.join();
    }
}

#[cfg(test)]
mod test_par_iter {
    use crate::iter::iter::ParIter;
    use std::sync::Arc;

    #[test]
    fn par_iter() {
        let resource_captured = Arc::new(vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3]);
        let tasks = 0..resource_captured.len();

        let resource_clone = resource_captured.clone();
        let par_iter = ParIter::new(tasks, move |a| {
            Ok(resource_captured.get(a).unwrap().to_owned())
        });

        let mut pos = 0;
        for i in par_iter {
            assert_eq!(*resource_clone.get(pos).unwrap(), i);
            pos += 1;
        }
    }
}
