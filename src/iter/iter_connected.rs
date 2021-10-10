use crate::api::BitcoinDB;
use crate::iter::fetch_connected_async::{connect_outpoints, update_unspent_cache};
use crate::iter::util::{get_task, DBCopy};
use crate::parser::proto::connected_proto::{BlockConnectable};
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
#[cfg(not(feature = "on-disk-utxo"))] use crate::iter::util::VecMap;
#[cfg(not(feature = "on-disk-utxo"))] use crate::parser::proto::connected_proto::TxConnectable;
#[cfg(not(feature = "on-disk-utxo"))] use hash_hasher::HashedMap;
#[cfg(feature = "on-disk-utxo")] use tempdir::TempDir;
#[cfg(feature = "on-disk-utxo")] use rocksdb::{Options, DB};

/// iterate through blocks, and connecting outpoints.
pub struct ConnectedBlockIter<TBlock> {
    result_receivers: Vec<Receiver<TBlock>>,
    result_order: Receiver<usize>,
    worker_thread: Option<Vec<JoinHandle<()>>>,
    error_state: Arc<AtomicBool>,
    #[cfg(feature = "on-disk-utxo")]
    rocks_db_path: TempDir,
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

        // UTXO cache
        #[cfg(not(feature = "on-disk-utxo"))]
        let unspent: Arc<
            Mutex<HashedMap<u128, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>,
        > = Arc::new(Mutex::new(HashedMap::default()));
        #[cfg(feature = "on-disk-utxo")]
        let cache_dir = TempDir::new("rocks_db").expect("failed to create rocksdb temp dir");
        #[cfg(feature = "on-disk-utxo")]
        let options = {
            let mut options = Options::default();
            options.create_if_missing(true);
            options
        };
        #[cfg(feature = "on-disk-utxo")]
        let unspent = Arc::new(Mutex::new(DB::open(&options, &cache_dir).expect("failed to open rocksdb")));

        // all tasks
        let heights = Arc::new(Mutex::new((0..end).collect::<VecDeque<u32>>()));

        // the channel for synchronizing cache update
        let (block_worker_register, block_order) = sync_channel(cpus * 10);
        let block_order = Arc::new(Mutex::new(block_order));
        let mut block_receivers = Vec::with_capacity(cpus);

        // output insertion threads
        for thread_number in 0..cpus {
            // block streams
            let (block_sender, block_receiver) = channel();
            let block_receiver = Arc::new(Mutex::new(block_receiver));

            // clone resources
            let unspent = unspent.clone();
            let error_state = error_state.clone();
            let heights = heights.clone();
            let db = db.clone();
            let block_worker_register = block_worker_register.clone();

            // output cache insertion workers
            let handle = thread::spawn(move || {
                loop {
                    match get_task(&heights, &block_worker_register, thread_number) {
                        // finish
                        None => break,
                        Some(height) => {
                            if !update_unspent_cache::<TBlock>(
                                &unspent,
                                &db,
                                height,
                                &error_state,
                                &block_sender,
                            ) {
                                break;
                            }
                        }
                    }
                }
            });
            block_receivers.push(block_receiver);
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
            let block_order = block_order.clone();
            let block_receivers = block_receivers.clone();

            let handle = thread::spawn(move || {
                loop {
                    // exclusive access to block receiver
                    let blk = {
                        let block_order_lock = block_order.lock().unwrap();
                        // receive thread_number for block receiver
                        if let Ok(worker_number) = block_order_lock.recv() {
                            let lock = block_receivers.get(worker_number).unwrap().lock();
                            register.send(thread_number).unwrap();
                            match lock.unwrap().recv() {
                                Ok(blk) => blk,
                                Err(_) => break,
                            }
                        } else {
                            break;
                        }
                    };
                    // release receivers lock

                    if !connect_outpoints(&unspent, &error_state, &result_sender, blk) {
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
            #[cfg(feature = "on-disk-utxo")]
            rocks_db_path: cache_dir,
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
        #[cfg(feature = "on-disk-utxo")]
        DB::destroy(&Options::default(), &self.rocks_db_path).unwrap();
    }
}
