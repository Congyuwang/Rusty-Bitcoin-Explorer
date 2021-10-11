use crate::api::BitcoinDB;
use crate::iter::fetch_connected_async::{connect_outpoints, update_unspent_cache};
#[cfg(not(feature = "on-disk-utxo"))]
use crate::iter::util::VecMap;
use crate::iter::util::{get_task, DBCopy};
use crate::parser::proto::connected_proto::BlockConnectable;
#[cfg(not(feature = "on-disk-utxo"))]
use crate::parser::proto::connected_proto::TxConnectable;
#[cfg(not(feature = "on-disk-utxo"))]
use hash_hasher::HashedMap;
#[cfg(feature = "on-disk-utxo")]
use rocksdb::{MemtableFactory, PlainTableFactoryOptions, SliceTransform, WriteOptions, Options, DB};
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
#[cfg(feature = "on-disk-utxo")]
use tempdir::TempDir;

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

            // create table
            options.create_if_missing(true);

            // configure mem-table to a large value
            options.set_write_buffer_size(1024 * 1024 * 1024 * 2);
            options.set_max_write_buffer_number(4);

            // configure l0 and l1 size, let them have the same size
            options.set_level_zero_file_num_compaction_trigger(4);
            options.set_max_bytes_for_level_base(1024 * 1024 * 1024 * 2 * 4);

            // set larger target file size (for potentially faster compaction)
            options.set_target_file_size_base(256 * 1024 * 1024);

            // use a smaller compaction multiplier
            options.set_max_bytes_for_level_multiplier(2.0);

            // use 8-byte prefix
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));

            // set to plain-table for better performance
            options.set_plain_table_factory(&PlainTableFactoryOptions {
                user_key_length: 20,
                // we don't need bloom filter as our `get` and `delete` guarantees to hit.
                bloom_bits_per_key: 0,
                hash_table_ratio: 0.75,
                index_sparseness: 16,
            });

            options
        };
        #[cfg(feature = "on-disk-utxo")]
        let unspent = Arc::new(Mutex::new(
            DB::open(&options, &cache_dir).expect("failed to open rocksdb"),
        ));

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

            // write without WAL
            #[cfg(feature = "on-disk-utxo")]
            let write_options = {
                let mut opt = WriteOptions::default();
                opt.disable_wal(true);
                opt
            };

            // output cache insertion workers
            let handle = thread::spawn(move || {
                loop {
                    match get_task(&heights, &block_worker_register, thread_number) {
                        // finish
                        None => break,
                        Some(height) => {
                            if !update_unspent_cache::<TBlock>(
                                &unspent,
                                #[cfg(feature = "on-disk-utxo")]
                                &write_options,
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
