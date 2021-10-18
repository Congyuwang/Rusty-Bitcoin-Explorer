use crate::api::BitcoinDB;
use crate::iter::fetch_connected_async::{connect_outpoints, update_unspent_cache};
use crate::iter::util::get_task;
#[cfg(not(feature = "on-disk-utxo"))]
use crate::iter::util::VecMap;
use crate::parser::proto::connected_proto::BlockConnectable;
#[cfg(not(feature = "on-disk-utxo"))]
use crate::parser::proto::connected_proto::TxConnectable;
#[cfg(not(feature = "on-disk-utxo"))]
use hash_hasher::HashedMap;
#[cfg(feature = "on-disk-utxo")]
use log::{error, warn};
#[cfg(feature = "on-disk-utxo")]
use rocksdb::{Options, PlainTableFactoryOptions, SliceTransform, WriteOptions, DB};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
#[cfg(feature = "on-disk-utxo")]
use tempdir::TempDir;

const MAX_SIZE_FOR_THREAD: usize = 10;

/// iterate through blocks, and connecting outpoints.
pub struct ConnectedBlockIter<TBlock> {
    result_receivers: Vec<Receiver<(TBlock, usize)>>,
    result_order: Receiver<usize>,
    worker_thread: Option<Vec<JoinHandle<()>>>,
    #[cfg(feature = "on-disk-utxo")]
    rocks_db_path: Option<TempDir>,
    iterator_stopper: Arc<AtomicBool>,
    is_killed: bool,
    current_height: usize,
}

impl<TBlock> ConnectedBlockIter<TBlock>
where
    TBlock: 'static + BlockConnectable + Send,
{
    /// the worker threads are dispatched in this `new` constructor!
    pub fn new(db: &BitcoinDB, end: usize) -> Self {
        let cpus = num_cpus::get();
        let mut handles = Vec::with_capacity(cpus * 2);
        let iterator_stopper = Arc::new(AtomicBool::new(false));

        // UTXO cache
        #[cfg(not(feature = "on-disk-utxo"))]
        let unspent: Arc<
            Mutex<HashedMap<u128, Arc<Mutex<VecMap<<TBlock::Tx as TxConnectable>::TOut>>>>>,
        > = Arc::new(Mutex::new(HashedMap::default()));
        #[cfg(feature = "on-disk-utxo")]
        let cache_dir = {
            match TempDir::new("rocks_db") {
                Ok(tempdir) => tempdir,
                Err(e) => {
                    error!("failed to create rocksDB tempdir for UTXO: {}", e);
                    return ConnectedBlockIter::null();
                }
            }
        };
        #[cfg(feature = "on-disk-utxo")]
        let options = {
            let mut options = Options::default();

            // create table
            options.create_if_missing(true);

            // config to more jobs
            options.set_max_background_jobs(cpus as i32);

            // configure mem-table to a large value (1 GB)
            options.set_write_buffer_size(0x40000000);

            // configure l0 and l1 size, let them have the same size (4 GB)
            options.set_level_zero_file_num_compaction_trigger(4);
            options.set_max_bytes_for_level_base(0x100000000);

            // 256MB file size
            options.set_target_file_size_base(0x10000000);

            // use a smaller compaction multiplier
            options.set_max_bytes_for_level_multiplier(4.0);

            // use 8-byte prefix (2 ^ 64 is far enough for transaction counts)
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));

            // set to plain-table for better performance
            options.set_plain_table_factory(&PlainTableFactoryOptions {
                // 16 (compressed txid) + 4 (i32 out n)
                user_key_length: 20,
                bloom_bits_per_key: 10,
                hash_table_ratio: 0.75,
                index_sparseness: 16,
            });

            options
        };
        #[cfg(feature = "on-disk-utxo")]
        let unspent = Arc::new(match DB::open(&options, &cache_dir) {
            Ok(db) => db,
            Err(e) => {
                error!("failed to create temp rocksDB for UTXO: {}", e);
                return ConnectedBlockIter::null();
            }
        });

        // all tasks
        let heights = Arc::new(Mutex::new((0..end).collect::<VecDeque<usize>>()));

        // the channel for synchronizing cache update
        let (block_worker_register, block_order) = channel();
        let block_order = Arc::new(Mutex::new(block_order));
        let mut block_receivers = Vec::with_capacity(cpus);

        // output insertion threads
        for thread_number in 0..cpus {
            // block streams
            let (block_sender, block_receiver) = sync_channel(MAX_SIZE_FOR_THREAD);
            let block_receiver = Arc::new(Mutex::new(block_receiver));

            // clone resources
            let unspent = unspent.clone();
            let heights = heights.clone();
            let db = db.clone();
            let block_worker_register = block_worker_register.clone();
            let iterator_stopper = iterator_stopper.clone();

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
                    // stop acquiring new tasks
                    if iterator_stopper.load(Ordering::SeqCst) {
                        break;
                    }
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
                                &block_sender,
                            ) {
                                iterator_stopper.fetch_or(true, Ordering::SeqCst);
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
        let (result_register, result_order) = channel();

        // block_streams
        let mut result_receivers = Vec::with_capacity(cpus);

        // Ensure that right block order is provided by producer.
        // Update this variable on receiving block from producer.
        // Check if this variable equal to block height received.
        // Otherwise, stop producer.
        let current_height = Arc::new(AtomicUsize::new(0));

        // consume UTXO cache and produce output
        for thread_number in 0..cpus {
            // result streams
            let (result_sender, result_receiver) = sync_channel(MAX_SIZE_FOR_THREAD);

            let register = result_register.clone();
            let unspent = unspent.clone();
            let block_order = block_order.clone();
            let block_receivers = block_receivers.clone();
            let current_height = current_height.clone();
            let iterator_stopper = iterator_stopper.clone();

            let handle = thread::spawn(move || {
                loop {
                    // exclusive access to block receiver
                    let (blk, height) = {
                        let block_order_lock = block_order.lock().unwrap();
                        // receive thread_number for block receiver
                        // might block here, must drop all senders
                        if let Ok((height, worker_number)) = block_order_lock.recv() {
                            let lock = block_receivers.get(worker_number).unwrap().lock();
                            if height != current_height.load(Ordering::SeqCst) {
                                // some producer thread has failed to send block
                                // stop all producers
                                iterator_stopper.fetch_or(true, Ordering::SeqCst);
                                continue;
                            }
                            register.send(thread_number).unwrap();
                            match lock.unwrap().recv() {
                                Ok(blk) => {
                                    current_height.fetch_add(1, Ordering::SeqCst);
                                    (blk, height)
                                }
                                Err(_) => {
                                    // stop all producers
                                    iterator_stopper.fetch_or(true, Ordering::SeqCst);
                                    continue;
                                }
                            }
                        } else {
                            // all producers have stopped (block order registers dropped)
                            // may stop consumers
                            break;
                        }
                    };
                    // release receivers lock

                    if !connect_outpoints(&unspent, &result_sender, blk, height) {
                        // stop all producers
                        iterator_stopper.fetch_or(true, Ordering::SeqCst);
                        continue;
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
            #[cfg(feature = "on-disk-utxo")]
            rocks_db_path: Some(cache_dir),
            iterator_stopper,
            is_killed: false,
            current_height: 0,
        }
    }

    #[cfg(feature = "on-disk-utxo")]
    fn null() -> Self {
        let result_order = {
            let (_, receiver) = sync_channel(1);
            receiver
        };
        ConnectedBlockIter {
            result_receivers: Vec::new(),
            result_order,
            worker_thread: Some(Vec::new()),
            #[cfg(feature = "on-disk-utxo")]
            rocks_db_path: None,
            iterator_stopper: Arc::new(AtomicBool::new(false)),
            is_killed: true,
            current_height: 0,
        }
    }
}

impl<T> ConnectedBlockIter<T> {
    /// stop workers, flush tasks
    fn kill(&mut self) {
        if !self.is_killed {
            // stop threads from getting new tasks
            self.iterator_stopper.fetch_or(true, Ordering::SeqCst);
            // flush the remaining tasks in the channel
            loop {
                let _ = match self.result_order.recv() {
                    Ok(thread_number) => self.result_receivers.get(thread_number).unwrap().recv(),
                    // all consumers (connecting workers) have stopped
                    Err(_) => break,
                };
            }
            self.is_killed = true;
        }
    }
}

impl<TBlock> Iterator for ConnectedBlockIter<TBlock> {
    type Item = TBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_killed {
            return None;
        }
        match self.result_order.recv() {
            Ok(thread_number) => match self.result_receivers.get(thread_number).unwrap().recv() {
                Ok((block, height)) => {
                    // Some threads might have stopped first.
                    // while the remaining working threads produces wrong order.
                    if self.current_height != height {
                        self.kill();
                        return None;
                    }
                    self.current_height += 1;
                    Some(block)
                }
                // some worker have stopped
                Err(_) => {
                    self.kill();
                    None
                }
            },
            // all consumers (connecting workers) have stopped
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
        self.kill();
        self.join();
        #[cfg(feature = "on-disk-utxo")]
        if let Some(rocks_db_path) = self.rocks_db_path.take() {
            let result = DB::destroy(&Options::default(), &rocks_db_path);
            if let Err(e) = result {
                warn!("failed to destroy temp rocksDB, {}", e);
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "on-disk-utxo")]
mod test_empty {
    use crate::{ConnectedBlockIter, SConnectedBlock};

    #[test]
    fn test_empty() {
        let mut empty = ConnectedBlockIter::null();
        for _ in 0..100 {
            let b: Option<SConnectedBlock> = empty.next();
            assert!(b.is_none());
        }
    }
}
