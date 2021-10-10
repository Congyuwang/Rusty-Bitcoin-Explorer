use crate::api::BitcoinDB;
use crate::parser::blk_file::BlkFile;
use crate::parser::block_index::BlockIndex;
use ahash::AHasher;
use bitcoin::Txid;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};

///
/// Key compression
///
pub(crate) trait Compress {
    fn compress(&self) -> u128;
}

impl Compress for Txid {
    #[inline]
    fn compress(&self) -> u128 {
        let mut hasher_0 = AHasher::new_with_keys(54321, 12345);
        let mut hasher_1 = AHasher::new_with_keys(12345, 54321);
        self.hash(&mut hasher_0);
        self.hash(&mut hasher_1);
        let hash_0 = (hasher_0.finish() as u128) << 64;
        let hash_1 = hasher_1.finish() as u128;
        hash_0 ^ hash_1
    }
}

///
/// a light weighted data structure for storing unspent output
///
#[cfg(feature = "in-memory-utxo")]
pub(crate) struct VecMap<T> {
    size: u16,
    inner: Box<[Option<T>]>,
}

#[cfg(feature = "in-memory-utxo")]
impl<T> VecMap<T> {
    #[inline(always)]
    pub(crate) fn from_vec(slice: Box<[Option<T>]>) -> Self {
        VecMap {
            size: slice.len() as u16,
            inner: slice,
        }
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, n: usize) -> Option<T> {
        let element = &mut self.inner[n];
        if let Some(_) = element {
            self.size -= 1;
        };
        element.take()
    }
}

///
/// Each thread owns the necessary resource for better performance.
///
#[derive(Clone)]
pub(crate) struct DBCopy {
    pub block_index: BlockIndex,
    pub blk_file: BlkFile,
}

impl DBCopy {
    pub(crate) fn from_bitcoin_db(db: &BitcoinDB) -> DBCopy {
        DBCopy {
            block_index: db.block_index.clone(),
            blk_file: db.blk_file.clone(),
        }
    }
}

///
/// Utility function for work stealing.
/// Exclusive access to task list.
///
#[inline(always)]
pub(crate) fn get_task<T>(
    tasks: &Arc<Mutex<VecDeque<T>>>,
    register: &SyncSender<usize>,
    thread_number: usize,
) -> Option<T> {
    // lock task list
    let mut task = tasks.lock().unwrap();
    let next_height = task.pop_front();
    // register task stealing
    if next_height.is_some() {
        register.send(thread_number).unwrap();
    }
    next_height
}

#[cfg(test)]
mod test_vec_map {
    use crate::api::STxOut;
    use crate::iter::util::VecMap;
    use bitcoin::TxOut;

    #[test]
    fn test_vec_map() {
        let mut vec: VecMap<STxOut> = VecMap::from_vec(
            vec![
                Some(TxOut::default().into()),
                Some(TxOut::default().into()),
                Some(TxOut::default().into()),
            ]
            .into_boxed_slice(),
        );
        assert_eq!(vec.size, 3);
        assert!(vec.remove(1).is_some());
        assert_eq!(vec.size, 2);
        assert!(vec.remove(1).is_none());
        assert_eq!(vec.size, 2);
        assert!(vec.remove(0).is_some());
        assert_eq!(vec.size, 1);
        assert!(vec.remove(0).is_none());
        assert_eq!(vec.size, 1);
        assert!(!vec.is_empty());
        assert!(vec.remove(2).is_some());
        assert!(vec.is_empty());
    }
}
