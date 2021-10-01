use crate::api::BitcoinDB;
use crate::parser::blk_file::BlkFile;
use crate::parser::block_index::BlockIndex;

/// a light weighted data structure for storing unspent output
pub(crate) struct VecMap<T> {
    size: u16,
    inner: Box<[Option<T>]>,
}

impl<T> VecMap<T> {
    pub(crate) fn from_vec(slice: Box<[Option<T>]>) -> Self {
        VecMap {
            size: slice.len() as u16,
            inner: slice,
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub(crate) fn remove(&mut self, n: usize) -> Option<T> {
        let element = &mut self.inner[n];
        if let Some(_) = element {
            self.size -= 1;
        };
        element.take()
    }
}

/// python iterator implementation does not allow lifetime.
/// Thus, we must own the necessary resource for the iterator
/// to work for python.
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
