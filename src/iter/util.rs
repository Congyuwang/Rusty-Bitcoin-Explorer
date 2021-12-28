///
/// a light weighted data structure for storing unspent output
///
#[cfg(not(feature = "on-disk-utxo"))]
pub(crate) struct VecMap<T> {
    size: u32,
    inner: Box<[Option<Box<T>>]>,
}

#[cfg(not(feature = "on-disk-utxo"))]
impl<T> VecMap<T> {
    #[inline(always)]
    pub(crate) fn from_vec(slice: Box<[Option<Box<T>>]>) -> Self {
        VecMap {
            size: slice.len() as u32,
            inner: slice,
        }
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, n: usize) -> Option<Box<T>> {
        let element = &mut self.inner[n];
        if let Some(_) = element {
            self.size -= 1;
        };
        element.take()
    }
}

#[cfg(test)]
#[cfg(not(feature = "on-disk-utxo"))]
mod test_vec_map {
    use crate::api::STxOut;
    use crate::iter::util::VecMap;
    use bitcoin::TxOut;

    #[test]
    fn test_vec_map() {
        let mut vec: VecMap<STxOut> = VecMap::from_vec(
            vec![
                Some(Box::new(TxOut::default().into())),
                Some(Box::new(TxOut::default().into())),
                Some(Box::new(TxOut::default().into())),
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
