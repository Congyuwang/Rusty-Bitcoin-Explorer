pub mod api;
pub mod bitcoinparser;
pub mod iter;

use bitcoin::hashes::hex::{FromHex, ToHex};
use bitcoin::Txid;
use pyo3::prelude::*;
use pythonize::pythonize;
use rayon::prelude::*;
use std::path::Path;
use pyo3::Python;
use pyo3::PyIterProtocol;
use crate::bitcoinparser::proto::connected_proto::{SConnectedBlock, FConnectedBlock};
use crate::bitcoinparser::proto::simple_proto::SBlock;
use crate::bitcoinparser::proto::full_proto::FBlock;

#[pyclass]
struct BitcoinDB {
    db: api::BitcoinDB,
}

#[pymethods]
impl BitcoinDB {
    ///
    /// `path`: bitcoind --datadir option.
    /// `tx_index`: whether to try to open tx_index levelDB.
    ///
    #[new]
    fn new(path: &str, tx_index: bool) -> PyResult<Self> {
        let path = Path::new(path);
        match api::BitcoinDB::new(path, tx_index) {
            Ok(db) => Ok(BitcoinDB { db }),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_full(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_block_full(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_simple(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_block_simple(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_full_connected(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_block_full_connected(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_simple_connected(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_block_simple_connected(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, heights, /)")]
    fn get_block_full_batch(&self, heights: Vec<i32>) -> PyResult<Vec<String>> {
        let db = &self.db;
        Ok(heights
            .par_iter()
            .filter_map(|h| db.get_block_full(*h).ok())
            .filter_map(|blk| serde_json::to_string(&blk).ok())
            .collect())
    }

    #[pyo3(text_signature = "($self, heights, /)")]
    fn get_block_simple_batch(&self, heights: Vec<i32>) -> PyResult<Vec<String>> {
        let db = &self.db;
        Ok(heights
            .par_iter()
            .filter_map(|h| db.get_block_simple(*h).ok())
            .filter_map(|blk| serde_json::to_string(&blk).ok())
            .collect())
    }

    /// only get the block header (in memory, no disk access)
    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_header(&self, height: usize, py: Python) -> PyResult<PyObject> {
        match self.db.get_block_header(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_hash_from_height(&self, height: usize) -> PyResult<String> {
        match self.db.get_hash_from_height(height) {
            Ok(b) => Ok(b.to_hex()),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, hash, /)")]
    fn get_height_from_hash(&self, hash: String) -> PyResult<i32> {
        match self.db.get_height_from_hash(&hash) {
            Ok(h) => Ok(h),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, txid, /)")]
    fn get_height_from_txid(&self, txid: String) -> PyResult<i32> {
        if let Ok(txid) = Txid::from_hex(&txid) {
            match self.db.get_block_height_of_transaction(&txid) {
                Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
                Ok(h) => Ok(h),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err(
                "invalid txid format",
            ))
        }
    }

    #[pyo3(text_signature = "($self, txid, /)")]
    fn get_transaction_full(&self, txid: String, py: Python) -> PyResult<PyObject> {
        if let Ok(txid) = Txid::from_hex(&txid) {
            match self.db.get_transaction_full(&txid) {
                Ok(t) => Ok(pythonize(py, &t)?),
                Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err(
                "invalid txid format",
            ))
        }
    }

    #[pyo3(text_signature = "($self, txid, /)")]
    fn get_transaction_simple(&self, txid: String, py: Python) -> PyResult<PyObject> {
        if let Ok(txid) = Txid::from_hex(&txid) {
            match self.db.get_transaction_simple(&txid) {
                Ok(t) => Ok(pythonize(py, &t)?),
                Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err(
                "invalid txid format",
            ))
        }
    }

    #[pyo3(text_signature = "($self, txid, /)")]
    fn get_transaction_full_connected(&self, txid: String, py: Python) -> PyResult<PyObject> {
        if let Ok(txid) = Txid::from_hex(&txid) {
            match self.db.get_transaction_full_connected(&txid) {
                Ok(t) => Ok(pythonize(py, &t)?),
                Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err(
                "invalid txid format",
            ))
        }
    }

    #[pyo3(text_signature = "($self, txid, /)")]
    fn get_transaction_simple_connected(&self, txid: String, py: Python) -> PyResult<PyObject> {
        if let Ok(txid) = Txid::from_hex(&txid) {
            match self.db.get_transaction_simple_connected(&txid) {
                Ok(t) => Ok(pythonize(py, &t)?),
                Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err(
                "invalid txid format",
            ))
        }
    }

    #[pyo3(text_signature = "($self, start, stop, /)")]
    fn iter_block_full_seq(&self, start: u32, stop: u32) -> PyResult<FBlockIteratorSequential> {
        FBlockIteratorSequential::new(&self.db, start, stop)
    }

    #[pyo3(text_signature = "($self, start, stop, /)")]
    fn iter_block_simple_seq(&self, start: u32, stop: u32) -> PyResult<SBlockIteratorSequential> {
        SBlockIteratorSequential::new(&self.db, start, stop)
    }

    #[pyo3(text_signature = "($self, start, stop, /)")]
    fn iter_block_full_array(&self, heights: Vec<u32>) -> PyResult<FBlockIteratorArray> {
        Ok(FBlockIteratorArray::new(&self.db, heights))
    }

    #[pyo3(text_signature = "($self, start, stop, /)")]
    fn iter_block_simple_array(&self, heights: Vec<u32>) -> PyResult<SBlockIteratorArray> {
        Ok(SBlockIteratorArray::new(&self.db, heights))
    }

    #[pyo3(text_signature = "($self, stop, /)")]
    fn iter_block_full_connected(&self, stop: u32) -> PyResult<FConnectedBlockIterator> {
        FConnectedBlockIterator::new(&self.db, stop)
    }

    #[pyo3(text_signature = "($self, stop, /)")]
    fn iter_block_simple_connected(&self, stop: u32) -> PyResult<SConnectedBlockIterator> {
        SConnectedBlockIterator::new(&self.db, stop)
    }

    #[pyo3(text_signature = "($self, /)")]
    fn get_max_height(&self) -> usize {
        self.db.block_index.records.len()
    }

    #[staticmethod]
    #[pyo3(text_signature = "($self, script_pub_key, /)")]
    fn parse_script(script_pub_key: String, py: Python) -> PyResult<PyObject> {
        let script = api::parse_script(&script_pub_key);
        match script {
            Ok(script) => Ok(pythonize(py, &script)?),
            Err(_) => Err(pyo3::exceptions::PyException::new_err(
                "failed to parse script_pub_key",
            )),
        }
    }
}

#[pyclass]
struct FBlockIteratorSequential {
    iter: iter::FBlockIteratorSequential,
}

impl FBlockIteratorSequential {
    fn new(db: &api::BitcoinDB, start: u32, end: u32) -> PyResult<FBlockIteratorSequential> {
        let inner_iter = iter::FBlockIteratorSequential::new(db, start, end);
        match inner_iter {
            Ok(iter) => Ok(FBlockIteratorSequential {
                iter
            }),
            Err(e) => {
                Err(pyo3::exceptions::PyException::new_err(e.to_string()))
            }
        }
    }
}

#[pyproto]
impl PyIterProtocol for FBlockIteratorSequential {

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        let option_block: Option<FBlock> = slf.iter.next();
        if let Some(output) = option_block {
            let gil_guard = Python::acquire_gil();
            let py = gil_guard.python();
            if let Ok(py_obj) = pythonize(py, &output) {
                Some(py_obj)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[pyclass]
struct SBlockIteratorSequential {
    iter: iter::SBlockIteratorSequential,
}

impl SBlockIteratorSequential {
    fn new(db: &api::BitcoinDB, start: u32, end: u32) -> PyResult<SBlockIteratorSequential> {
        let inner_iter = iter::SBlockIteratorSequential::new(db, start, end);
        match inner_iter {
            Ok(iter) => Ok(SBlockIteratorSequential {
                iter
            }),
            Err(e) => {
                Err(pyo3::exceptions::PyException::new_err(e.to_string()))
            }
        }
    }
}

#[pyproto]
impl PyIterProtocol for SBlockIteratorSequential {

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        let option_block: Option<SBlock> = slf.iter.next();
        if let Some(output) = option_block {
            let gil_guard = Python::acquire_gil();
            let py = gil_guard.python();
            if let Ok(py_obj) = pythonize(py, &output) {
                Some(py_obj)
            } else {
                None
            }
        } else {
            None
        }
    }
}


#[pyclass]
struct FBlockIteratorArray {
    iter: iter::FBlockIteratorArray,
}

impl FBlockIteratorArray {
    fn new(db: &api::BitcoinDB, heights: Vec<u32>) -> FBlockIteratorArray {
        FBlockIteratorArray {
            iter: iter::FBlockIteratorArray::new(db, heights)
        }
    }
}

#[pyproto]
impl PyIterProtocol for FBlockIteratorArray {

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        let option_block: Option<FBlock> = slf.iter.next();
        if let Some(output) = option_block {
            let gil_guard = Python::acquire_gil();
            let py = gil_guard.python();
            if let Ok(py_obj) = pythonize(py, &output) {
                Some(py_obj)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[pyclass]
struct SBlockIteratorArray {
    iter: iter::SBlockIteratorArray,
}

impl SBlockIteratorArray {
    fn new(db: &api::BitcoinDB, heights: Vec<u32>) -> SBlockIteratorArray {
        SBlockIteratorArray {
            iter: iter::SBlockIteratorArray::new(db, heights)
        }
    }
}

#[pyproto]
impl PyIterProtocol for SBlockIteratorArray {

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        let option_block: Option<SBlock> = slf.iter.next();
        if let Some(output) = option_block {
            let gil_guard = Python::acquire_gil();
            let py = gil_guard.python();
            if let Ok(py_obj) = pythonize(py, &output) {
                Some(py_obj)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[pyclass]
struct FConnectedBlockIterator {
    iter: iter::FConnectedBlockIterator,
}

impl FConnectedBlockIterator {
    fn new(db: &api::BitcoinDB, end: u32) -> PyResult<FConnectedBlockIterator> {
        let inner_iter = iter::FConnectedBlockIterator::new(db, end);
        match inner_iter {
            Ok(iter) => Ok(FConnectedBlockIterator {
                iter
            }),
            Err(e) => {
                Err(pyo3::exceptions::PyException::new_err(e.to_string()))
            }
        }
    }
}

#[pyproto]
impl PyIterProtocol for FConnectedBlockIterator {

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        let option_block: Option<FConnectedBlock> = slf.iter.next();
        if let Some(output) = option_block {
            let gil_guard = Python::acquire_gil();
            let py = gil_guard.python();
            if let Ok(py_obj) = pythonize(py, &output) {
                Some(py_obj)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[pyclass]
struct SConnectedBlockIterator {
    iter: iter::SConnectedBlockIterator,
}

impl SConnectedBlockIterator {
    fn new(db: &api::BitcoinDB, end: u32) -> PyResult<SConnectedBlockIterator> {
        let inner_iter = iter::SConnectedBlockIterator::new(db, end);
        match inner_iter {
            Ok(iter) => Ok(SConnectedBlockIterator {
                iter
            }),
            Err(e) => {
                Err(pyo3::exceptions::PyException::new_err(e.to_string()))
            }
        }
    }
}

#[pyproto]
impl PyIterProtocol for SConnectedBlockIterator {

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        let option_block: Option<SConnectedBlock> = slf.iter.next();
        if let Some(output) = option_block {
            let gil_guard = Python::acquire_gil();
            let py = gil_guard.python();
            if let Ok(py_obj) = pythonize(py, &output) {
                Some(py_obj)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[pymodule]
fn bitcoinquery(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<BitcoinDB>()?;
    Ok(())
}
