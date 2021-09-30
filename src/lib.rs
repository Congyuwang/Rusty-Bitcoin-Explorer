//!
//! # Introduction
//!
//! This library is designed for efficient and massive deserialization
//! of the binary Bitcoin Core block files.
//!
//! It decodes all transactions, addresses, script types,
//! connects outpoints of inputs to outputs, to figure out
//! input addresses.
//!
//! This library allows efficient and versatile reading of all
//! bitcoin transaction records. This is good for analysis and research on
//! bitcoin trading behaviour.
//!
//! ## Caveat
//!
//! Currently it only supports all standard script types.
//!
//! # Example
//!
//! ```rust
//! use bitcoin_explorer::BitcoinDB;
//! use std::path::Path;
//!
//! let path = Path::new("/Users/me/bitcoin").unwrap();
//!
//! // launch without reading txindex
//! let db = BitcoinDB::new(path, false).unwrap();
//!
//! // launch attempting to read txindex
//! let db = BitcoinDB::new(path, true).unwrap();
//! ```
//!

pub mod api;
pub mod iter;
pub mod parser;

#[doc(inline)]
pub use crate::api::*;
use pyo3::prelude::*;
use pyo3::PyIterProtocol;
use pyo3::Python;
use pythonize::pythonize;
use std::path::Path;

#[pyclass(name = "BitcoinDB")]
struct BitcoinDBPy {
    db: api::BitcoinDB,
}

#[pymethods]
impl BitcoinDBPy {
    #[new]
    fn new(path: &str, tx_index: bool) -> PyResult<Self> {
        let path = Path::new(path);
        match api::BitcoinDB::new(path, tx_index) {
            Ok(db) => Ok(BitcoinDBPy { db }),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_full(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_block::<FBlock>(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_simple(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_block::<SBlock>(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_full_connected(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_connected_block::<FConnectedBlock>(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_simple_connected(&self, height: i32, py: Python) -> PyResult<PyObject> {
        match self.db.get_connected_block::<SConnectedBlock>(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_header(&self, height: usize, py: Python) -> PyResult<PyObject> {
        match self.db.get_header(height) {
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
            match self.db.get_height_of_transaction(&txid) {
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
            match self.db.get_transaction::<FTransaction>(&txid) {
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
            match self.db.get_transaction::<STransaction>(&txid) {
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
            match self
                .db
                .get_connected_transaction::<FConnectedTransaction>(&txid)
            {
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
            match self
                .db
                .get_connected_transaction::<SConnectedTransaction>(&txid)
            {
                Ok(t) => Ok(pythonize(py, &t)?),
                Err(e) => Err(pyo3::exceptions::PyException::new_err(e.to_string())),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err(
                "invalid txid format",
            ))
        }
    }

    #[pyo3(text_signature = "($self, stop, /)")]
    fn iter_block_full_arr(&self, heights: Vec<u32>) -> PyResult<FBlockIteratorArray> {
        Ok(FBlockIteratorArray::new(&self.db, heights))
    }

    #[pyo3(text_signature = "($self, stop, /)")]
    fn iter_block_simple_arr(&self, heights: Vec<u32>) -> PyResult<SBlockIteratorArray> {
        Ok(SBlockIteratorArray::new(&self.db, heights))
    }

    #[pyo3(text_signature = "($self, start, stop, /)")]
    fn iter_block_full_seq(&self, start: u32, stop: u32) -> PyResult<FBlockIteratorSequential> {
        Ok(FBlockIteratorSequential::new(&self.db, start, stop))
    }

    #[pyo3(text_signature = "($self, start, stop, /)")]
    fn iter_block_simple_seq(&self, start: u32, stop: u32) -> PyResult<SBlockIteratorSequential> {
        Ok(SBlockIteratorSequential::new(&self.db, start, stop))
    }

    #[pyo3(text_signature = "($self, stop, /)")]
    fn iter_block_full_connected(&self, stop: u32) -> PyResult<FConnectedBlockIterator> {
        Ok(FConnectedBlockIterator::new(&self.db, stop))
    }

    #[pyo3(text_signature = "($self, stop, /)")]
    fn iter_block_simple_connected(&self, stop: u32) -> PyResult<SConnectedBlockIterator> {
        Ok(SConnectedBlockIterator::new(&self.db, stop))
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
struct SBlockIteratorArray {
    iter: BlockIterator<SBlock>,
}

impl SBlockIteratorArray {
    fn new(db: &api::BitcoinDB, heights: Vec<u32>) -> Self {
        SBlockIteratorArray {
            iter: db.iter_heights::<SBlock>(heights),
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
struct FBlockIteratorArray {
    iter: BlockIterator<FBlock>,
}

impl FBlockIteratorArray {
    fn new(db: &api::BitcoinDB, heights: Vec<u32>) -> Self {
        FBlockIteratorArray {
            iter: db.iter_heights::<FBlock>(heights),
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
struct FBlockIteratorSequential {
    iter: BlockIterator<FBlock>,
}

impl FBlockIteratorSequential {
    fn new(db: &api::BitcoinDB, start: u32, end: u32) -> Self {
        FBlockIteratorSequential {
            iter: db.iter_block::<FBlock>(start, end),
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
    iter: BlockIterator<SBlock>,
}

impl SBlockIteratorSequential {
    fn new(db: &api::BitcoinDB, start: u32, end: u32) -> SBlockIteratorSequential {
        SBlockIteratorSequential {
            iter: db.iter_block::<SBlock>(start, end),
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
struct FConnectedBlockIterator {
    iter: api::ConnectedBlockIterator<FConnectedBlock>,
}

impl FConnectedBlockIterator {
    fn new(db: &api::BitcoinDB, end: u32) -> FConnectedBlockIterator {
        FConnectedBlockIterator {
            iter: db.iter_connected_block(end),
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
    iter: api::ConnectedBlockIterator<SConnectedBlock>,
}

impl SConnectedBlockIterator {
    fn new(db: &api::BitcoinDB, end: u32) -> SConnectedBlockIterator {
        SConnectedBlockIterator {
            iter: db.iter_connected_block(end),
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
fn bitcoin_explorer(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<BitcoinDBPy>()?;
    Ok(())
}
