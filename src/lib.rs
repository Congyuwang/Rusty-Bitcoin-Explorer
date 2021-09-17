pub mod api;
pub mod bitcoinparser;

use bitcoin::hashes::hex::{FromHex, ToHex};
use bitcoin::Txid;
use pyo3::prelude::*;
use pythonize::pythonize;
use rayon::prelude::*;
use std::path::Path;

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

    #[pyo3(text_signature = "($self, heights, /)")]
    fn get_block_full_connected_batch(&self, heights: Vec<i32>) -> PyResult<Vec<String>> {
        let db = &self.db;
        Ok(heights
            .par_iter()
            .filter_map(|h| db.get_block_full_connected(*h).ok())
            .filter_map(|blk| serde_json::to_string(&blk).ok())
            .collect())
    }

    #[pyo3(text_signature = "($self, heights, /)")]
    fn get_block_simple_connected_batch(&self, heights: Vec<i32>) -> PyResult<Vec<String>> {
        let db = &self.db;
        Ok(heights
            .par_iter()
            .filter_map(|h| db.get_block_simple_connected(*h).ok())
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

    #[pyo3(text_signature = "($self, /)")]
    fn get_max_height(&self) -> usize {
        self.db.block_index.records.len()
    }
}

#[pymodule]
fn bitcoinquery(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<BitcoinDB>()?;
    Ok(())
}
