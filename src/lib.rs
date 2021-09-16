mod api;
mod bitcoinparser;

use crate::api::TxDB;
use pyo3::prelude::*;
use pythonize::pythonize;
use rayon::prelude::*;
use std::path::Path;

#[pyclass]
struct BitcoinDB {
    db: api::BitcoinDB,
    tx_db: Option<api::TxDB>,
}

#[pymethods]
impl BitcoinDB {
    ///
    /// `tx_index`: whether to open tx_index levelDB.
    /// Setting `tx_index` to `false` allows easy python parallelization.
    ///
    #[new]
    fn new(path: &str, tx_index: bool) -> PyResult<Self> {
        let path = Path::new(path);
        match api::BitcoinDB::new(path) {
            Ok(db) => {
                if tx_index {
                    let tx_db = TxDB::new(path, &db.block_index);
                    Ok(BitcoinDB { db, tx_db: Some(tx_db) })
                } else {
                    Ok(BitcoinDB { db, tx_db: None })
                }
            }
            Err(_) => Err(pyo3::exceptions::PyException::new_err(
                "failed to launch bitcoinDB",
            )),
        }
    }

    /// get complete block of height
    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block(&self, height: i32) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        match self.db.get_block_of_height(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(_) => Err(pyo3::exceptions::PyException::new_err(
                "failed to get block",
            )),
        }
    }

    /// get blocks of heights in parallel
    #[pyo3(text_signature = "($self, heights, /)")]
    fn get_block_batch(&self, heights: Vec<i32>) -> PyResult<Vec<String>> {
        let db = &self.db;
        Ok(heights.par_iter()
            .filter_map(|h| {
                db.get_block_of_height(*h).ok()
            })
            .filter_map(|blk| {
                serde_json::to_string(&blk).ok()
            })
            .collect())
    }

    /// only get the block header (in memory, no disk access)
    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_header(&self, height: usize) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        match self.db.get_block_header(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(_) => Err(pyo3::exceptions::PyException::new_err("height not found")),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_hash(&self, height: usize) -> PyResult<String> {
        match self.db.block_index.records.get(height) {
            None => Err(pyo3::exceptions::PyException::new_err("height not found")),
            Some(s) => Ok(s.block_hash.to_string()),
        }
    }

    #[pyo3(text_signature = "($self, hash, /)")]
    fn get_height_from_hash(&self, hash: String) -> PyResult<i32> {
        match self.db.block_index.hash_to_height.get(&hash) {
            None => Err(pyo3::exceptions::PyException::new_err("hash not found")),
            Some(h) => Ok(*h),
        }
    }

    #[pyo3(text_signature = "($self, txid, /)", name = "get_height_from_txid")]
    fn query_height_from_txid(&mut self, txid: String) -> PyResult<i32> {
        if let Some(tx_db) = self.tx_db.as_mut() {
            match tx_db.query_block_height_of_transaction(&txid) {
                Err(_) => Err(pyo3::exceptions::PyException::new_err("txid not found")),
                Ok(h) => Ok(h),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err("tx_index not set to True"))
        }
    }

    #[pyo3(text_signature = "($self, txid, /)", name = "get_transaction")]
    fn query_transaction(&mut self, txid: String) -> PyResult<PyObject> {
        if let Some(tx_db) = self.tx_db.as_mut() {
            let gil = Python::acquire_gil();
            let py = gil.python();
            match tx_db.query_transaction(&txid, &self.db.blk_store) {
                Ok(t) => Ok(pythonize(py, &t)?),
                Err(_) => Err(pyo3::exceptions::PyException::new_err("txid not found")),
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err("tx_index not set to True"))
        }
    }

    #[staticmethod]
    #[pyo3(text_signature = "($self, script_pub_key, /)")]
    fn parse_script(script_pub_key: String) -> PyResult<PyObject> {
        let script = api::parse_script(&script_pub_key);
        match script {
            Ok(script) => {
                let gil = Python::acquire_gil();
                let py = gil.python();
                Ok(pythonize(py, &script)?)
            }
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
