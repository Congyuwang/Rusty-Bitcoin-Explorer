mod api;
mod bitcoinparser;

use bitcoinparser::parsed_proto::Block;
use pyo3::prelude::*;
use pythonize::pythonize;
use rayon::prelude::*;
use std::path::Path;
use crate::api::TxDB;

#[pyclass]
struct BitcoinDB {
    db: api::BitcoinDB,
    tx_db: api::TxDB,
}

#[pymethods]
impl BitcoinDB {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let path = Path::new(path);
        match api::BitcoinDB::new(path) {
            Ok(db) => {
                let tx_db = TxDB::new(path, &db.block_index);
                Ok(BitcoinDB {
                    db,
                    tx_db,
                })},
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
    fn get_block_batch(&self, heights: Vec<i32>) -> PyResult<String> {
        let db = &self.db;
        let blocks: Vec<Option<Block>> = heights
                .par_iter()
                .map(|h| match db.get_block_of_height(*h) {
                    Ok(block) => Some(block),
                    Err(_) => None,
                }).collect();
        match serde_json::to_string(&blocks) {
            Ok(s) => Ok(s),
            Err(_) => Err(pyo3::exceptions::PyException::new_err("failed to serialize"))
        }
    }

    /// only get the block header (in memory, no disk access)
    #[pyo3(text_signature = "($self, height, /)")]
    fn get_block_header(&self, height: usize) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        match self.db.get_block_header(height) {
            Ok(block) => Ok(pythonize(py, &block)?),
            Err(_) => Err(pyo3::exceptions::PyException::new_err(
                "height not found",
            )),
        }
    }

    #[pyo3(text_signature = "($self, height, /)")]
    fn get_hash(&self, height: usize) -> PyResult<String> {
        match self.db.block_index.records.get(height) {
            None => Err(pyo3::exceptions::PyException::new_err(
                "height not found",
            )),
            Some(s) => Ok(s.block_hash.to_string())
        }
    }

    #[pyo3(text_signature = "($self, hash, /)")]
    fn get_height_from_hash(&self, hash: String) -> PyResult<i32> {
        match self.db.block_index.hash_to_height.get(&hash) {
            None => Err(pyo3::exceptions::PyException::new_err(
                "hash not found",
            )),
            Some(h) => Ok(*h)
        }
    }

    #[pyo3(text_signature = "($self, txid, /)", name = "get_height_from_txid")]
    fn query_height_from_txid(&mut self, txid: String) -> PyResult<i32> {
        match self.tx_db.query_block_height_of_transaction(&txid) {
            Err(_) => Err(pyo3::exceptions::PyException::new_err(
                "txid not found",
            )),
            Ok(h) => Ok(h)
        }
    }

    #[pyo3(text_signature = "($self, txid, /)", name = "get_transaction")]
    fn query_transaction(&mut self, txid: String) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        match self.tx_db.query_transaction(&txid, &self.db.blk_store) {
            Ok(t) => Ok(pythonize(py, &t)?),
            Err(_) => Err(pyo3::exceptions::PyException::new_err(
                "txid not found",
            ))
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
            ))
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
