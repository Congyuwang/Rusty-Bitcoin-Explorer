use crate::parser::errors::{OpError, OpErrorKind, OpResult};
use crate::parser::reader::BlockchainRead;
use bitcoin::{Block, Transaction};
use std::collections::HashMap;
use std::convert::From;
use std::fs::{self, DirEntry, File};
use std::io::{self, BufReader, Cursor, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Holds all necessary data about a raw blk file
#[derive(Debug, Clone)]
pub struct BlkFile {
    files: HashMap<i32, PathBuf>,
}

impl BlkFile {
    pub(crate) fn new(path: &Path) -> OpResult<BlkFile> {
        Ok(BlkFile {
            files: BlkFile::scan_path(path)?,
        })
    }

    pub(crate) fn read_block(&self, n_file: i32, offset: u32) -> OpResult<Block> {
        if let Some(blk_path) = self.files.get(&n_file) {
            let mut r = BufReader::new(File::open(blk_path)?);
            r.seek(SeekFrom::Start(offset as u64 - 4))?;
            let block_size = r.read_u32()?;
            let block = r.read_u8_vec(block_size)?;
            Cursor::new(block).read_block()
        } else {
            Err(OpError::from("blk file not found, sync with bitcoin core"))
        }
    }

    pub(crate) fn read_transaction(
        &self,
        n_file: i32,
        n_pos: u32,
        n_tx_offset: u32,
    ) -> OpResult<Transaction> {
        if let Some(blk_path) = self.files.get(&n_file) {
            let mut r = BufReader::new(File::open(blk_path)?);
            r.seek(SeekFrom::Start(n_pos as u64 + n_tx_offset as u64 + 80))?;
            r.read_transaction()
        } else {
            Err(OpError::from("blk file not found, sync with bitcoin core"))
        }
    }

    fn scan_path(path: &Path) -> OpResult<HashMap<i32, PathBuf>> {
        let mut collected = HashMap::with_capacity(4000);
        for entry in fs::read_dir(path)? {
            match entry {
                Ok(de) => {
                    let path = BlkFile::resolve_path(&de)?;
                    if !path.is_file() {
                        continue;
                    };
                    if let Some(file_name) = path.as_path().file_name() {
                        if let Some(file_name) = file_name.to_str() {
                            if let Some(index) = BlkFile::parse_blk_index(&file_name) {
                                collected.insert(index, path);
                            }
                        }
                    }
                }
                Err(msg) => {
                    return Err(OpError::from(msg));
                }
            }
        }
        if collected.is_empty() {
            Err(OpError::new(OpErrorKind::RuntimeError).join_msg("No blk files found!"))
        } else {
            Ok(collected)
        }
    }

    fn resolve_path(entry: &DirEntry) -> io::Result<PathBuf> {
        if entry.file_type()?.is_symlink() {
            fs::read_link(entry.path())
        } else {
            Ok(entry.path())
        }
    }

    fn parse_blk_index(file_name: &str) -> Option<i32> {
        let prefix = "blk";
        let ext = ".dat";
        if file_name.starts_with(prefix) && file_name.ends_with(ext) {
            file_name[prefix.len()..(file_name.len() - ext.len())]
                .parse::<i32>()
                .ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_blk_index() {
        assert_eq!(0, BlkFile::parse_blk_index("blk00000.dat").unwrap());
        assert_eq!(6, BlkFile::parse_blk_index("blk6.dat").unwrap());
        assert_eq!(1202, BlkFile::parse_blk_index("blk1202.dat").unwrap());
        assert_eq!(
            13412451,
            BlkFile::parse_blk_index("blk13412451.dat").unwrap()
        );
        assert_eq!(true, BlkFile::parse_blk_index("blkindex.dat").is_none());
        assert_eq!(true, BlkFile::parse_blk_index("invalid.dat").is_none());
    }
}
