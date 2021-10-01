use crate::api::BitcoinDB;
use crate::parser::blk_file::BlkFile;
use crate::parser::block_index::BlockIndex;

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
