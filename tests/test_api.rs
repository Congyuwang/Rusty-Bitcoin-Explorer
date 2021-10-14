//!
//! Integration Test
//!
//! Test multiple APIs. Cross checking results between each other.
//!
#[cfg(test)]
mod iterator_tests {
    use bitcoin::{Block, Transaction};
    use bitcoin_explorer::{BitcoinDB, SBlock, SConnectedBlock, SConnectedTransaction};
    use std::path::PathBuf;

    const END: u32 = 700000;

    fn get_test_db() -> BitcoinDB {
        let mut crate_root_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        crate_root_dir.push("./resources/tests/Bitcoin");
        BitcoinDB::new(&crate_root_dir, true).unwrap()
    }

    #[test]
    fn test_iter_block() {
        let db = get_test_db();

        let mut h = 0;
        for blk in db.iter_block::<SBlock>(0, END) {
            let blk_ref = db.get_block::<SBlock>(h).unwrap();
            assert_eq!(blk, blk_ref);
            h += 1;
        }
        assert_eq!(h as usize, db.get_block_count())
    }

    #[test]
    /// see that iterator ends correctly
    fn test_iter_block_early_end() {
        let db = get_test_db();
        let early_end = 100000;

        let mut h = 0;
        for _ in db.iter_block::<SBlock>(0, early_end) {
            h += 1;
        }
        assert_eq!(h as u32, early_end)
    }

    #[test]
    /// ensure that the iterator can be dropped
    fn test_iter_block_break() {
        let db = get_test_db();
        let break_height = 100000;

        let mut h = 0;
        let mut some_blk = None;
        for blk in db.iter_block::<SBlock>(0, END) {
            some_blk = Some(blk);
            if h == break_height {
                break;
            }
            h += 1;
        }
        assert_eq!(some_blk, Some(db.get_block(break_height).unwrap()))
    }

    #[test]
    fn test_get_transactions() {
        let db = get_test_db();
        let early_end = 100000;

        for blk in db.iter_block::<Block>(0, early_end) {
            for tx in blk.txdata {
                assert_eq!(db.get_transaction::<Transaction>(&tx.txid()).unwrap(), tx);
            }
        }
    }

    #[test]
    fn test_iter_connected() {
        let db = get_test_db();

        let mut h = 0;
        for blk in db.iter_connected_block::<SConnectedBlock>(END) {
            assert_eq!(blk.header, db.get_block::<SBlock>(h).unwrap().header);
            h += 1;
        }
        assert_eq!(h as usize, db.get_block_count())
    }

    #[test]
    /// see that iterator ends correctly
    fn test_iter_connected_early_end() {
        let db = get_test_db();
        let early_end = 100000;

        let mut h = 0;
        for blk in db.iter_connected_block::<SConnectedBlock>(early_end) {
            let blk_ref = db.get_connected_block::<SConnectedBlock>(h).unwrap();
            assert_eq!(blk, blk_ref);
            h += 1;
        }
    }

    #[test]
    /// ensure that the iterator can be dropped
    fn test_iter_connected_break() {
        let db = get_test_db();
        let break_height = 100000;

        let mut h = 0;
        let mut some_blk = None;
        for blk in db.iter_connected_block::<SConnectedBlock>(END) {
            some_blk = Some(blk);
            if h == break_height {
                break;
            }
            h += 1;
        }
        assert_eq!(
            some_blk,
            Some(
                db.get_connected_block::<SConnectedBlock>(break_height)
                    .unwrap()
            )
        )
    }

    #[test]
    fn test_get_connected_transactions() {
        let db = get_test_db();
        let early_end = 100000;

        for blk in db.iter_connected_block::<SConnectedBlock>(early_end) {
            for tx in blk.txdata {
                assert_eq!(
                    db.get_connected_transaction::<SConnectedTransaction>(&tx.txid)
                        .unwrap(),
                    tx
                );
            }
        }
    }
}
