# bitcoin-explorer

`bitcoin_explorer` is a python package developed in `rust` 
for accessing bitcoin-core binary files as a database.

## How to install?

This library is designed for both python and rust.

For python users, `pip install bitcoin-explorer`.

For rust users, include `bitcoin-explorer="0.1"` in `Cargo.toml`.

### Install Requirements

Currently, only macOS py39 wheels are provided. 

For pip to build the package on other OS or python version,
make sure that `rust` toolchain is installed,
and `cmake` is also installed.

## Documentation

For rust users, go to [Rust Documentation](https://docs.rs/bitcoin-explorer/)

For python documentation, go to [Documentation](https://congyuwang.github.io/Rusty-Bitcoin-Explorer/bitcoin_explorer.html).

## Compatibility Note

This package deals with the binary file of another software `Bitcoin Core`.
It might not be compatible with older Bitcoin Core versions.

Currently, it is compatible with Bitcoin Core version
`Bitcoin Core version v0.21.1.0-g194b9b8792d9b0798fdb570b79fa51f1d1f5ebaf
Copyright (C) 2009-2020 The Bitcoin Core developers`.

## Performance

The current performance bottleneck is disk IO.
SSD allows faster performance.

On HDD, iterating from heights 600000 to 700000 takes less than 30 minutes.

Iterating through ``connected blocks`` from 0 to 700000 takes 5 hours.

## Example (rust)

### get a complete block (full format (i.e., see doc for what is full/simple format))
```rust
use bitcoin_explorer::api::{BitcoinDB, FBlock};
use std::path::Path;

let path = Path::new("/Users/me/bitcoin").unwrap();

// launch without reading txindex
let db = BitcoinDB::new(path, false).unwrap();

// get block of height 600000
let block: FBlock = db.get_block_full(600000).unwrap();
```

### get a particular transaction

```rust
use bitcoin_explorer::api::{BitcoinDB, STransaction, Txid, FromHex};
use std::path::Path;

let path = Path::new("/Users/me/bitcoin").unwrap();

// !!must launch with txindex=true!!
let db = BitcoinDB::new(path, true).unwrap();

// get transaction
// e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
let txid_str = "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468";
let txid = Txid::from_hex(txid_str).unwrap();
let tx: STransaction = db.get_transaction_simple(&txid).unwrap();
```

### Iterate through blocks
```rust
use bitcoin_explorer::api::BitcoinDB;
use std::path::Path;

let path = Path::new("/Users/me/bitcoin").unwrap();

// launch without reading txindex
let db = BitcoinDB::new(path, false).unwrap();

// iterate over block from 600000 to 700000
for block in db.get_block_full_iter_seq(600000, 700000) {
    for tx in block.txdata {
        println!("do something for this transaction");
    }
}
```

### Iterate through blocks (simple format) with outpoints connected to outputs
```rust
use bitcoin_explorer::api::BitcoinDB;
use std::path::Path;

let path = Path::new("/Users/me/bitcoin").unwrap();

// launch without reading txindex
let db = BitcoinDB::new(path, false).unwrap();

// iterate over block from 0 to 700000
for block in db.get_block_simple_connected_iter(700000) {
    for tx in block.txdata {
        println!("do something for this transaction");
    }
}
```

## Example (python)

It contains one class `BitcoinDB`.

```python
import bitcoin_explorer as bex

# parse the same path as `--datadir` argument for `bitcoind`.
db = bex.BitcoinDB("~/Bitcoin")

# get the length of the longest chain currently on disk.
db.get_max_height()

# get block of a certain height
db.get_block(1000)

# to retrieve the connected outputs of each inputs as well
db.get_block(1000, connected=True)

# get block hash of a certain height.
db.get_hash_from_height(1000)

# a fast method for getting just the header.
# in memory query, no disk access
db.get_block_header(1000)

# get block of height 1000.
db.get_height_from_hash("some hash")

# get transaction from txid.
# This queries the `levelDB` each time, thus it is relatively slow.
db.get_transaction("some txid")

# get the height of the block which this transaction belongs.
db.get_height_from_txid("some txid")

# get the script type and addresses from a script public key
db.parse_script("some hex script pubic key")

# use iterator
for block in db.get_block_iter_range(start=1000, end=2000):
    do_something_with(block)

# use iterator, iterate over heights
for block in db.get_block_iter_array(heights=[1, 3, 5, 7, 9]):
    do_something_with(block)
    
# use iterator, connect outpoints
for block in db.get_block_iter_range(end=700000, connected=True):
    do_something_with(block)
```
