# bitcoinquery

`bitcoinquery` is a python package developed in `rust` 
for accessing bitcoin-core binary files as a database.

## Compatibility Note

This package deals with the binary file of another software `Bitcoin Core`.
It might not be compatible with older Bitcoin Core versions.

Currently, it is compatible with Bitcoin Core version
`Bitcoin Core version v0.21.1.0-g194b9b8792d9b0798fdb570b79fa51f1d1f5ebaf
Copyright (C) 2009-2020 The Bitcoin Core developers`.


## Usage

It contains one class `BitcoinDB`.

```python
import bitcoinquery as bq

# parse the same path as `--datadir` argument for `bitcoind`.
db = bq.BitcoinDB("~/Bitcoin")

# get the length of the longest chain currently on disk.
db.get_max_height()

# get block of a certain height
db.get_block(1000)

# to retrieve the connected outputs of each inputs as well
db.get_block(1000, connected=True)

# get block hash of a certain height.
db.get_hash(1000)

# a fast method for getting just the header.
# in memory query, no disk access
db.get_block_header(1000)

# get block of height 1000.
db.get_height_from_hash("some hash")

# get blocks of heights 100..199.
# (This method uses `rayon` parallel computing to take advantage of multicore)
# this function returns json string instead of object.
db.get_block_batch(list(range(100, 200)))

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

# use iterator over heights
for block in db.get_block_iter_array(list(range(10000, 20000))):
    do_something_with(block)
```

## How to Build

This is a PyO3 project. Use maturin to build and publish.
`pip install maturin`.

On mac, when using cargo to build, add the following into `.cargo/cargo.toml`.

```toml
[target.x86_64-apple-darwin]
rustflags = [
  "-C", "link-arg=-undefined",
  "-C", "link-arg=dynamic_lookup",
]

[target.aarch64-apple-darwin]
rustflags = [
  "-C", "link-arg=-undefined",
  "-C", "link-arg=dynamic_lookup",
]

```
