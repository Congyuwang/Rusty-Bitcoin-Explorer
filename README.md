# bitcoinquery

`bitcoinquery` is a python package developed in `rust` 
for accessing bitcoin-core binary files as a database.

It contains one class `BitcoinDB`.

## Usage

```python
import bitcoinquery as bq

# parse the same path as `--datadir` argument for `bitcoind`.
db = bq.BitcoinDB("~/Bitcoin")

# get the length of the longest chain currently on disk.
db.get_max_height()

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
db.parse_script(script_pub_key)


```
