"""
BitcoinQuery: Query Bitcoin Core Data Files As A Database.

1. How to install?
    - `pip install bitcoin-explorer`

2. How to build?
    - install `rust` toolchain.
    - download the complete repo from github.
    - `cd` to the repository root.
    - run `pip install maturin` to install `maturin`.
    - run `maturin develop --release` to install the package directly to
      the currently available python environment.
      Or you may run `maturin build --release` to build the wheel
      and install the `whl` file use `pip install` command.

3. How to use?
    - Download Bitcoin Core app from bitcoin official website.
    - Sync full Bitcoin data.
    - If you wish to query transaction with txid (i.e. transaction id),
      run Bitcoin Core or `bitcoind` with flag `--txindex=1`,
      or rerun Bitcoin Core with `--reindex --txindex=1`.
    - After having the Bitcoin blockchain data, shutdown Bitcoin Core
      or `bitcoind`. This program cannot run concurrently with
      Bitcoin Core.
    - Open `python`, import the package, and instantiate `BitcoinDB`.

"""
import logging as _log
from typing import Iterator, List

from .bitcoin_explorer import BitcoinDB as _BitcoinDB
from pathlib import Path

_log.basicConfig(
    level=_log.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)


class BitcoinDB:
    """Query bitcoin core like a database."""

    def __init__(self, path: str, tx_index: bool = True):
        """Instantiate this class to query bitcoin core files.

        Notes:
            Pass the bitcoin directory to the argument, wait for
            the program to read the block index, and possibly transaction
            index. This should take a few seconds.

            Caveat: instantiating the class with tx_index=True
            will hold the LOCK of txindex levelDB, thus preventing
            another instance to use the transaction ID query related
            APIs. Although the other APIs will work just fine.
            Multiple instances of the class should work properly,
            which can be used for parallel programming, and thus
            increasing processing speed.

            However, it is important to know that, **during the starting
            period of BitcoinDB object, it locks the block index levelDB,
            which would make another BitcoinDB object fail to instantiate
            at the same time.** If multiple instances of BitcoinDB objects
            are needed, they must wait for the other instance to finish
            reading the levelDB.


        Examples:
            ::

                # import package
                import bitcoin_explorer as bq

                # launch bitcoinDB
                db = bq.BitcoinDB("~/Bitcoin", tx_index=True)

                # query block information of height 10000
                block = db.get_block(10000)
                print(block["txdata"])

        Args:
            path: the same as bitcoin core `--datadir` as path argument.
            tx_index (bool): if your `Bitcoin Core` is launched with
                `--txindex=1`, you may set `tx_index=True` to query
                transactions with `txid` (transaction id).
                When this is set to `True`, BitcoinDB will attempt to
                open the levelDB in `indexes/txindex` directory,
                and will print a warning if it fails to open.
        """
        path = Path(path).absolute()
        self.db = _BitcoinDB(str(path), tx_index)
        self.tx_index = tx_index

    def get_max_height(self) -> int:
        """Get the maximum height found in block index.

        Notes:
            It is not necessarily true that any height smaller than
            `get_max_height()` can be used to query `get_block()`.
            This max_height is only the max height of block header
            information. The actual block data might not have been
            synced for querying.

            However, `get_block_header()`, `get_height_from_hash()`
            `get_hash_from_height()`, will necessarily return valid
            data, as long as height is smaller than `get_max_height()`.

        Returns: max height in block index.

        """
        return self.db.get_max_height()

    def get_block(self, height: int,
                  simplify: bool = True,
                  connected: bool = False) -> dict:
        """Get the block of a specific height.

        Notes:
            To get addresses of inputs, set connected to `True`.

            However, tracing block inputs in this manner requires
            extensive query to txindex levelDB, and random reading
            of disk, which is too slow. It takes several seconds
            to get just one big block (i.e., blocks with 1 thousand
            or more transactions) if connected is set to `True`.

            The best way to retrieve a large number of input addresses
            (may be for the whole blockchain) is to retrieve blocks
            in sequential order using the `get_block_iter_range()`
            method, and set `connected=True`, which is almost as fast
            as `connected=False`.
            ``

        Args:
            height: the height of the block.
            simplify: whether to use simpler format, (i.e., discard
                witness program, script public hash, etc.), which makes
                parsing to python objects lot faster.
            connected: whether to replace inputs by related previous outputs.

        Returns: block data, or error if the height is not yet synced.

        """
        if not connected:
            if simplify:
                return self.db.get_block_simple(height)
            else:
                return self.db.get_block_full(height)
        else:
            if not self.tx_index:
                raise Exception("tx_index is set to False")
            if simplify:
                return self.db.get_block_simple_connected(height)
            else:
                return self.db.get_block_full_connected(height)

    def get_block_header(self, height: int) -> dict:
        """Get block header information.

        Notes:
            Block header information include **number of transactions**,
            **block time**, **nounce**, and so on.

            This is an in-memory query, thus very fast.
            This method is useful for computing blockchain statistics.

        Args:
            height: block height.

        Returns: header information.

        """
        return self.db.get_block_header(height)

    def get_hash_from_height(self, height: int) -> str:
        """Get block hash of a certain height.

        Notes:
            This is an in-memory query, thus very fast.

        Args:
            height(int): block height.

        Returns: block hash (hex string).

        """
        return self.db.get_hash_from_height(height)

    def get_height_from_hash(self, block_hash: str) -> int:
        """Get block height of certain hash.

        Notes:
            This is an in-memory query, thus very fast.

        Args:
            block_hash(str): block hash (hex string).

        Returns: block height.

        """
        return self.db.get_height_from_hash(block_hash)

    def get_transaction(self,
                        txid: str,
                        simplify: bool = True,
                        connected: bool = False) -> dict:
        """Get transaction data from transaction ID.

        Notes:
            Setting connected to `True` will retrieve the sender's
            addresses. This operation queries the on-disk levelDB
            to read transactions, and is not the most performant.

        Args:
            txid (str): transaction id (hex string).
            simplify (bool): default True. Use simpler format.
            connected (bool): whether to replace inputs by
                related outputs according to input outpoints.

        Returns: transaction data.

        """
        if not self.tx_index:
            raise Exception("tx_index is set to False")
        if not connected:
            if simplify:
                return self.db.get_transaction_simple(txid)
            else:
                return self.db.get_transaction_full(txid)
        else:
            if not self.tx_index:
                raise Exception("tx_index is set to False")
            if simplify:
                return self.db.get_transaction_simple_connected(txid)
            else:
                return self.db.get_transaction_full_connected(txid)

    def get_height_from_txid(self, txid: str) -> int:
        """Get height of the block that includes a transaction.

        Notes:
            This operation queries the on-disk levelDB (txindex).
            Thus is not the most performant operation.

        Args:
            txid(str): transaction id (hex string).

        Returns: height (int).

        """
        if not self.tx_index:
            raise Exception("tx_index is set to False")
        return self.db.get_height_from_txid(txid)

    def get_block_iter_range(self,
                             stop,
                             start: int = 0,
                             simplify: bool = True,
                             connected: bool = False) -> Iterator[dict]:
        """Iterate through blocks in ascending order of heights.

        This iterator fails fast. Any error interrupts it.

        Notes:
            This is best way to retrieve a large number of input
            addresses (set connected to `True`).

            It loops over blocks and transactions in sequential
            order, and keeps track of unspent outputs, which enables
            looking up outpoints in memory instead of using levelDB.

        Examples:
            ::

                # get connected blocks use a `for in loop`
                for block in db.get_block_iter_range(stop=700000, connected=True):
                    do_some_computation_with_the_block(block)

        Args:
            stop: stop height (excluded).
            start: starting height, which is ignored if `connected=True`
                since connected iterator can only run from first block.
            simplify: default True. Use simpler format.
            connected: default False. Set to `True` to find out input
                addresses.

        Returns: python iterator of block.

        """
        if not connected:
            if simplify:
                return self.db.iter_block_simple_seq(start, stop)
            else:
                return self.db.iter_block_full_seq(start, stop)
        else:
            if simplify:
                return self.db.iter_block_simple_connected(stop)
            else:
                return self.db.iter_block_full_connected(stop)

    def get_block_iter_array(self,
                             heights: List[int],
                             simplify: bool = True) -> Iterator[dict]:
        """Iterate through blocks of a given list of heights.

        This iterator fails fast. Any error interrupts it.

        Notes:
            This iterator is also implemented to execute in parallel.
            However, the throughput might be restricted by python.

            This iterator does not support connecting outpoints.

        Examples:
            ::

                # get connected blocks use a `for in loop`
                for block in db.get_block_iter_array(list(range(2000, 3000))):
                    do_some_computation_with_the_block(block)

        Args:
            heights: a list of heights.
            simplify: default True. Use simpler format.

        Returns: python iterator of block.

        """
        if simplify:
            return self.db.iter_block_simple_arr(heights)
        else:
            return self.db.iter_block_full_arr(heights)

    def parse_script(self, script_pub_key: str) -> dict:
        """Decode the script type and addresses from script public key.

        Args:
            script_pub_key: script public key (hex encoded string).

        Returns: script type and related addresses.

        """
        return self.db.parse_script(script_pub_key)
