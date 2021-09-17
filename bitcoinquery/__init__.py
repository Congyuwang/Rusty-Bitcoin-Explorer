import logging as _log
from .bitcoinquery import BitcoinDB as _BitcoinDB

_log.basicConfig(
    level=_log.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)


class BitcoinDB:

    def __init__(self, path: str, tx_index: bool = True):
        """launch BitcoinDB.

        Note that this cannot be pickled to another process.

        :param path: the path for bitcoin root directory,
        the same parameter as `--datadir` flag for `bitcoind`.
        :param tx_index: whether to try to open tx_index levelDB.
        """
        self.db = _BitcoinDB(path, tx_index)
        self.tx_index = tx_index

    def get_block(self, height: int, simplify: bool = True, connected: bool = False):
        """
        get the block of a specific height.
        :param height: the height of the block
        :param simplify: whether to use simpler format (faster)
        :param connected: whether to replace outpoints by connected outputs
        :return:
        """
        if not connected:
            if simplify:
                return self.db.get_block_simple(height)
            else:
                return self.db.get_block_full(height)
        else:
            if simplify:
                return self.db.get_block_simple_connected(height)
            else:
                return self.db.get_block_full_connected(height)

    def get_max_height(self):
        """get the length of the longest chain currently on disk."""
        return self.db.get_max_height()

    def get_hash_from_height(self, height: int):
        """get block hash of a certain height."""
        return self.db.get_hash_from_height(height)

    def get_height_from_hash(self, block_hash: str):
        """get block height of certain hash."""
        return self.db.get_height_from_hash(block_hash)

    def get_block_header(self, height: int):
        """get block header in memory.

        a fast method for getting just the header.
        in memory query, no disk access.

        :return: block header python object
        """
        return self.db.get_block_header(height)

    def get_transaction(self,
                        txid: str,
                        simplify: bool = True,
                        connected: bool = False):
        """get transactions from txid

        This queries the `levelDB` each time, thus it is relatively slow.

        :param simplify: simplify transaction info (faster)
        :param txid: transaction id (hex encoded string)
        :param connected: whether to replace outpoints by output
        :return: list of transactions
        """
        if not self.tx_index:
            raise Exception("tx_index is set to False")
        if not connected:
            if simplify:
                return self.db.get_transaction_simple(txid)
            else:
                return self.db.get_transaction_full(txid)
        else:
            if simplify:
                return self.db.get_transaction_simple_connected(txid)
            else:
                return self.db.get_transaction_full_connected(txid)

    def get_height_from_txid(self, txid: str):
        """get the height of the block which this transaction belongs.

        :param txid: transaction id (hex encoded string)
        :return: the height of the block
        """
        if not self.tx_index:
            raise Exception("tx_index is set to False")
        return self.db.get_height_from_txid(txid)

    def parse_script(self, script_pub_key: str):
        """get the script type and addresses from hex encoded script public key.

        :param script_pub_key: script public key (hex encoded string)
        :return: addresses in the script
        """
        return self.db.parse_script(script_pub_key)

    def get_block_batch(self, heights: list[int],
                        simplify: bool = True,
                        connected: bool = False) -> list[str]:
        """get multiple blocks in parallel, return list of json string.

        :param simplify: simplify block info (faster)
        :param heights: a list of heights
        :return: list of json strings
        """
        if not connected:
            if simplify:
                return self.db.get_block_simple_batch(heights)
            else:
                return self.db.get_block_full_batch(heights)
        else:
            if simplify:
                return self.db.get_block_simple_connected_batch(heights)
            else:
                return self.db.get_block_full_connected_batch(heights)
