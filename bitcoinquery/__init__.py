import logging as _log
from .bitcoinquery import BitcoinDB as _db

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
        :param tx_index: whether to open tx_index.
        """
        self.db = _db(path, tx_index)
        self.tx_index = tx_index

    def get_block(self, height: int, simplify: bool = True):
        """get the block of a specific height."""
        if simplify:
            return self.db.get_block_simple(height)
        else:
            return self.db.get_block(height)

    def get_max_height(self):
        """get the length of the longest chain currently on disk."""
        return self.db.get_max_height()

    def get_hash(self, height: int):
        """get block hash of a certain height."""
        self.db.get_hash(height)

    def get_block_header(self, height: int):
        """get block header in memory.

        a fast method for getting just the header.
        in memory query, no disk access.

        :return: block header python object
        """
        return self.db.get_hash(height)

    def get_height_from_hash(self, block_hash: str):
        """get block height of certain hash."""
        return self.db.get_height_from_hash(block_hash)

    def get_transaction(self, txid: str, simplify: bool = True):
        """get transactions from txid

        This queries the `levelDB` each time, thus it is relatively slow.

        :param simplify: simplify transaction info (faster)
        :param txid: transaction id (hex encoded string)

        :return: list of transactions
        """
        if not self.tx_index:
            raise Exception("tx_index is set to False")
        if simplify:
            return self.db.get_transaction_simple(txid)
        else:
            return self.db.get_transaction(txid)

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

    def get_block_batch(self, heights: list[int], simplify: bool = True) -> list[str]:
        """get multiple blocks in parallel, return list of json string.

        :param simplify: simplify block info (faster)
        :param heights: a list of heights
        :return: list of json strings
        """
        if simplify:
            return self.db.get_block_batch_simple(heights)
        else:
            return self.db.get_block_batch(heights)
