from collections.abc import AsyncIterator

from dipdup.datasources.evm_blockscout import BlockscoutDatasource
from dipdup.fetcher import DataFetcher
from dipdup.fetcher import readahead_by_level
from dipdup.models.evm_blockscout import BlockscoutTransactionData
from dipdup.models.evm_blockscout import TransactionRequest


class TransactionFetcher(DataFetcher[BlockscoutTransactionData]):
    """Fetches transactions from REST API, merges them and yields by level."""

    _datasource: BlockscoutDatasource

    def __init__(
        self,
        datasource: BlockscoutDatasource,
        first_level: int,
        last_level: int,
        filters: tuple[TransactionRequest, ...],
    ) -> None:
        super().__init__(datasource, first_level, last_level)
        self._filters = filters

    async def fetch_by_level(self) -> AsyncIterator[tuple[int, tuple[BlockscoutTransactionData, ...]]]:
        """Iterate over transactions fetched fetched from REST.

        Resulting data is splitted by level, deduped, sorted and ready to be processed by BlockscoutTransactionsIndex.
        """
        transaction_iter = self._datasource.iter_transactions(
            self._first_level,
            self._last_level,
            self._filters,
        )
        async for level, batch in readahead_by_level(transaction_iter, limit=5_000):
            yield level, batch
