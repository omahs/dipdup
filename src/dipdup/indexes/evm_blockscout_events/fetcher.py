from collections.abc import AsyncIterator

from dipdup.datasources.evm_blockscout import BlockscoutDatasource
from dipdup.fetcher import DataFetcher
from dipdup.fetcher import readahead_by_level
from dipdup.models.evm_blockscout import BlockscoutEventData


class EventLogFetcher(DataFetcher[BlockscoutEventData]):
    """Fetches contract events from REST API, merges them and yields by level."""

    _datasource: BlockscoutDatasource

    def __init__(
        self,
        datasource: BlockscoutDatasource,
        first_level: int,
        last_level: int,
        topics: tuple[tuple[str | None, str], ...],
    ) -> None:
        super().__init__(datasource, first_level, last_level)
        self._topics = topics

    async def fetch_by_level(self) -> AsyncIterator[tuple[int, tuple[BlockscoutEventData, ...]]]:
        """Iterate over events fetched from graphql.

        Resulting data is split by level, deduped, sorted and ready to be processed by BlockscoutEventsIndex.
        """
        event_iter = self._datasource.iter_event_logs(
            self._topics,
            self._first_level,
            self._last_level,
        )
        async for level, batch in readahead_by_level(event_iter, limit=5_000):
            yield level, batch
