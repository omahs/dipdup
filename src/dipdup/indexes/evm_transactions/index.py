from collections import deque
from typing import Any

from dipdup.config.evm_transactions import EvmTransactionsHandlerConfig
from dipdup.config.evm_transactions import EvmTransactionsIndexConfig
from dipdup.datasources.evm_node import EvmNodeDatasource
from dipdup.datasources.evm_subsquid import EvmSubsquidDatasource
from dipdup.exceptions import ConfigInitializationException
from dipdup.exceptions import FrameworkException
from dipdup.indexes.evm_subsquid import SubsquidIndex
from dipdup.indexes.evm_subsquid import get_sighash
from dipdup.indexes.evm_transactions.fetcher import EvmEvmTransactionFetcher
from dipdup.indexes.evm_transactions.fetcher import EvmNodeTransactionFetcher
from dipdup.indexes.evm_transactions.matcher import match_transactions
from dipdup.models import RollbackMessage
from dipdup.models.evm import EvmTransaction
from dipdup.models.evm import EvmTransactionData
from dipdup.models.evm_subsquid import TransactionRequest
from dipdup.models.subsquid import SubsquidMessageType
from dipdup.prometheus import Metrics

QueueItem = tuple[EvmTransactionData, ...] | RollbackMessage
Datasource = EvmSubsquidDatasource | EvmNodeDatasource


class EvmTransactionsIndex(
    SubsquidIndex[EvmTransactionsIndexConfig, QueueItem, Datasource],
    message_type=SubsquidMessageType.transactions,
):
    def _match_level_data(self, handlers: Any, level_data: Any) -> deque[Any]:
        return match_transactions(self._ctx.package, handlers, level_data)

    async def _call_matched_handler(
        self,
        handler_config: EvmTransactionsHandlerConfig,
        transaction: EvmTransaction[Any],
    ) -> None:
        if not handler_config.parent:
            raise ConfigInitializationException

        await self._ctx.fire_handler(
            handler_config.callback,
            handler_config.parent.name,
            None,
            transaction,
        )

    async def _synchronize_subsquid(self, sync_level: int) -> None:
        first_level = self.state.level + 1
        fetcher = self._create_subsquid_fetcher(first_level, sync_level)

        async for _level, transactions in fetcher.fetch_by_level():
            await self._process_level_data(transactions, sync_level)
            Metrics.set_sqd_processor_last_block(_level)

    async def _synchronize_node(self, sync_level: int) -> None:
        first_level = self.state.level + 1
        fetcher = self._create_node_fetcher(first_level, sync_level)

        async for _level, transactions in fetcher.fetch_by_level():
            await self._process_level_data(transactions, sync_level)
            Metrics.set_sqd_processor_last_block(_level)

    def _create_subsquid_fetcher(self, first_level: int, last_level: int) -> EvmEvmTransactionFetcher:

        filters: deque[TransactionRequest] = deque()
        for handler_config in self._config.handlers:
            query: TransactionRequest = {}
            if (from_ := handler_config.from_) and from_.address:
                query['from'] = [from_.address]
            if (to_ := handler_config.to) and to_.address:
                query['to'] = [to_.address]
            if method := handler_config.method:
                query['sighash'] = [get_sighash(self._ctx.package, method, to_)]
            if not query:
                raise NotImplementedError
            filters.append(query)

        try:
            datasource = self.subsquid_datasources[0]
        except IndexError:
            raise FrameworkException('Creating subsquid fetcher without subsquid datasource') from None

        return EvmEvmTransactionFetcher(
            datasource=datasource,
            first_level=first_level,
            last_level=last_level,
            filters=tuple(filters),
        )

    def _create_node_fetcher(self, first_level: int, last_level: int) -> EvmNodeTransactionFetcher:
        return EvmNodeTransactionFetcher(
            datasources=self.node_datasources,
            first_level=first_level,
            last_level=last_level,
        )
