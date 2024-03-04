import asyncio
import time
from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from collections import deque
from typing import Any
from typing import Generic
from typing import TypeVar
from typing import cast

from web3 import Web3

from dipdup.config import BlockscoutIndexConfigU
from dipdup.config.evm import EvmContractConfig
from dipdup.context import DipDupContext
from dipdup.datasources.evm_blockscout import BlockscoutDatasource
from dipdup.datasources.evm_node import NODE_LAST_MILE
from dipdup.exceptions import ConfigurationError
from dipdup.exceptions import FrameworkException
from dipdup.index import Index
from dipdup.index import IndexQueueItemT
from dipdup.models.evm_blockscout import BlockscoutMessageType
from dipdup.package import DipDupPackage
from dipdup.performance import metrics
from dipdup.prometheus import Metrics

IndexConfigT = TypeVar('IndexConfigT', bound=BlockscoutIndexConfigU)
DatasourceT = TypeVar('DatasourceT', bound=BlockscoutDatasource)



_sighashes: dict[str, str] = {}


def get_sighash(package: DipDupPackage, method: str, to: EvmContractConfig | None = None) -> str:
    """Method in config is either a full signature or a method name. We need to convert it to a sighash first."""
    key = method + (to.module_name if to else '')
    if key in _sighashes:
        return _sighashes[key]

    if {'(', ')'} <= set(method) and not to:
        _sighashes[key] = Web3.keccak(text=method).hex()[:10]
    elif to:
        _sighashes[key] = package.get_converted_abi(to.module_name)['methods'][method]['sighash']
    else:
        raise ConfigurationError('`to` field is missing; `method` is expected to be a full signature')
    return _sighashes[key]


class BlockscoutIndex(
    Index[IndexConfigT, IndexQueueItemT, DatasourceT],
    ABC,
    Generic[IndexConfigT, IndexQueueItemT, DatasourceT],
    message_type=BlockscoutMessageType,
):
    def __init__(
        self,
        ctx: DipDupContext,
        config: IndexConfigT,
        datasource: DatasourceT,
    ) -> None:
        super().__init__(ctx, config, datasource)

    @abstractmethod
    async def _synchronize_blockscout(self, sync_level: int) -> None: ...

    @abstractmethod
    def _match_level_data(
        self,
        handlers: Any,
        level_data: Any,
    ) -> deque[Any]: ...

    @abstractmethod
    async def _call_matched_handler(
        self,
        handler_config: Any,
        level_data: Any,
    ) -> None: ...



    def get_sync_level(self) -> int:
        """Get level index needs to be synchronized to depending on its subscription status"""
        sync_levels = set()
        for sub in self._config.get_subscriptions():
            sync_levels.add(self.datasource.get_sync_level(sub))

        if None in sync_levels:
            sync_levels.remove(None)
        if not sync_levels:
            raise FrameworkException('Initialize config before starting `IndexDispatcher`')

        # NOTE: Multiple sync levels means index with new subscriptions was added in runtime.
        # NOTE: Choose the highest level; outdated realtime messages will be dropped from the queue anyway.
        return max(cast(set[int], sync_levels))

    async def get_blocks_batch(
        self,
        levels: set[int],
        full_transactions: bool = False,
    ) -> dict[int, dict[str, Any]]:
        blocks: dict[int, Any] = {}
        if not levels:
            return blocks

        tasks: deque[asyncio.Task[Any]] = deque()

        async def _fetch(level: int) -> None:
            blocks[level] = await self.datasource.get_block_by_level(
                block_number=level,
                full_transactions=full_transactions,
            )

        for level in levels:
            tasks.append(
                asyncio.create_task(
                    _fetch(level),
                    name=f'get_block_range:{level}',
                ),
            )

        await asyncio.gather(*tasks)
        return blocks

    async def get_logs_batch(
        self,
        first_level: int,
        last_level: int,
    ) -> dict[int, list[dict[str, Any]]]:
        grouped_logs: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
        logs = await self.datasource.get_logs(
            {
                'fromBlock': hex(first_level),
                'toBlock': hex(last_level),
            },
        )
        for log in logs:
            grouped_logs[int(log['blockNumber'], 16)].append(log)
        return grouped_logs

    async def _get_node_sync_level(self, blockscout_level: int, index_level: int) -> int | None:
        node_sync_level = await self.datasource.get_head_level()
        blockscout_lag = abs(node_sync_level - blockscout_level)
        blockscout_available = blockscout_level - index_level
        self._logger.info('Blockscout is %s levels behind; %s available', blockscout_lag, blockscout_available)
        if blockscout_available < NODE_LAST_MILE:
            return node_sync_level
        if self._config.node_only:
            self._logger.debug('`node_only` flag is set; using node anyway')
            return node_sync_level
        return None

    async def _synchronize(self, sync_level: int) -> None:
        """Fetch event logs via Fetcher and pass to message callback"""
        index_level = await self._enter_sync_state(sync_level)
        if index_level is None:
            return

        levels_left = sync_level - index_level
        if levels_left <= 0:
            return

        sync_level = await self.datasource.get_head_level()
        Metrics.set_sqd_processor_chain_height(sync_level)

        await self._synchronize_blockscout(sync_level)

        await self._exit_sync_state(sync_level)

    async def _process_level_data(
        self,
        level_data: Any,
        sync_level: int,
    ) -> None:
        if not level_data:
            return

        batch_level = level_data[0].level
        index_level = self.state.level

        print(batch_level, index_level)

        if batch_level <= index_level:
            raise FrameworkException(f'Batch level is lower than index level: {batch_level} <= {index_level}')

        self._logger.debug('Processing data of level %s', batch_level)
        started_at = time.time()
        matched_handlers = self._match_level_data(self._config.handlers, level_data)

        total_matched = len(matched_handlers)
        Metrics.set_index_handlers_matched(total_matched)
        metrics[f'{self.name}:handlers_matched'] += total_matched
        metrics[f'{self.name}:time_in_matcher'] += (time.time() - started_at) / 60

        # NOTE: We still need to bump index level but don't care if it will be done in existing transaction
        if not matched_handlers:
            await self._update_state(level=batch_level)
            return

        print(matched_handlers)
        breakpoint()

        started_at = time.time()
        async with self._ctx.transactions.in_transaction(batch_level, sync_level, self.name):
            for handler_config, data in matched_handlers:
                await self._call_matched_handler(handler_config, data)
            await self._update_state(level=batch_level)
        metrics[f'{self.name}:time_in_callbacks'] += (time.time() - started_at) / 60
