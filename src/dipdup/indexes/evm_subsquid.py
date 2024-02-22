import asyncio
import random
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

from dipdup.config import SubsquidIndexConfigU
from dipdup.config.evm import EvmContractConfig
from dipdup.config.evm_node import EvmNodeDatasourceConfig
from dipdup.context import DipDupContext
from dipdup.datasources.evm_node import NODE_LAST_MILE
from dipdup.datasources.evm_node import EvmNodeDatasource
from dipdup.datasources.evm_subsquid import SubsquidDatasource
from dipdup.exceptions import ConfigurationError
from dipdup.exceptions import FrameworkException
from dipdup.index import Index
from dipdup.index import IndexQueueItemT
from dipdup.models.evm_subsquid import SubsquidMessageType
from dipdup.package import DipDupPackage
from dipdup.performance import metrics
from dipdup.prometheus import Metrics

IndexConfigT = TypeVar('IndexConfigT', bound=SubsquidIndexConfigU)
DatasourceT = TypeVar('DatasourceT', bound=SubsquidDatasource)

batch_sleep = 0.4
logs_active_range_start_list: list[int] = sorted(
    [2400, 2900, 4200, 4900, 5300, 14600, 15000, 15200, 15300, 15500, 16000, 20700, 20800, 30700, 30800, 30900, 31000, 31100, 31300, 31500, 31600, 31700, 31800, 31900, 32100, 32200, 33100, 35300, 35400, 35500, 35700, 39900, 40100, 40200, 41000, 41100, 41200, 41300, 41400, 44300, 44400, 44600, 46100, 46600, 48900, 49800, 50700, 51200, 51300, 51400, 51900, 52000, 52100, 55100, 55200, 56100, 56200, 56600, 56800, 57500, 58300, 58600, 59700, 60100, 60200, 60800, 61800, 67600, 67700, 67800, 68900, 69200, 69500, 69600, 69700, 69800, 70600, 70800, 71000, 72700, 76900, 77700, 77800, 88400, 88500, 89100, 89200, 91800, 91900, 92100, 92500, 92800, 93400, 93600, 100800, 100900, 101300, 101600, 101700, 101900, 102300, 102400, 102900, 103000, 103100, 103200, 103400, 103500, 103600, 103800, 104400, 161700, 170100, 171500, 173400, 173900, 174000, 174100, 174200, 175300, 175400, 175500, 175600, 175700, 176400, 176500, 177400, 179400, 180800, 181500, 181800, 200100, 200300]
)
async def validate_logs_batch_level(batch_first_level, batch_last_level):
    for index in logs_active_range_start_list:
        active_range = range(index, index+100)
        if batch_first_level in active_range or batch_last_level in active_range:
            await asyncio.sleep(batch_sleep)
            return True

    return batch_first_level >= logs_active_range_start_list[-1]+100


tx_active_levels_list: list[int] = sorted(
    [2457, 2576, 2813, 2817, 2838, 2939, 2960, 2970, 2983, 3021, 4203, 4243, 4919, 4952, 4992, 5356, 5361, 5367, 14574, 14664, 14670, 14695, 14703, 14710, 14762, 14766, 15074, 15123, 15176, 15270, 15328, 15382, 15384, 15544, 15592, 15995, 16009, 20783, 20799, 20817, 20821, 20826, 20842, 20863, 30737, 30758, 30779, 30815, 30912, 30915, 30917, 30929, 30944, 30950, 30954, 30965, 31057, 31069, 31097, 31126, 31132, 31141, 31332, 31452, 31484, 31539, 31549, 31565, 31568, 31569, 31572, 31575, 31653, 31746, 31752, 31765, 31766, 31767, 31769, 31773, 31776, 31844, 31902, 31913, 32128, 32145, 32146, 32147, 32151, 32154, 32170, 32229, 33149, 33151, 33164, 35363, 35366, 35368, 35369, 35371, 35402, 35408, 35476, 35485, 35504, 35505, 35509, 35510, 35727, 35741, 39936, 40120, 40124, 40131, 40194, 40224, 40225, 41072, 41074, 41187, 41189, 41191, 41192, 41213, 41215, 41216, 41217, 41250, 41251, 41252, 41253, 41340, 41342, 41344, 41345, 41405, 41406, 41407, 41408, 44304, 44307, 44313, 44475, 44477, 44603, 44605, 46105, 46108, 46648, 46652, 48974, 48978, 49820, 49823, 49840, 49843, 50723, 50729, 50746, 50747, 50759, 50760, 50767, 51296, 51299, 51305, 51424, 51428, 51432, 51457, 51782, 51913, 51916, 51966, 52018, 52128, 52138, 52147, 52150, 52158, 55143, 55250, 55253, 55266, 55269, 56089, 56117, 56182, 56210, 56218, 56221, 56253, 56256, 56610, 56630, 56633, 56653, 56670, 56672, 56687, 56690, 56840, 57528, 58388, 58677, 58678, 59751, 59756, 59788, 59791, 59961, 60135, 60221, 60617, 60804, 60807, 61848, 61852, 61854, 61856, 61859, 61862, 66912, 67680, 67683, 67757, 67766, 67778, 67802, 67838, 67873, 67964, 68021, 68501, 68913, 68916, 68922, 68927, 69264, 69559, 69576, 69579, 69663, 69757, 69760, 69885, 69891, 70699, 70849, 71012, 71033, 71038, 71068, 71074, 72732, 76904, 76946, 77789, 77839, 80005, 82480, 82505, 82509, 82629, 88438, 88440, 88481, 88551, 88589, 88596, 88644, 88663, 88668, 88685, 88989, 88995, 89115, 89171, 89182, 89207, 89234, 89252, 89282, 90669, 90805, 90827, 90833, 91883, 91884, 91937, 91944, 91946, 91973, 92115, 92121, 92146, 92159, 92171, 92175, 92178, 92560, 92577, 92613, 92635, 92656, 92679, 92824, 92850, 92868, 93435, 93621, 99450, 99518, 100897, 100909, 101334, 101343, 101358, 101373, 101380, 101385, 101387, 101601, 101606, 101610, 101622, 101628, 101634, 101645, 101656, 101662, 101665, 101678, 101691, 101697, 101701, 101715, 101968, 102226, 102235, 102242, 102252, 102264, 102269, 102274, 102288, 102293, 102312, 102316, 102453, 102489, 102986, 102991, 102997, 103017, 103049, 103052, 103064, 103071, 103087, 103154, 103216, 103227, 103495, 103530, 103544, 103573, 103582, 103665, 103667, 103829, 104445, 161078, 161083, 161778, 170189, 171548, 171549, 171551, 171553, 171569, 171570, 171571, 171573, 173456, 173990, 173992, 174028, 174043, 174096, 174110, 174114, 174126, 174167, 174186, 174188, 174189, 174214, 174215, 174261, 174262, 175394, 175398, 175410, 175411, 175482, 175484, 175486, 175487, 175490, 175492, 175494, 175524, 175558, 175692, 175696, 175697, 175698, 175704, 175707, 176409, 176418, 176425, 176428, 176430, 176435, 176437, 176440, 176583, 176585, 176587, 176591, 176594, 176597, 177413, 177415, 177416, 177418, 177419, 177420, 177488, 177491, 177493, 177495, 177496, 177497, 179397, 179408, 179414, 179418, 179419, 179431, 179489, 180807, 180810, 180812, 180814, 180816, 180818, 181553, 181565, 181842, 181844, 200116, 200120, 200141, 200143, 200337, 200340, 200342, 200376]
)
async def validate_tx_level(level):
    if level in tx_active_levels_list:
        await asyncio.sleep(batch_sleep)
        return True

    return level > tx_active_levels_list[-1]


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


class SubsquidIndex(
    Index[IndexConfigT, IndexQueueItemT, DatasourceT],
    ABC,
    Generic[IndexConfigT, IndexQueueItemT, DatasourceT],
    message_type=SubsquidMessageType,
):
    def __init__(
        self,
        ctx: DipDupContext,
        config: IndexConfigT,
        datasource: DatasourceT,
    ) -> None:
        super().__init__(ctx, config, datasource)

        node_field = self._config.datasource.node
        if node_field is None:
            node_field = ()
        elif isinstance(node_field, EvmNodeDatasourceConfig):
            node_field = (node_field,)
        self._node_datasources = tuple(
            self._ctx.get_evm_node_datasource(node_config.name) for node_config in node_field
        )

    @abstractmethod
    async def _synchronize_subsquid(self, sync_level: int) -> None: ...

    @abstractmethod
    async def _synchronize_node(self, sync_level: int) -> None: ...

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

    @property
    def node_datasources(self) -> tuple[EvmNodeDatasource, ...]:
        return self._node_datasources

    def get_random_node(self) -> EvmNodeDatasource:
        if not self._node_datasources:
            raise FrameworkException('A node datasource requested, but none attached to this index')
        return random.choice(self._node_datasources)

    def get_sync_level(self) -> int:
        """Get level index needs to be synchronized to depending on its subscription status"""
        sync_levels = set()
        for sub in self._config.get_subscriptions():
            sync_levels.add(self.datasource.get_sync_level(sub))
            for datasource in self.node_datasources or ():
                sync_levels.add(datasource.get_sync_level(sub))

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
            blocks[level] = await self.get_random_node().get_block_by_level(
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
        logs = await self.get_random_node().get_logs(
            {
                'fromBlock': hex(first_level),
                'toBlock': hex(last_level),
            },
        )
        for log in logs:
            grouped_logs[int(log['blockNumber'], 16)].append(log)
        return grouped_logs

    async def _get_node_sync_level(self, subsquid_level: int, index_level: int) -> int | None:
        if not self.node_datasources:
            return None

        node_sync_level = await self.get_random_node().get_head_level()
        subsquid_lag = abs(node_sync_level - subsquid_level)
        subsquid_available = subsquid_level - index_level
        self._logger.info('Subsquid is %s levels behind; %s available', subsquid_lag, subsquid_available)
        if subsquid_available < NODE_LAST_MILE:
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

        subsquid_sync_level = await self.datasource.get_head_level()
        Metrics.set_sqd_processor_chain_height(subsquid_sync_level)
        node_sync_level = await self._get_node_sync_level(subsquid_sync_level, index_level)

        # NOTE: Fetch last blocks from node if there are not enough realtime messages in queue
        if node_sync_level:
            sync_level = min(sync_level, node_sync_level)
            self._logger.debug('Using node datasource; sync level: %s', sync_level)
            await self._synchronize_node(sync_level)
        else:
            sync_level = min(sync_level, subsquid_sync_level)
            await self._synchronize_subsquid(sync_level)

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

        started_at = time.time()
        async with self._ctx.transactions.in_transaction(batch_level, sync_level, self.name):
            for handler_config, data in matched_handlers:
                await self._call_matched_handler(handler_config, data)
            await self._update_state(level=batch_level)
        metrics[f'{self.name}:time_in_callbacks'] += (time.time() - started_at) / 60
