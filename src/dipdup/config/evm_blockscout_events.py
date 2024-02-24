from __future__ import annotations

from dataclasses import field
from typing import TYPE_CHECKING
from typing import Literal

from pydantic.dataclasses import dataclass

from dipdup.config import AbiDatasourceConfig
from dipdup.config import HandlerConfig
from dipdup.config.evm import EvmContractConfig
from dipdup.config.evm_blockscout import BlockscoutDatasourceConfig
from dipdup.config.evm_blockscout import BlockscoutIndexConfig
from dipdup.models.evm_node import EvmNodeHeadSubscription
from dipdup.models.evm_node import EvmNodeLogsSubscription
from dipdup.utils import pascal_to_snake
from dipdup.utils import snake_to_pascal

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dipdup.subscriptions import Subscription


@dataclass
class BlockscoutEventsHandlerConfig(HandlerConfig):
    """Blockscout event handler

    :param callback: Callback name
    :param contract: EVM contract
    :param name: Event name
    """

    contract: EvmContractConfig
    name: str

    def iter_imports(self, package: str) -> Iterator[tuple[str, str]]:
        yield 'dipdup.context', 'HandlerContext'
        yield 'dipdup.models.evm_blockscout', 'BlockscoutEvent'
        yield package, 'models as models'

        event_cls = snake_to_pascal(self.name)
        event_module = pascal_to_snake(self.name)
        module_name = self.contract.module_name
        yield f'{package}.types.{module_name}.evm_events.{event_module}', event_cls

    def iter_arguments(self) -> Iterator[tuple[str, str]]:
        event_cls = snake_to_pascal(self.name)
        yield 'ctx', 'HandlerContext'
        yield 'event', f'BlockscoutEvent[{event_cls}]'


@dataclass
class BlockscoutEventsIndexConfig(BlockscoutIndexConfig):
    """Blockscout datasource config

    :param kind: Always 'evm.blockscout.events'
    :param datasource: Blockscout datasource
    :param handlers: Event handlers
    :param abi: One or more `evm.abi` datasource(s) for the same network
    :param first_level: Level to start indexing from
    :param last_level: Level to stop indexing and disable this index
    """

    kind: Literal['evm.blockscout.events']
    datasource: BlockscoutDatasourceConfig
    handlers: tuple[BlockscoutEventsHandlerConfig, ...] = field(default_factory=tuple)
    abi: AbiDatasourceConfig | tuple[AbiDatasourceConfig, ...] | None = None

    first_level: int = 0
    last_level: int = 0

    def get_subscriptions(self) -> set[Subscription]:
        subs: set[Subscription] = {EvmNodeHeadSubscription()}
        for handler in self.handlers:
            if address := handler.contract.address:
                subs.add(EvmNodeLogsSubscription(address=address))
        return subs
