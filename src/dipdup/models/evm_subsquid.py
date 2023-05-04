from enum import Enum
from typing import Any
from typing import Generic
from typing import TypeVar

from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from dipdup.models.evm_node import EvmNodeLogData

PayloadT = TypeVar('PayloadT', bound=BaseModel)


# FIXME: Outdated values
class SubsquidMessageType(Enum):
    """Enum for filenames in squid archives"""

    blocks = 'blocks.arrow_stream'
    logs = 'logs.arrow_stream'


@dataclass
class SubsquidEventData:
    address: str
    block_hash: str
    # block_number: int
    data: str
    index: int
    # removed: bool
    topics: tuple[str, ...]
    transaction_hash: str
    transaction_index: int
    level: int
    # TODO: timestamp

    @classmethod
    def from_json(
        cls,
        event_json: dict[str, Any],
    ) -> 'SubsquidEventData':
        return SubsquidEventData(
            address=event_json['address'],
            data=event_json['data'],
            topics=tuple(event_json['topics']),
            level=event_json['blockNumber'],
            index=event_json['index'],
            block_hash=event_json['blockHash'],
            transaction_hash=event_json['transactionHash'],
            transaction_index=event_json['transactionIndex'],
        )

    @property
    def block_number(self) -> int:
        return self.level


@dataclass
class SubsquidEvent(Generic[PayloadT]):
    data: SubsquidEventData | EvmNodeLogData
    payload: PayloadT


class SubsquidOperation:
    ...