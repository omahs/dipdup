from __future__ import annotations

from abc import ABC
from typing import Literal

from pydantic import validator
from pydantic.dataclasses import dataclass

from dipdup.config import HttpConfig
from dipdup.config import IndexConfig
from dipdup.config import IndexDatasourceConfig
from dipdup.exceptions import ConfigurationError


@dataclass
class BlockscoutDatasourceConfig(IndexDatasourceConfig):
    """Blockscout datasource config

    :param kind: always 'evm.blockscout'
    :param url: URL of Blockscout GraphQL API
    :param http: HTTP client configuration
    """

    kind: Literal['evm.blockscout']
    url: str
    http: HttpConfig | None = None

    @property
    def merge_subscriptions(self) -> bool:
        return False

    @property
    def rollback_depth(self) -> int:
        return 0

    @validator('url')
    def _valid_url(cls, v: str) -> str:
        if not v.startswith(('http', 'https')):
            raise ConfigurationError('Blockscout Network URL must start with http(s)')
        return v


@dataclass
class BlockscoutIndexConfig(IndexConfig, ABC):
    """EVM index that use Blockscout GraphQL API as a datasource

    :param kind: starts with 'evm.blockscout'
    :param datasource: Blockscout datasource config
    """

    datasource: BlockscoutDatasourceConfig
