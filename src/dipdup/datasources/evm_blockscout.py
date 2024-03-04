import asyncio
import zipfile
from collections import defaultdict
from collections import deque
from collections.abc import AsyncIterator
from io import BytesIO
from typing import Any

import pyarrow.ipc  # type: ignore[import-untyped]

from dipdup.config import HttpConfig
from dipdup.config.evm_blockscout import BlockscoutDatasourceConfig
from dipdup.datasources import IndexDatasource
from dipdup.exceptions import DatasourceError
from dipdup.models.evm_blockscout import FieldSelection
from dipdup.models.evm_blockscout import LogRequest
from dipdup.models.evm_blockscout import Query
from dipdup.models.evm_blockscout import BlockscoutEventData
from dipdup.models.evm_blockscout import BlockscoutTransactionData
from dipdup.models.evm_blockscout import TransactionRequest

POLL_INTERVAL = 1.0
LOG_FIELDS: FieldSelection = {
    'block': {
        'timestamp': True,
    },
    'log': {
        'logIndex': True,
        'transactionIndex': True,
        'transactionHash': True,
        'address': True,
        'data': True,
        'topics': True,
    },
}
TRANSACTION_FIELDS: FieldSelection = {
    'block': {
        'timestamp': True,
    },
    'transaction': {
        'chainId': True,
        'contractAddress': True,
        'cumulativeGasUsed': True,
        'effectiveGasPrice': True,
        'from': True,
        'gasPrice': True,
        'gas': True,
        'gasUsed': True,
        'hash': True,
        'input': True,
        'maxFeePerGas': True,
        'maxPriorityFeePerGas': True,
        'nonce': True,
        'r': True,
        'sighash': True,
        'status': True,
        's': True,
        'to': True,
        'transactionIndex': True,
        'type': True,
        'value': True,
        'v': True,
        'yParity': True,
    },
}


def unpack_data(content: bytes) -> dict[str, list[dict[str, Any]]]:
    """Extract data from Blockscout zip+pyarrow archives"""
    data = {}
    with zipfile.ZipFile(BytesIO(content), 'r') as arch:
        for item in arch.filelist:
            with arch.open(item) as f, pyarrow.ipc.open_stream(f) as reader:
                table: pyarrow.Table = reader.read_all()
                data[item.filename] = table.to_pylist()
    return data


class BlockscoutDatasource(IndexDatasource[BlockscoutDatasourceConfig]):
    _default_http_config = HttpConfig()

    def __init__(self, config: BlockscoutDatasourceConfig) -> None:
        super().__init__(config, False)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            await self.initialize()

    async def subscribe(self) -> None:
        pass


    async def iter_event_logs(
        self,
        topics: tuple[tuple[str | None, str], ...],
        first_level: int,
        last_level: int,
    ) -> AsyncIterator[tuple[BlockscoutEventData, ...]]:
        current_level = first_level

        topics_by_address = defaultdict(list)
        for address, topic in topics:
            address = address.replace('0x', '\\x')
            topic = topic.replace('0x', '\\x')
            topics_by_address[address].append(topic)

        filter_list = []
        for address, topic_list in topics_by_address.items():
            filter_list.append({'_and': {
                'address_hash': {'_eq': address},
                'first_topic': {'_in': topic_list}
            }})

        while current_level <= last_level:
            query = """
($block_number: Int_comparison_exp, $handler_filter: [logs_bool_exp!], $limit: Int) {
  logs(where: {block_number: $block_number, _or: $handler_filter}, limit: $limit, order_by: {block_number: asc, index: asc}) {
    block_number
    block {
      timestamp
    }
    log_index: index
    transaction {
      transaction_index: index
      transaction_hash: hash
    }
    topic_0: first_topic
    topic_1: second_topic
    topic_2: third_topic
    topic_3: fourth_topic
    data
    address: address_hash
  }
}            
            """

            variables = {
                'limit': 1000,
                'block_number': {'_gte': current_level, '_lte': last_level},
                'handler_filter': filter_list,
            }
            response = await self.query(query=query, variables=variables)

            if not response['logs']:
                current_level = last_level + 1
                continue

            levels = {}
            for raw_log in response['logs']:
                level = raw_log['block_number']
                levels.setdefault(level, [])
                levels[level].append(raw_log)

            for level in levels:
                current_level = level + 1
                logs: deque[BlockscoutEventData] = deque()
                for raw_log in levels[level]:
                    logs.append(
                        BlockscoutEventData.from_json(raw_log, level),
                    )
                self._logger.info(f'Yield {len(logs)} events from level {level}...')
                yield tuple(logs)

    async def iter_transactions(
        self,
        first_level: int,
        last_level: int,
        filters: tuple[TransactionRequest, ...],
    ) -> AsyncIterator[tuple[BlockscoutTransactionData, ...]]:
        current_level = first_level

        while current_level <= last_level:
            query: Query = {
                'fields': TRANSACTION_FIELDS,
                'fromBlock': current_level,
                'toBlock': last_level,
                'transactions': list(filters),
            }
            response = await self.query_worker(query, current_level)

            for level_item in response:
                level = level_item['header']['number']
                timestamp = level_item['header']['timestamp']
                current_level = level + 1
                transactions: deque[BlockscoutTransactionData] = deque()
                for raw_transaction in level_item['transactions']:
                    transaction = BlockscoutTransactionData.from_json(raw_transaction, level, timestamp)
                    # NOTE: `None` falue is for chains and block ranges not compliant with the post-Byzantinum
                    # hard fork EVM specification (e.g. before 4.370,000 on Ethereum).
                    if transaction.status != 0:
                        transactions.append(transaction)
                yield tuple(transactions)

    async def initialize(self) -> None:
        level = await self.get_head_level()

        if not level:
            raise DatasourceError('Blockscout is not ready yet', self.name)

        self.set_sync_level(None, level)

    async def get_head_level(self) -> int:
        response = await self.query('{ blocks(order_by: {number: desc}, limit: 1) { head: number } }')
        return int(response['blocks'][0]['head'])

    async def query(self, query: str, name: str | None = None, variables: dict | None = None) -> dict:
        if variables is None:
            variables = {}
        payload = {
            'query': 'query '+query,
            'operationName': name,
            'variables': variables,
        }
        response = await self.request(method='post', url='v1/graphql', json=payload)
        return response['data']
