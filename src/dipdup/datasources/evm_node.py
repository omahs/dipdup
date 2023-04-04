import asyncio
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import uuid4

from pysignalr.messages import CompletionMessage

from dipdup.config import HttpConfig
from dipdup.config.evm_node import EvmNodeDatasourceConfig
from dipdup.datasources import IndexDatasource
from dipdup.exceptions import DatasourceError
from dipdup.models import MessageType
from dipdup.models.evm_node import NodeSubscription
from dipdup.pysignalr import Message
from dipdup.pysignalr import WebsocketMessage
from dipdup.pysignalr import WebsocketProtocol
from dipdup.pysignalr import WebsocketTransport

EmptyCallback = Callable[[], Awaitable[None]]
HeadCallback = Callable[['EvmNodeDatasource', dict[str, Any]], Awaitable[None]]
RollbackCallback = Callable[['IndexDatasource', MessageType, int, int], Awaitable[None]]


class EvmNodeDatasource(IndexDatasource[EvmNodeDatasourceConfig]):
    _default_http_config = HttpConfig()

    def __init__(self, config: EvmNodeDatasourceConfig, merge_subscriptions: bool = False) -> None:
        super().__init__(config, merge_subscriptions)
        self._ws_client: WebsocketTransport | None = None
        self._requests: dict[str, tuple[asyncio.Event, Any]] = {}
        self._subscription_ids: dict[str, NodeSubscription] = {}

        self._on_connected_callbacks: set[EmptyCallback] = set()
        self._on_disconnected_callbacks: set[EmptyCallback] = set()
        self._on_rollback_callbacks: set[RollbackCallback] = set()
        self._on_head_callbacks: set[HeadCallback] = set()

    async def initialize(self) -> None:
        pass

    async def run(self) -> None:
        client = self._get_ws_client()
        await client.run()

    async def subscribe(self) -> None:
        if self._requests:
            return

        missing_subscriptions = self._subscriptions.missing_subscriptions
        if not missing_subscriptions:
            return

        self._logger.info('Subscribing to %s channels', len(missing_subscriptions))
        for subscription in missing_subscriptions:
            if isinstance(subscription, NodeSubscription):
                await self._subscribe(subscription)

        self._logger.info('Subscribed to %s channels', len(missing_subscriptions))

    async def emit_rollback(self, type_: MessageType, from_level: int, to_level: int) -> None:
        for fn in self._on_rollback_callbacks:
            await fn(self, type_, from_level, to_level)

    async def emit_connected(self) -> None:
        for fn in self._on_connected_callbacks:
            await fn()

    async def emit_disconnected(self) -> None:
        for fn in self._on_disconnected_callbacks:
            await fn()

    async def emit_head(self, head: dict[str, Any]) -> None:
        for fn in self._on_head_callbacks:
            await fn(self, head)

    def call_on_head(self, fn: HeadCallback) -> None:
        self._on_head_callbacks.add(fn)

    def call_on_connected(self, fn: EmptyCallback) -> None:
        self._on_connected_callbacks.add(fn)

    def call_on_disconnected(self, fn: EmptyCallback) -> None:
        self._on_disconnected_callbacks.add(fn)

    async def _subscribe(self, subscription: NodeSubscription) -> None:
        self._logger.debug('Subscribing to %s', subscription)
        response = await self._jsonrpc_request(
            'eth_subscribe',
            subscription.get_params(),
        )
        self._subscription_ids[response['result']] = subscription
        # FIXME
        self._subscriptions.set_sync_level(subscription, 0)

    async def _jsonrpc_request(
        self,
        method: str,
        params: Any,
    ) -> Any:
        request_id = uuid4().hex
        request = {
            'jsonrpc': '2.0',
            'id': request_id,
            'method': method,
            'params': params,
        }
        event = asyncio.Event()
        self._requests[request_id] = (event, None)

        message = WebsocketMessage(request)
        client = self._get_ws_client()
        await client.send(message)

        await event.wait()
        return self._requests[request_id][1]

    async def _on_message(self, message: Message) -> None:
        if not isinstance(message, WebsocketMessage):
            raise DatasourceError(f'Unknown message type: {type(message)}', self.name)

        data = message.data

        if 'id' in data:
            request_id = data['id']
            if request_id not in self._requests:
                raise DatasourceError(f'Unknown request ID: {data["id"]}', self.name)

            event = self._requests[request_id][0]
            self._requests[request_id] = (event, data)
            event.set()
        elif 'method' in data:
            if data['method'] == 'eth_subscription':
                subscription_id = data['params']['subscription']
                subscription = self._subscription_ids[subscription_id]
                await self._handle_subscription(subscription, data['params']['result'])
            else:
                raise DatasourceError(f'Unknown method: {data["method"]}', self.name)
        else:
            raise DatasourceError(f'Unknown message: {data}', self.name)

    async def _handle_subscription(self, subscription: NodeSubscription, data: Any) -> None:
        print(subscription, data)
        raise NotImplementedError

    async def _on_error(self, message: CompletionMessage) -> None:
        raise DatasourceError(f'Node error: {message}', self.name)

    async def _on_connected(self) -> None:
        self._logger.info('Realtime connection established')
        # NOTE: Subscribing here will block WebSocket loop, don't do it.
        await self.emit_connected()

    async def _on_disconnected(self) -> None:
        self._logger.info('Realtime connection lost, resetting subscriptions')
        self._subscriptions.reset()
        await self.emit_disconnected()

    def _get_ws_client(self) -> WebsocketTransport:
        if self._ws_client:
            return self._ws_client

        self._logger.info('Creating Websocket client')

        url = self._config.ws_url
        self._ws_client = WebsocketTransport(
            url=url,
            protocol=WebsocketProtocol(),
            callback=self._on_message,
            skip_negotiation=True,
        )

        self._ws_client.on_open(self._on_connected)
        self._ws_client.on_close(self._on_disconnected)
        self._ws_client.on_error(self._on_error)

        return self._ws_client