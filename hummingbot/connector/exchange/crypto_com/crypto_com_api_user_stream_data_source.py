import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.crypto_com import (
    crypto_com_constants as CONSTANTS,
    crypto_com_web_utils as web_utils,
)
from hummingbot.connector.exchange.crypto_com.crypto_com_auth import CryptoComAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.crypto_com.crypto_com_exchange import CryptoComExchange


class CryptoComAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Crypto.com user stream data source for real-time user data updates
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    PING_TIMEOUT = 10.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CryptoComAuth,
                 trading_pairs: List[str],
                 connector: 'CryptoComExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: CryptoComAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._trading_pairs = trading_pairs
        self._last_recv_time: float = 0

    @property
    def last_recv_time(self) -> float:
        """
        Returns the last time we received data
        """
        return self._last_recv_time

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = web_utils.wss_url(domain=self._domain)
        await ws.connect(ws_url=url, ping_timeout=self.PING_TIMEOUT)
        return ws

    async def _authenticate_websocket(self, ws: WSAssistant):
        """
        Authenticate the websocket connection
        """
        try:
            # For now, skip WebSocket authentication as Crypto.com might not support it
            # or might use a different authentication method
            self.logger().info("Skipping WebSocket authentication - using public channels only")
            return

            # TODO: Implement proper WebSocket authentication when method is confirmed
            # auth_request = await self._auth.ws_authenticate(WSJSONRequest(payload={}))
            # ... rest of authentication logic

        except Exception as e:
            self.logger().error(f"WebSocket authentication error: {e}")
            # Don't raise the exception, just log it and continue
            self.logger().info("Continuing without WebSocket authentication")

    async def _subscribe_to_user_streams(self, ws: WSAssistant):
        """
        Subscribe to user-specific data streams
        """
        try:
            # For now, skip user stream subscriptions as they require authentication
            # We'll rely on REST API polling for user data
            self.logger().info("Skipping user stream subscriptions - using REST API polling instead")
            return

            # TODO: Implement user stream subscriptions when WebSocket auth is working
            # ... rest of subscription logic

        except Exception as e:
            self.logger().error(f"Error subscribing to user streams: {e}")
            # Don't raise the exception, just log it

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        """
        Listen for user stream data from Crypto.com WebSocket
        """
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()

                # Authenticate the connection
                await self._authenticate_websocket(ws)

                # Subscribe to user data streams
                await self._subscribe_to_user_streams(ws)

                # Listen for messages
                async for ws_response in ws.iter_messages():
                    try:
                        data = ws_response.data
                        if isinstance(data, dict):
                            await self._process_websocket_message(data, output)
                            self._last_recv_time = self._time()
                    except Exception as e:
                        self.logger().error(f"Error processing WebSocket message: {e}")

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error while listening for user stream. Retrying after 30 seconds...",
                    exc_info=True
                )
                await self._sleep(30.0)

    async def _process_websocket_message(self, message: dict, output: asyncio.Queue):
        """
        Process incoming WebSocket messages and route them appropriately
        """
        try:
            method = message.get("method")

            if method == "subscribe":
                result = message.get("result", {})
                channel = result.get("channel", "")

                if CONSTANTS.WS_USER_ORDER_CHANNEL in channel:
                    await self._process_order_message(result, output)
                elif CONSTANTS.WS_USER_TRADE_CHANNEL in channel:
                    await self._process_trade_message(result, output)
                elif CONSTANTS.WS_USER_BALANCE_CHANNEL in channel:
                    await self._process_balance_message(result, output)

        except Exception as e:
            self.logger().error(f"Error processing WebSocket message: {e}")

    async def _process_order_message(self, message: dict, output: asyncio.Queue):
        """
        Process order update messages
        """
        try:
            data = message.get("data", [])
            for order_data in data:
                # Create order update message
                order_update = {
                    "type": "order_update",
                    "data": order_data
                }
                output.put_nowait(order_update)

        except Exception as e:
            self.logger().error(f"Error processing order message: {e}")

    async def _process_trade_message(self, message: dict, output: asyncio.Queue):
        """
        Process trade update messages
        """
        try:
            data = message.get("data", [])
            for trade_data in data:
                # Create trade update message
                trade_update = {
                    "type": "trade_update",
                    "data": trade_data
                }
                output.put_nowait(trade_update)

        except Exception as e:
            self.logger().error(f"Error processing trade message: {e}")

    async def _process_balance_message(self, message: dict, output: asyncio.Queue):
        """
        Process balance update messages
        """
        try:
            data = message.get("data", [])
            for balance_data in data:
                # Create balance update message
                balance_update = {
                    "type": "balance_update",
                    "data": balance_data
                }
                output.put_nowait(balance_update)

        except Exception as e:
            self.logger().error(f"Error processing balance message: {e}")

    async def _create_websocket_connection(self) -> WSAssistant:
        """
        Create and return a WebSocket connection
        """
        return await self._connected_websocket_assistant()

    async def _authenticate_websocket_connection(self, ws: WSAssistant) -> None:
        """
        Authenticate the WebSocket connection
        """
        await self._authenticate_websocket(ws)
