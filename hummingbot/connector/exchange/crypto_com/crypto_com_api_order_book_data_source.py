import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.crypto_com import (
    crypto_com_constants as CONSTANTS,
    crypto_com_web_utils as web_utils,
)
from hummingbot.connector.exchange.crypto_com.crypto_com_order_book import CryptoComOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.crypto_com.crypto_com_exchange import CryptoComExchange


class CryptoComAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CryptoComExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.WS_TRADE_CHANNEL
        self._diff_messages_queue_key = CONSTANTS.WS_BOOK_CHANNEL
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {
            "instrument_name": symbol,
            "depth": 150  # Maximum depth for Crypto.com
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.BOOK_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.BOOK_PATH_URL,
        )

        return data

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Identify the channel that originated the message
        """
        try:
            # For Crypto.com WebSocket messages, check if it's a subscription result
            if event_message.get("method") == "subscribe":
                result = event_message.get("result", {})
                channel = result.get("channel", "")
                return channel

            # For other message types, try to extract channel info
            if "channel" in event_message:
                return event_message["channel"]

            # Default fallback
            return ""
        except Exception:
            return ""

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

                # Subscribe to order book updates
                book_channel = f"{CONSTANTS.WS_BOOK_CHANNEL}.{symbol}"
                book_request = WSJSONRequest(payload={
                    "method": "subscribe",
                    "params": {
                        "channels": [book_channel]
                    },
                    "nonce": int(time.time() * 1000)
                })
                await ws.send(book_request)

                # Subscribe to trades
                trade_channel = f"{CONSTANTS.WS_TRADE_CHANNEL}.{symbol}"
                trade_request = WSJSONRequest(payload={
                    "method": "subscribe",
                    "params": {
                        "channels": [trade_channel]
                    },
                    "nonce": int(time.time() * 1000)
                })
                await ws.send(trade_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = web_utils.wss_url(domain=self._domain)
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Creates an order book snapshot message
        """
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        update_id: int = int(snapshot_timestamp * 1000)

        if snapshot_response.get("code") == 0 and "result" in snapshot_response:
            result_data = snapshot_response["result"]["data"][0]

            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": [(bid[0], bid[1]) for bid in result_data.get("bids", [])],
                "asks": [(ask[0], ask[1]) for ask in result_data.get("asks", [])],
            }

            snapshot_msg: OrderBookMessage = CryptoComOrderBook.snapshot_message_from_exchange(
                order_book_message_content,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            return snapshot_msg
        else:
            raise ValueError(f"Invalid snapshot response: {snapshot_response}")

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse trade message from websocket
        """
        if raw_message.get("method") == "subscribe":
            result = raw_message.get("result", {})
            channel = result.get("channel", "")

            if CONSTANTS.WS_TRADE_CHANNEL in channel:
                data = result.get("data", [])

                for trade_data in data:
                    symbol = trade_data.get("instrument_name")
                    if symbol:
                        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                        trade_message_content = {
                            "trading_pair": trading_pair,
                            "trade_type": trade_data["side"].lower(),
                            "trade_id": trade_data["trade_id"],
                            "update_id": trade_data["trade_id"],
                            "price": trade_data["price"],
                            "amount": trade_data["quantity"],
                        }

                        trade_message: OrderBookMessage = CryptoComOrderBook.trade_message_from_exchange(
                            trade_message_content,
                            trade_data["dataTime"] / 1000,
                            metadata={"trading_pair": trading_pair}
                        )
                        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse order book diff message from websocket
        """
        if raw_message.get("method") == "subscribe":
            result = raw_message.get("result", {})
            channel = result.get("channel", "")

            if CONSTANTS.WS_BOOK_CHANNEL in channel:
                data = result.get("data", [])

                for book_data in data:
                    symbol = book_data.get("instrument_name")
                    if symbol:
                        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                        diff_message_content = {
                            "trading_pair": trading_pair,
                            "update_id": book_data.get("update_id", int(time.time() * 1000)),
                            "bids": [(bid[0], bid[1]) for bid in book_data.get("bids", [])],
                            "asks": [(ask[0], ask[1]) for ask in book_data.get("asks", [])],
                        }

                        diff_message: OrderBookMessage = CryptoComOrderBook.diff_message_from_exchange(
                            diff_message_content,
                            book_data.get("t", time.time() * 1000) / 1000,
                            metadata={"trading_pair": trading_pair}
                        )
                        message_queue.put_nowait(diff_message)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket trade channel
        """
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    data = ws_response.data
                    if isinstance(data, dict):
                        await self._parse_trade_message(data, output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                    exc_info=True
                )
                await self._sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for order book diffs using websocket order book channel
        """
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    data = ws_response.data
                    if isinstance(data, dict):
                        await self._parse_order_book_diff_message(data, output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                    exc_info=True
                )
                await self._sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for order book snapshots by fetching the full order book
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: OrderBookMessage = await self._order_book_snapshot(trading_pair)
                        output.put_nowait(snapshot)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error(f"Unexpected error fetching order book snapshot for {trading_pair}.",
                                            exc_info=True)
                        await self._sleep(5.0)

                await self._sleep(self.ONE_HOUR)  # Refresh snapshot every hour

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await self._sleep(5.0)
