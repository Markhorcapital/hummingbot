import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.gemini import gemini_constants as CONSTANTS, gemini_web_utils as web_utils
from hummingbot.connector.exchange.gemini.gemini_order_book import GeminiOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gemini.gemini_exchange import GeminiExchange


class GeminiAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Data source for Gemini order book and trade information.

    Gemini WebSocket V2 Market Data:
    - URL: wss://api.gemini.com/v2/marketdata
    - Subscription format: {"type": "subscribe", "subscriptions": [{"name": "l2", "symbols": [...]}]}
    - Updates include both order book changes and trades in the same feed
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'GeminiExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

        # Message queue keys
        self._trade_messages_queue_key = CONSTANTS.WS_TRADE_EVENT
        self._diff_messages_queue_key = CONSTANTS.WS_L2_UPDATES

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Get last traded prices for trading pairs.

        :param trading_pairs: List of trading pairs
        :param domain: Domain (unused)
        :return: Dict mapping trading pair to last price
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def subscribe_to_trading_pair(self, trading_pair: str):
        """Add a trading pair to the subscription list (takes effect on next WS connect)."""
        if trading_pair not in self._trading_pairs:
            self._trading_pairs.append(trading_pair)

    async def unsubscribe_from_trading_pair(self, trading_pair: str):
        """Remove a trading pair from the subscription list (takes effect on next WS connect)."""
        if trading_pair in self._trading_pairs:
            self._trading_pairs.remove(trading_pair)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieve order book snapshot from Gemini REST API.

        Endpoint: GET /v1/book/{symbol}
        No authentication required (public endpoint).

        Response format:
        {
            "bids": [{"price": "50000.00", "amount": "0.5"}, ...],
            "asks": [{"price": "50001.00", "amount": "0.4"}, ...]
        }

        :param trading_pair: Trading pair in Hummingbot format
        :return: Order book snapshot data
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # Build endpoint path
        snapshot_path = CONSTANTS.ORDER_BOOK_PATH_URL.replace("{symbol}", symbol)

        # Add parameters for limiting order book depth if needed
        params = {
            "limit_bids": 50,  # Limit to top 50 levels
            "limit_asks": 50
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=snapshot_path, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to Gemini WebSocket channels.

        Gemini WebSocket V2 subscription format:
        {
            "type": "subscribe",
            "subscriptions": [
                {
                    "name": "l2",
                    "symbols": ["BTCUSD", "ETHUSD", ...]
                }
            ]
        }

        The "l2" channel provides both order book updates and trades.

        :param ws: WebSocket assistant
        """
        try:
            # Convert trading pairs to Gemini symbols
            symbols = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                symbols.append(symbol)

            # Build subscription message
            payload = {
                "type": "subscribe",
                "subscriptions": [
                    {
                        "name": CONSTANTS.WS_L2_CHANNEL,  # "l2"
                        "symbols": symbols
                    }
                ]
            }

            subscribe_request = WSJSONRequest(payload=payload)
            await ws.send(subscribe_request)

            self.logger().info(f"Subscribed to Gemini L2 order book channels for {len(symbols)} symbols...")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to Gemini order book streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Create and connect WebSocket assistant for public market data.

        :return: Connected WebSocket assistant
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.wss_public_url(self._domain),
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        )
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Fetch order book snapshot and convert to OrderBookMessage.

        :param trading_pair: Trading pair in Hummingbot format
        :return: OrderBookMessage with snapshot
        """
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = GeminiOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse trade message from Gemini WebSocket and add to queue.

        Gemini includes trades in the L2 feed under the "trades" key.

        :param raw_message: Raw WebSocket message
        :param message_queue: Queue to add parsed message to
        """
        # Check if message contains trades
        if "trades" in raw_message and raw_message["trades"]:
            symbol = raw_message.get("symbol")
            if symbol:
                try:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                    # Process each trade in the message
                    for trade in raw_message["trades"]:
                        trade["symbol"] = symbol  # Ensure symbol is in trade data
                        trade_message = GeminiOrderBook.trade_message_from_exchange(
                            trade,
                            metadata={"trading_pair": trading_pair}
                        )
                        message_queue.put_nowait(trade_message)

                except Exception:
                    self.logger().exception(f"Error parsing trade message for {symbol}")

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse order book diff message from Gemini WebSocket and add to queue.

        Gemini L2 updates format:
        {
            "type": "l2_updates",
            "symbol": "BTCUSD",
            "changes": [
                ["buy", "50000.00", "0.5"],  # [side, price, amount]
                ["sell", "50001.00", "0.3"],
                ...
            ]
        }

        :param raw_message: Raw WebSocket message (or OrderBookMessage if from parent class)
        :param message_queue: Queue to add parsed message to
        """
        # Handle case where parent class passes an OrderBookMessage instead of raw dict
        if isinstance(raw_message, OrderBookMessage):
            # Already processed, just add to queue
            message_queue.put_nowait(raw_message)
            return

        # Check if this is an L2 update message
        if raw_message.get("type") == CONSTANTS.WS_L2_UPDATES:
            symbol = raw_message.get("symbol")
            if symbol:
                try:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                    # Convert Gemini changes format to standard format
                    # Gemini: [["buy", "price", "amount"], ...]
                    # Standard: {"bids": [[price, amount]], "asks": [[price, amount]]}
                    changes = raw_message.get("changes", [])
                    bids = []
                    asks = []

                    for change in changes:
                        if len(change) >= 3:
                            side, price, amount = change[0], change[1], change[2]
                            if side == "buy":
                                bids.append([price, amount])
                            elif side == "sell":
                                asks.append([price, amount])

                    # Create formatted message
                    formatted_msg = {
                        "trading_pair": trading_pair,
                        "bids": bids,
                        "asks": asks,
                        "update_id": int(time.time() * 1000)  # Use timestamp as update ID
                    }

                    order_book_message: OrderBookMessage = GeminiOrderBook.diff_message_from_exchange(
                        formatted_msg,
                        time.time(),
                        metadata={"trading_pair": trading_pair}
                    )
                    message_queue.put_nowait(order_book_message)

                except Exception:
                    self.logger().exception(f"Error parsing order book diff for {symbol}")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determine which channel a message belongs to.

        Gemini combines trades and order book updates in the L2 feed.

        :param event_message: WebSocket message
        :return: Channel identifier
        """
        message_type = event_message.get("type", "")

        # Heartbeat messages
        if message_type == CONSTANTS.WS_HEARTBEAT_EVENT:
            return ""

        # Subscription acknowledgment
        elif message_type == CONSTANTS.WS_SUBSCRIPTION_ACK:
            return ""

        # L2 updates (order book changes)
        elif message_type == CONSTANTS.WS_L2_UPDATES:
            return self._diff_messages_queue_key

        # Trade events (included in L2 feed)
        elif "trades" in event_message:
            return self._trade_messages_queue_key

        return ""

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        """
        Process messages from Gemini WebSocket.

        Overridden to handle Gemini's combined L2 feed.

        :param websocket_assistant: WebSocket assistant
        """
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data

            # Handle different message types
            if isinstance(data, dict):
                msg_type = data.get("type", "")

                # Heartbeat - ignore
                if msg_type == CONSTANTS.WS_HEARTBEAT_EVENT:
                    continue

                # Subscription acknowledgment
                elif msg_type == CONSTANTS.WS_SUBSCRIPTION_ACK:
                    self.logger().info("Gemini WebSocket subscription acknowledged")
                    continue

                # L2 updates - contains both order book changes and trades
                elif msg_type == CONSTANTS.WS_L2_UPDATES:
                    # Parse order book changes
                    await self._parse_order_book_diff_message(data, self._message_queue[self._diff_messages_queue_key])

                    # Parse trades if present
                    if "trades" in data:
                        await self._parse_trade_message(data, self._message_queue[self._trade_messages_queue_key])
