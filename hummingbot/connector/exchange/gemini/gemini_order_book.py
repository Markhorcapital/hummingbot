import logging
import time
from typing import Dict, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.logger import HummingbotLogger


class GeminiOrderBook(OrderBook):
    """
    Order book for Gemini exchange.

    Handles parsing of Gemini-specific order book messages.
    """

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict,
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Convert Gemini order book snapshot to OrderBookMessage.

        Gemini snapshot format (from GET /v1/book/{symbol}):
        {
            "bids": [
                {"price": "50000.00", "amount": "0.5"},
                {"price": "49999.00", "amount": "0.3"},
                ...
            ],
            "asks": [
                {"price": "50001.00", "amount": "0.4"},
                {"price": "50002.00", "amount": "0.2"},
                ...
            ]
        }

        Hummingbot expects bids/asks as list of lists: [["price", "amount"], ...]

        :param msg: Snapshot data from exchange
        :param timestamp: Snapshot timestamp
        :param metadata: Additional metadata (trading_pair, etc.)
        :return: OrderBookMessage
        """
        # Convert Gemini's object format to Hummingbot's array format
        converted_msg = {
            "bids": [[entry["price"], entry["amount"]] for entry in msg.get("bids", [])],
            "asks": [[entry["price"], entry["amount"]] for entry in msg.get("asks", [])],
            "update_id": int(timestamp * 1000)  # Add update_id using timestamp in milliseconds
        }

        if metadata:
            converted_msg.update(metadata)

        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=converted_msg,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict,
                                   timestamp: float,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Convert Gemini order book update to OrderBookMessage.

        Gemini L2 WebSocket update format:
        {
            "type": "l2_updates",
            "symbol": "BTCUSD",
            "changes": [
                ["buy", "50000.00", "0.5"],    # [side, price, amount]
                ["sell", "50001.00", "0.3"],
                ...
            ],
            "trades": [
                {
                    "type": "trade",
                    "symbol": "BTCUSD",
                    "event_id": 123456,
                    "timestamp": 1234567890,
                    "price": "50000.00",
                    "quantity": "0.1",
                    "side": "buy"
                }
            ]
        }

        :param msg: Update data from WebSocket
        :param timestamp: Update timestamp
        :param metadata: Additional metadata (trading_pair, etc.)
        :return: OrderBookMessage
        """
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Convert Gemini trade event to OrderBookMessage.

        Gemini trade format (from L2 WebSocket):
        {
            "type": "trade",
            "symbol": "BTCUSD",
            "event_id": 123456,
            "timestamp": 1234567890,
            "price": "50000.00",
            "quantity": "0.1",
            "side": "buy"
        }

        :param msg: Trade data from WebSocket
        :param metadata: Additional metadata (trading_pair, etc.)
        :return: OrderBookMessage
        """
        if metadata:
            msg.update(metadata)

        # Use timestamp from message if available, otherwise use current time
        timestamp = msg.get("timestamp", msg.get("timestampms", time.time() * 1000)) / 1000

        return OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )
