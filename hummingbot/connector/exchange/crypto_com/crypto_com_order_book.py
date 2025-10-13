from typing import Dict, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class CryptoComOrderBook(OrderBook):
    """
    Crypto.com-specific order book implementation
    """

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message from exchange data
        """
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: float,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message from exchange data
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
                                    msg: Dict[str, any],
                                    timestamp: float,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a trade message from exchange data
        """
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )
