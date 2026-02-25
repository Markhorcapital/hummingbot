"""
Gemini Exchange Connector for Hummingbot

This module provides integration with Gemini exchange, supporting:
- Spot trading (LIMIT, MARKET, LIMIT_MAKER orders)
- Real-time order book data via WebSocket
- Real-time order updates via WebSocket
- HMAC-SHA384 authentication

Key differences from other exchanges:
- Uses POST for all private endpoints (even data retrieval)
- Lowercase parameters (side="buy" not "BUY")
- Order types have "exchange" prefix ("exchange limit")
- Authentication via base64-encoded signed payloads
"""

from hummingbot.connector.exchange.gemini.gemini_exchange import GeminiExchange

__all__ = ["GeminiExchange"]
