from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USD"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),  # 0.1% maker fee
    taker_percent_fee_decimal=Decimal("0.0035"),  # 0.35% taker fee
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information.

    Gemini symbol details format:
    {
        "symbol": "BTCUSD",
        "base_currency": "BTC",
        "quote_currency": "USD",
        "tick_size": 1e-8,
        "quote_increment": 0.01,
        "min_order_size": "0.00001",
        "status": "open",
        ...
    }

    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    # Check if the symbol status is "open" (available for trading)
    status = exchange_info.get("status", "").lower()
    return status == "open"


def convert_to_gemini_symbol(hb_trading_pair: str) -> str:
    """
    Convert Hummingbot trading pair format to Gemini symbol format.

    Gemini uses no separator and uppercase: BTCUSD, ETHUSD, etc.
    Hummingbot uses hyphen separator: BTC-USD, ETH-USD, etc.

    :param hb_trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USD")
    :return: Trading pair in Gemini format (e.g., "BTCUSD")
    """
    return hb_trading_pair.replace("-", "").upper()


def convert_from_gemini_symbol(gemini_symbol: str) -> str:
    """
    Convert Gemini symbol format to Hummingbot trading pair format.

    Note: This requires the symbol details to properly split base and quote currencies.
    For now, we'll use a simple approach for common pairs.

    :param gemini_symbol: Trading pair in Gemini format (e.g., "BTCUSD")
    :return: Trading pair in Hummingbot format (e.g., "BTC-USD")
    """
    # Common quote currencies (in order of priority)
    quote_currencies = ["USDT", "USDC", "USD", "GUSD", "EUR", "GBP", "DAI", "BTC", "ETH"]

    gemini_symbol_upper = gemini_symbol.upper()

    for quote in quote_currencies:
        if gemini_symbol_upper.endswith(quote):
            base = gemini_symbol_upper[:-len(quote)]
            return f"{base}-{quote}"

    # If no match found, raise an error
    raise ValueError(f"Unable to parse Gemini symbol: {gemini_symbol}")


class GeminiConfigMap(BaseConnectorConfigMap):
    connector: str = "gemini"
    gemini_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Gemini API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    gemini_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Gemini API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="gemini")


KEYS = GeminiConfigMap.model_construct()

# No other domains for Gemini (only sandbox vs production)
OTHER_DOMAINS = []
OTHER_DOMAINS_PARAMETER = {}
OTHER_DOMAINS_EXAMPLE_PAIR = {}
OTHER_DOMAINS_DEFAULT_FEES = {}
