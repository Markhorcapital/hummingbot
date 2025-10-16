from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),  # 0.1% maker fee
    taker_percent_fee_decimal=Decimal("0.001"),  # 0.1% taker fee
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("active", False) and exchange_info.get("tradable", False)


def convert_from_exchange_symbol(exchange_symbol: str) -> str:
    """
    Converts an exchange symbol to Hummingbot format
    Crypto.com uses _USD for USDT pairs (e.g., "BTC_USD" -> "BTC-USDT")
    :param exchange_symbol: symbol in exchange format (e.g., "BTC_USD")
    :return: symbol in Hummingbot format (e.g., "BTC-USDT")
    """
    # Convert _USD to -USDT for Crypto.com's format
    if exchange_symbol.endswith("_USD"):
        return exchange_symbol[:-4] + "-USDT"
    return exchange_symbol.replace("_", "-").upper()


def convert_to_exchange_symbol(hb_symbol: str) -> str:
    """
    Converts a Hummingbot symbol to exchange format
    Crypto.com uses _USD for USDT pairs (e.g., "BTC-USDT" -> "BTC_USD")
    :param hb_symbol: symbol in Hummingbot format (e.g., "BTC-USDT")
    :return: symbol in exchange format (e.g., "BTC_USD")
    """
    # Crypto.com uses _USD instead of _USDT
    if hb_symbol.endswith("-USDT"):
        return hb_symbol[:-5].upper() + "_USD"
    return hb_symbol.replace("-", "_").upper()


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    """
    Creates a client order ID for Crypto.com
    :param is_buy: True if the order is a buy order
    :param trading_pair: The trading pair for the order
    :return: A unique client order ID
    """
    import time
    import uuid

    side = "B" if is_buy else "S"
    timestamp = int(time.time() * 1000)
    unique_id = str(uuid.uuid4())[:8]

    return f"HBOT-{side}-{timestamp}-{unique_id}"


class CryptoComConfigMap(BaseConnectorConfigMap):
    connector: str = "crypto_com"
    crypto_com_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Crypto.com API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    crypto_com_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Crypto.com secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    crypto_com_sandbox_mode: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda cm: "Use Crypto.com sandbox environment? (Yes/No)",
            "prompt_on_new": False,
        }
    )

    class Config:
        title = "crypto_com"


KEYS = CryptoComConfigMap.construct()

OTHER_DOMAINS = ["sandbox"]
OTHER_DOMAINS_PARAMETER = {"sandbox": "sandbox"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"sandbox": "BTC-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"sandbox": DEFAULT_FEES}
OTHER_DOMAINS_KEYS = {"sandbox": KEYS}
