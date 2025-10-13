from typing import Callable, Dict, Optional

from hummingbot.connector.exchange.crypto_com import crypto_com_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for public REST API endpoints
    :param path_url: a public REST endpoint
    :param domain: the Crypto.com domain to connect to ("com" or "sandbox")
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.SANDBOX_REST_URL if domain == "sandbox" else CONSTANTS.REST_URL
    return f"{base_url}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for private REST API endpoints
    :param path_url: a private REST endpoint
    :param domain: the Crypto.com domain to connect to ("com" or "sandbox")
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.SANDBOX_REST_URL if domain == "sandbox" else CONSTANTS.REST_URL
    return f"{base_url}{path_url}"


def wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for WebSocket connections
    :param domain: the Crypto.com domain to connect to ("com" or "sandbox")
    :return: the full WebSocket URL
    """
    return CONSTANTS.SANDBOX_WSS_URL if domain == "sandbox" else CONSTANTS.WSS_URL


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    time_provider: Optional[Callable] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    Creates a WebAssistantsFactory instance
    :param throttler: the throttler instance to use
    :param time_synchronizer: the time synchronizer instance to use
    :param domain: the domain to connect to
    :param time_provider: the time provider function to use
    :param auth: the authentication instance to use
    :return: a WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(
                synchronizer=time_synchronizer,
                time_provider=time_provider
            ),
        ]
    )
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    Creates a WebAssistantsFactory instance without time synchronizer pre-processor
    :param throttler: the throttler instance to use
    :return: a WebAssistantsFactory instance
    """
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates a default throttler instance with Crypto.com's rate limits
    :return: an AsyncThrottler instance
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Function to get current server time from Crypto.com
    :param throttler: the throttler instance to use for the request
    :param domain: the Crypto.com domain to connect to
    :return: the current server time
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()

    try:
        # Since Crypto.com doesn't have a dedicated server time endpoint,
        # we'll make a lightweight request to get-instruments and use the response time
        await rest_assistant.execute_request(
            url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
        )
        # Use current system time since Crypto.com doesn't return server time in response
        import time
        return time.time()
    except Exception:
        # Fallback to system time if API call fails
        import time
        return time.time()


def symbol_map_from_exchange_info(exchange_info: Dict) -> Dict[str, str]:
    """
    Creates a symbol map from exchange information
    :param exchange_info: the exchange information
    :return: a dictionary mapping exchange symbols to Hummingbot symbols
    """
    symbol_map = {}

    if "result" in exchange_info and "data" in exchange_info["result"]:
        instruments = exchange_info["result"]["data"]
        for instrument in instruments:
            if isinstance(instrument, dict) and "symbol" in instrument:
                exchange_symbol = instrument["symbol"]
                hb_symbol = convert_from_exchange_symbol(exchange_symbol)
                symbol_map[exchange_symbol] = hb_symbol

    return symbol_map


def convert_from_exchange_symbol(exchange_symbol: str) -> str:
    """
    Converts an exchange symbol to Hummingbot format
    :param exchange_symbol: symbol in exchange format (e.g., "BTC_USDT")
    :return: symbol in Hummingbot format (e.g., "BTC-USDT")
    """
    from hummingbot.connector.exchange.crypto_com.crypto_com_utils import convert_from_exchange_symbol as utils_convert
    return utils_convert(exchange_symbol)


def convert_to_exchange_symbol(hb_symbol: str) -> str:
    """
    Converts a Hummingbot symbol to exchange format
    :param hb_symbol: symbol in Hummingbot format (e.g., "BTC-USDT")
    :return: symbol in exchange format (e.g., "BTC_USDT")
    """
    from hummingbot.connector.exchange.crypto_com.crypto_com_utils import convert_to_exchange_symbol as utils_convert
    return utils_convert(hb_symbol)


def format_trading_rules(instruments_info: Dict) -> Dict[str, Dict]:
    """
    Formats trading rules from instruments information
    :param instruments_info: the instruments information from the exchange
    :return: a dictionary of formatted trading rules
    """
    trading_rules = {}

    if "result" in instruments_info and "data" in instruments_info["result"]:
        instruments = instruments_info["result"]["data"]
        for instrument in instruments:
            if isinstance(instrument, dict):
                symbol = instrument.get("symbol")
                if symbol:
                    trading_rules[symbol] = {
                        "symbol": symbol,
                        "base_currency": instrument.get("base_currency"),
                        "quote_currency": instrument.get("quote_currency"),
                        "price_decimals": instrument.get("price_decimals", 8),
                        "quantity_decimals": instrument.get("quantity_decimals", 8),
                        "min_quantity": instrument.get("min_quantity", "0.00000001"),
                        "max_quantity": instrument.get("max_quantity", "100000000"),
                        "min_price": instrument.get("min_price", "0.00000001"),
                        "max_price": instrument.get("max_price", "100000000"),
                        "tradable": instrument.get("tradable", False),
                        "active": instrument.get("active", False),
                    }

    return trading_rules
