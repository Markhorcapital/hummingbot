from typing import Callable, Optional

import hummingbot.connector.exchange.gemini.gemini_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint.

    Gemini's URL structure is simpler than Binance:
    https://api.gemini.com/v1/symbols (the version is in the path itself)

    :param path_url: a public REST endpoint (e.g., "/v1/symbols")
    :param domain: domain ("com" for production, not used but kept for consistency)
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URL
    # Path already includes /v1/ prefix
    return base_url + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint.

    Gemini uses the same base URL for public and private endpoints.
    The difference is in authentication headers, not the URL.

    :param path_url: a private REST endpoint (e.g., "/v1/balances")
    :param domain: domain ("com" for production)
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URL
    # Path already includes /v1/ prefix
    return base_url + path_url


def wss_public_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates the WebSocket URL for public market data.

    :param domain: domain ("com" for production)
    :return: the WebSocket URL for public market data
    """
    return CONSTANTS.WSS_PUBLIC_URL


def wss_private_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates the WebSocket URL for private user data (order events).

    :param domain: domain ("com" for production)
    :return: the WebSocket URL for private user data
    """
    return CONSTANTS.WSS_PRIVATE_URL


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    """
    Get current server time.

    Since Gemini doesn't have a dedicated server time endpoint,
    we return system time in milliseconds.

    :param throttler: Rate limiter (unused but kept for compatibility)
    :param domain: Domain (unused but kept for compatibility)
    :return: Current time in milliseconds
    """
    import time
    return time.time() * 1000


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None) -> WebAssistantsFactory:
    """
    Builds the API factory for making HTTP requests.

    Note: Gemini doesn't have a server time endpoint, so we use system time.
    The time_synchronizer is still included for consistency but uses local time.

    :param throttler: Rate limiter
    :param time_synchronizer: Time synchronizer (uses local time for Gemini)
    :param domain: domain
    :param time_provider: Time provider function (should be async)
    :param auth: Authentication handler
    :return: WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()

    # For Gemini, we use system time since there's no server time endpoint
    # The time_provider must be an async callable that returns time in milliseconds
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
        ])

    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
        throttler: AsyncThrottler,
        auth: Optional[AuthBase] = None) -> WebAssistantsFactory:
    """
    Builds an API factory without the time synchronizer preprocessor.
    Used for requests that don't need time synchronization.

    :param throttler: Rate limiter
    :param auth: Authentication handler
    :return: WebAssistantsFactory instance
    """
    api_factory = WebAssistantsFactory(throttler=throttler, auth=auth)
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates an async throttler with Gemini's rate limits.

    Gemini rate limits:
    - Public API: 120 requests per minute
    - Private API: 600 requests per minute

    :return: AsyncThrottler instance
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def endpoint_from_url(url: str) -> str:
    """
    Extracts the endpoint path from a full URL.

    Examples:
    - "https://api.gemini.com/v1/balances" → "/v1/balances"
    - "/v1/balances" → "/v1/balances"

    :param url: Full URL or path
    :return: Endpoint path
    """
    if url.startswith("http"):
        # Extract path from full URL
        parts = url.split("/", 3)
        if len(parts) >= 4:
            return "/" + parts[3]
        return "/"
    return url
