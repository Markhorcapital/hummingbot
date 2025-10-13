from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "HBOT"
MAX_ORDER_ID_LEN = 36

# Base URLs
REST_URL = "https://api.crypto.com/exchange/v1"
WSS_URL = "wss://stream.crypto.com/exchange/v1/market"
SANDBOX_REST_URL = "https://uat-api.3ona.co/exchange/v1"
SANDBOX_WSS_URL = "wss://uat-stream.3ona.co/exchange/v1/market"

API_VERSION = "v1"

# Public API endpoints - Updated to match current Crypto.com API
INSTRUMENTS_PATH_URL = "/public/get-instruments"
BOOK_PATH_URL = "/public/get-book"
TICKER_PATH_URL = "/public/get-tickers"  # Changed from get-ticker to get-tickers
TRADES_PATH_URL = "/public/get-trades"
CANDLESTICK_PATH_URL = "/public/get-candlestick"
SERVER_TIME_PATH_URL = "/public/get-instruments"  # Crypto.com doesn't have dedicated server time endpoint

# Private API endpoints
CREATE_ORDER_PATH_URL = "/private/create-order"
CANCEL_ORDER_PATH_URL = "/private/cancel-order"
CANCEL_ALL_ORDERS_PATH_URL = "/private/cancel-all-orders"
GET_ORDER_HISTORY_PATH_URL = "/private/get-order-history"
GET_ORDER_DETAIL_PATH_URL = "/private/get-order-detail"
GET_TRADES_PATH_URL = "/private/get-trades"
ACCOUNT_SUMMARY_PATH_URL = "/private/get-account-summary"
USER_BALANCE_PATH_URL = "/private/user-balance"

# WebSocket channels
WS_BOOK_CHANNEL = "book"
WS_TRADE_CHANNEL = "trade"
WS_TICKER_CHANNEL = "ticker"
WS_USER_ORDER_CHANNEL = "user.order"
WS_USER_TRADE_CHANNEL = "user.trade"
WS_USER_BALANCE_CHANNEL = "user.balance"

# Order types
ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"
ORDER_TYPE_STOP_LOSS = "STOP_LOSS"
ORDER_TYPE_STOP_LIMIT = "STOP_LIMIT"
ORDER_TYPE_TAKE_PROFIT = "TAKE_PROFIT"
ORDER_TYPE_TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"

# Order sides
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

# Order status mapping
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "OPEN": OrderState.OPEN,
    "PARTIAL_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.FAILED,
}

# Time in force
TIME_IN_FORCE_GTC = "GOOD_TILL_CANCEL"
TIME_IN_FORCE_IOC = "IMMEDIATE_OR_CANCEL"
TIME_IN_FORCE_FOK = "FILL_OR_KILL"

# Rate Limits (based on Crypto.com's documented limits)
RATE_LIMITS = [
    # Public API limits - 100 requests per second
    RateLimit(limit_id="public_api", limit=100, time_interval=1),

    # Private API limits - 100 requests per second
    RateLimit(limit_id="private_api", limit=100, time_interval=1),

    # Order management limits - 15 requests per second
    RateLimit(limit_id="order_management", limit=15, time_interval=1),

    # Specific endpoint limits
    RateLimit(limit_id=INSTRUMENTS_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=BOOK_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=TICKER_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=TRADES_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=15, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=15, time_interval=1),
    RateLimit(limit_id=GET_ORDER_HISTORY_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=ACCOUNT_SUMMARY_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=USER_BALANCE_PATH_URL, limit=100, time_interval=1),
]

# WebSocket heartbeat interval
WS_HEARTBEAT_TIME_INTERVAL = 30

# Error codes
ORDER_NOT_EXIST_ERROR_CODE = 10004
ORDER_NOT_EXIST_MESSAGE = "ORDER_NOT_FOUND"
INSUFFICIENT_BALANCE_ERROR_CODE = 10007
INSUFFICIENT_BALANCE_MESSAGE = "INSUFFICIENT_BALANCE"

# Time synchronization error codes
TIME_SYNC_ERROR_CODES = [10003, 10009]  # INVALID_REQUEST_PAYLOAD, INVALID_DATE_RANGE

# Exchange info
EXCHANGE_NAME = "crypto_com"

# Request timeout
REQUEST_TIMEOUT = 10.0

# Maximum number of retries for failed requests
MAX_REQUEST_RETRIES = 3
