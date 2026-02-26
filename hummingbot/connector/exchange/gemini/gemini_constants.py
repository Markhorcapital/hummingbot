from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "HBOT"
MAX_ORDER_ID_LEN = 36

# Base URLs
REST_URL = "https://api.gemini.com"
WSS_PUBLIC_URL = "wss://api.gemini.com/v2/marketdata"
WSS_PRIVATE_URL = "wss://api.gemini.com/v1/order/events"

# Sandbox URLs (for testing)
REST_URL_SANDBOX = "https://api.sandbox.gemini.com"
WSS_PUBLIC_URL_SANDBOX = "wss://api.sandbox.gemini.com/v2/marketdata"
WSS_PRIVATE_URL_SANDBOX = "wss://api.sandbox.gemini.com/v1/order/events"

# API Version
API_VERSION = "v1"

# ==========================================
# PUBLIC API ENDPOINTS (GET, no authentication required)
# ==========================================
SYMBOLS_PATH_URL = "/v1/symbols"
SYMBOL_DETAILS_PATH_URL = "/v1/symbols/details/{symbol}"
ORDER_BOOK_PATH_URL = "/v1/book/{symbol}"
TICKER_PATH_URL = "/v1/pubticker/{symbol}"
TICKER_V2_PATH_URL = "/v2/ticker/{symbol}"
TRADES_PATH_URL = "/v1/trades/{symbol}"
PRICE_FEED_PATH_URL = "/v1/pricefeed"
AUCTION_HISTORY_PATH_URL = "/v1/auction/{symbol}/history"

# ==========================================
# PRIVATE API ENDPOINTS (POST, requires authentication)
# NOTE: Gemini uses POST for all private endpoints, even data retrieval!
# ==========================================
# Account endpoints
BALANCES_PATH_URL = "/v1/balances"
NOTIONAL_VOLUME_PATH_URL = "/v1/notionalvolume"
NOTIONAL_BALANCES_PATH_URL = "/v1/notionalbalances/{currency}"
AVAILABLE_BALANCES_PATH_URL = "/v1/available"

# Order endpoints
ORDER_NEW_PATH_URL = "/v1/order/new"
ORDER_CANCEL_PATH_URL = "/v1/order/cancel"
ORDER_CANCEL_ALL_PATH_URL = "/v1/order/cancel/all"
ORDER_CANCEL_SESSION_PATH_URL = "/v1/order/cancel/session"
ORDER_STATUS_PATH_URL = "/v1/order/status"

# Order query endpoints
ACTIVE_ORDERS_PATH_URL = "/v1/orders"
PAST_TRADES_PATH_URL = "/v1/mytrades"

# Heartbeat endpoint
HEARTBEAT_PATH_URL = "/v1/heartbeat"

# Request timeout
TIMEOUT = 30

# ==========================================
# Order Parameters
# ==========================================
# NOTE: Gemini uses lowercase for side parameters
SIDE_BUY = "buy"
SIDE_SELL = "sell"

# Order types (note the "exchange" prefix!)
ORDER_TYPE_LIMIT = "exchange limit"
ORDER_TYPE_MARKET = "exchange market"
ORDER_TYPE_IOC = "immediate-or-cancel"
ORDER_TYPE_FOK = "fill-or-kill"
ORDER_TYPE_MAKER_ONLY = "maker-or-cancel"
ORDER_TYPE_AUCTION_ONLY = "auction-only"
ORDER_TYPE_INDICATION_OF_INTEREST = "indication-of-interest"

# ==========================================
# Rate Limits
# ==========================================
# Gemini has different rate limits for public and private APIs
PUBLIC_REQUEST_WEIGHT = "PUBLIC_REQUEST_WEIGHT"
PRIVATE_REQUEST_WEIGHT = "PRIVATE_REQUEST_WEIGHT"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

# Rate limits
PUBLIC_RATE_LIMIT = 120   # requests per minute
PRIVATE_RATE_LIMIT = 600  # requests per minute

RATE_LIMITS = [
    # Pools
    RateLimit(limit_id=PUBLIC_REQUEST_WEIGHT, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=PRIVATE_REQUEST_WEIGHT, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE),

    # Public endpoints
    RateLimit(limit_id=SYMBOLS_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=SYMBOL_DETAILS_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=ORDER_BOOK_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=TICKER_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=TICKER_V2_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=TRADES_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=PRICE_FEED_PATH_URL, limit=PUBLIC_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_REQUEST_WEIGHT, 1)]),

    # Private endpoints
    RateLimit(limit_id=BALANCES_PATH_URL, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=ORDER_NEW_PATH_URL, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=ORDER_CANCEL_PATH_URL, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=ACTIVE_ORDERS_PATH_URL, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=PAST_TRADES_PATH_URL, limit=PRIVATE_RATE_LIMIT, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_REQUEST_WEIGHT, 1)]),
]

# ==========================================
# Order States Mapping
# ==========================================
# Map Gemini order states to Hummingbot OrderState
ORDER_STATE = {
    "live": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

# ==========================================
# WebSocket Configuration
# ==========================================
WS_HEARTBEAT_TIME_INTERVAL = 30

# WebSocket event types (public market data)
WS_HEARTBEAT_EVENT = "heartbeat"
WS_SUBSCRIPTION_ACK = "subscription_ack"
WS_L2_UPDATES = "l2_updates"
WS_TRADE_EVENT = "trade"

# WebSocket subscription channels
WS_L2_CHANNEL = "l2"

# Order event types (private WebSocket)
WS_ORDER_INITIAL = "initial"
WS_ORDER_ACCEPTED = "accepted"
WS_ORDER_REJECTED = "rejected"
WS_ORDER_BOOKED = "booked"
WS_ORDER_FILL = "fill"
WS_ORDER_CANCELLED = "cancelled"
WS_ORDER_CANCEL_REJECTED = "cancel_rejected"
WS_ORDER_CLOSED = "closed"

# ==========================================
# Authentication Headers
# ==========================================
HEADER_API_KEY = "X-GEMINI-APIKEY"
HEADER_PAYLOAD = "X-GEMINI-PAYLOAD"
HEADER_SIGNATURE = "X-GEMINI-SIGNATURE"
HEADER_NONCE = "X-GEMINI-NONCE"
CONTENT_TYPE_TEXT = "text/plain"
CONTENT_TYPE_JSON = "application/json"

# ==========================================
# Error Messages
# ==========================================
# Common Gemini error messages
ORDER_NOT_FOUND_ERROR = "OrderNotFound"
INVALID_SIGNATURE_ERROR = "InvalidSignature"
INVALID_NONCE_ERROR = "InvalidNonce"
INSUFFICIENT_FUNDS_ERROR = "InsufficientFunds"
INVALID_PRICE_ERROR = "InvalidPrice"
INVALID_QUANTITY_ERROR = "InvalidQuantity"
RATE_LIMIT_ERROR = "RateLimitExceeded"
MAINTENANCE_ERROR = "MaintenanceMode"
