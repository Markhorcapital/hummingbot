import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.gemini import (
    gemini_constants as CONSTANTS,
    gemini_utils,
    gemini_web_utils as web_utils,
)
from hummingbot.connector.exchange.gemini.gemini_api_order_book_data_source import GeminiAPIOrderBookDataSource
from hummingbot.connector.exchange.gemini.gemini_api_user_stream_data_source import GeminiAPIUserStreamDataSource
from hummingbot.connector.exchange.gemini.gemini_auth import GeminiAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class GeminiExchange(ExchangePyBase):
    """
    Gemini exchange connector for Hummingbot.

    Key Differences from other exchanges:
    - Uses POST for ALL private endpoints (even data retrieval)
    - Authentication via HMAC-SHA384 with base64-encoded payloads
    - Lowercase parameters (side="buy" not "BUY")
    - Order types have "exchange" prefix ("exchange limit" not "LIMIT")
    - No separate server time endpoint
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 gemini_api_key: str,
                 gemini_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Initialize the Gemini exchange connector.

        :param client_config_map: Client configuration
        :param gemini_api_key: Gemini API key
        :param gemini_api_secret: Gemini API secret
        :param trading_pairs: List of trading pairs to track
        :param trading_required: Whether trading is required
        :param domain: Domain (for future sandbox support)
        """
        self.api_key = gemini_api_key
        self.secret_key = gemini_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_gemini_timestamp = 1.0
        super().__init__(client_config_map)

    @staticmethod
    def gemini_order_type(order_type: OrderType) -> str:
        """
        Convert Hummingbot OrderType to Gemini order type string.

        Gemini uses "exchange" prefix for all order types.

        :param order_type: Hummingbot OrderType
        :return: Gemini order type string
        """
        if order_type == OrderType.LIMIT:
            return CONSTANTS.ORDER_TYPE_LIMIT  # "exchange limit"
        elif order_type == OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_MARKET  # "exchange market"
        elif order_type == OrderType.LIMIT_MAKER:
            return CONSTANTS.ORDER_TYPE_MAKER_ONLY  # "maker-or-cancel"
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

    @staticmethod
    def to_hb_order_type(gemini_type: str) -> OrderType:
        """
        Convert Gemini order type string to Hummingbot OrderType.

        :param gemini_type: Gemini order type string
        :return: Hummingbot OrderType
        """
        gemini_type_lower = gemini_type.lower()
        if "limit" in gemini_type_lower and "maker" not in gemini_type_lower:
            return OrderType.LIMIT
        elif "market" in gemini_type_lower:
            return OrderType.MARKET
        elif "maker" in gemini_type_lower:
            return OrderType.LIMIT_MAKER
        else:
            return OrderType.LIMIT  # default

    @property
    def authenticator(self):
        return GeminiAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "gemini"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.SYMBOLS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.SYMBOLS_PATH_URL

    @property
    def check_network_request_path(self):
        # Gemini doesn't have a ping endpoint, use symbols as health check
        return CONSTANTS.SYMBOLS_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """
        Get current prices for all trading pairs.

        :return: List of price information
        """
        pairs_prices = await self._api_get(path_url=CONSTANTS.PRICE_FEED_PATH_URL)
        return pairs_prices

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """
        Check if exception is related to time synchronization issues.

        Gemini error messages for nonce/timestamp issues:
        - "InvalidNonce"
        - Timestamp-related errors
        """
        error_description = str(request_exception)
        is_time_synchronizer_related = (
            CONSTANTS.INVALID_NONCE_ERROR in error_description
            or "nonce" in error_description.lower()
            or "timestamp" in error_description.lower()
        )
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """
        Check if exception indicates order was not found during status update.

        :param status_update_exception: Exception from order status update
        :return: True if order not found
        """
        return CONSTANTS.ORDER_NOT_FOUND_ERROR in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """
        Check if exception indicates order was not found during cancellation.

        :param cancelation_exception: Exception from order cancellation
        :return: True if order not found
        """
        return CONSTANTS.ORDER_NOT_FOUND_ERROR in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return GeminiAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return GeminiAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculate trading fee for an order.

        Gemini fees (default):
        - Maker: 0.1%
        - Taker: 0.35%

        :return: Trade fee object
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Place an order on Gemini exchange.

        IMPORTANT: Gemini uses POST with specific payload structure.

        :param order_id: Client order ID
        :param trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USD")
        :param amount: Order amount
        :param trade_type: BUY or SELL
        :param order_type: LIMIT, MARKET, or LIMIT_MAKER
        :param price: Order price (required for limit orders)
        :return: Tuple of (exchange_order_id, timestamp)
        """
        amount_str = f"{amount:f}"
        gemini_order_type = self.gemini_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

        # Convert to Gemini symbol format (e.g., "BTC-USD" → "BTCUSD")
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # Build order parameters
        api_params = {
            "symbol": symbol,
            "side": side_str,
            "type": gemini_order_type,
            "amount": amount_str,
            "client_order_id": order_id
        }

        # Add price for limit orders
        if order_type.is_limit_type():
            price_str = f"{price:f}"
            api_params["price"] = price_str

        try:
            # CRITICAL: Use POST for order placement
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_NEW_PATH_URL,
                data=api_params,
                is_auth_required=True)

            # Extract order ID and timestamp from response
            exchange_order_id = str(order_result["order_id"])

            # Gemini returns timestamp in milliseconds
            transact_time = order_result.get("timestampms", int(self._time_synchronizer.time() * 1000)) * 1e-3

        except IOError as e:
            error_description = str(e)
            # Handle server overload gracefully
            is_server_overloaded = "503" in error_description
            if is_server_overloaded:
                exchange_order_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise

        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order on Gemini exchange.

        IMPORTANT: Gemini uses POST (not DELETE) for cancellation.
        Can cancel by order_id (exchange ID) or client_order_id.

        :param order_id: Client order ID
        :param tracked_order: The order to cancel
        :return: True if successfully cancelled
        """
        # Gemini can cancel by exchange order_id or client_order_id
        # Prefer exchange order_id if available, otherwise use client_order_id
        api_params = {}

        if tracked_order.exchange_order_id:
            api_params["order_id"] = tracked_order.exchange_order_id
        else:
            api_params["client_order_id"] = order_id

        # Use POST for cancellation
        cancel_result = await self._api_post(
            path_url=CONSTANTS.ORDER_CANCEL_PATH_URL,
            data=api_params,
            is_auth_required=True)

        # Check if cancellation was successful
        # Gemini returns the order object with is_cancelled=true if successful
        is_cancelled = cancel_result.get("is_cancelled", False)

        return is_cancelled

    async def _format_trading_rules(self, exchange_info_list: List[Dict[str, Any]]) -> List[TradingRule]:
        """
        Convert Gemini symbol details into Hummingbot trading rules.

        Gemini symbol details format:
        [
            {
                "symbol": "BTCUSD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "tick_size": 1e-8,
                "quote_increment": 0.01,
                "min_order_size": "0.00001",
                "status": "open",
                ...
            },
            ...
        ]

        :param exchange_info_list: List of symbol information from Gemini
        :return: List of TradingRule objects
        """
        retval = []

        for symbol_info in exchange_info_list:
            try:
                # Skip if not a valid/open symbol
                if not gemini_utils.is_exchange_information_valid(symbol_info):
                    continue

                symbol = symbol_info["symbol"]

                # Convert Gemini symbol to Hummingbot trading pair
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                # Extract trading rules
                min_order_size = Decimal(str(symbol_info.get("min_order_size", "0")))
                tick_size = Decimal(str(symbol_info.get("tick_size", "0")))
                quote_increment = Decimal(str(symbol_info.get("quote_increment", "0")))

                # Gemini doesn't provide min_notional directly, so we rely on min_order_size
                min_notional = Decimal("0")

                retval.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=quote_increment,
                        min_base_amount_increment=tick_size,
                        min_notional_size=min_notional
                    )
                )

            except Exception:
                self.logger().exception(f"Error parsing trading pair rule {symbol_info}. Skipping.")

        return retval

    async def _update_trading_fees(self):
        """
        Update trading fees from the exchange.

        Gemini's fee structure is volume-based. For now, we'll use default fees
        defined in gemini_utils.py. In the future, this could fetch actual
        fee tiers based on notional volume.
        """
        # Use default fees from config
        # In the future, could call /v1/notionalvolume to get actual fee tier
        pass

    async def _update_trading_rules(self):
        """
        Fetch and update trading rules from Gemini.

        Gemini provides symbol details via /v1/symbols/details/:symbol
        or we can get all symbols and then fetch details for each.
        """
        # Get list of all symbols (not used directly, but kept for future use)
        await self._api_get(
            path_url=CONSTANTS.SYMBOLS_PATH_URL,
            is_auth_required=False
        )

        # For each symbol we're trading, get detailed information
        symbol_details = []
        for trading_pair in self._trading_pairs:
            try:
                gemini_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                detail_path = CONSTANTS.SYMBOL_DETAILS_PATH_URL.replace("{symbol}", gemini_symbol)

                detail = await self._api_get(
                    path_url=detail_path,
                    is_auth_required=False,
                    throttler_limit_id=CONSTANTS.SYMBOL_DETAILS_PATH_URL  # Use template for rate limiter
                )
                symbol_details.append(detail)
            except Exception:
                self.logger().exception(f"Failed to get symbol details for {trading_pair}")

        # Format and store trading rules
        trading_rules_list = await self._format_trading_rules(symbol_details)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _user_stream_event_listener(self):
        """
        Listen to user stream events and update order statuses.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")
                client_order_id = event_message.get("client_order_id")
                exchange_order_id = str(event_message.get("order_id"))

                # Gemini doesn't always send client_order_id in all messages, logic to find it
                tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)

                if not tracked_order:
                    # Try to find by exchange_order_id if client_order_id is missing or mismatch
                    for order in self._order_tracker.all_updatable_orders.values():
                        if order.exchange_order_id == exchange_order_id:
                            tracked_order = order
                            break

                if not tracked_order:
                    # Order not tracked, skip
                    continue

                # Update the exchange_order_id in the tracker if it wasn't set (e.g. from initial placement)
                if tracked_order.exchange_order_id is None:
                    tracked_order.update_exchange_order_id(exchange_order_id)

                if event_type == CONSTANTS.WS_ORDER_FILL:
                    # Handle Fill
                    fill_amount = Decimal(str(event_message.get("fill_amount", "0")))
                    fill_price = Decimal(str(event_message.get("fill_price", "0")))
                    fee_amount = Decimal(str(event_message.get("fee_amount", "0")))
                    fee_currency = event_message.get("fee_currency", "USD")

                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        percent_token=fee_currency,
                        flat_fees=[TokenAmount(amount=fee_amount, token=fee_currency)]
                    )

                    trade_update = TradeUpdate(
                        trade_id=str(event_message.get("tid", event_message.get("timestampms"))),  # Use tid or timestamp
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_amount,
                        fill_quote_amount=fill_amount * fill_price,
                        fill_price=fill_price,
                        fill_timestamp=event_message.get("timestampms", 0) * 1e-3,
                    )
                    self._order_tracker.process_trade_update(trade_update)

                # Handle Order Status Updates
                new_state = None
                if event_type == CONSTANTS.WS_ORDER_ACCEPTED:
                    new_state = OrderState.OPEN
                elif event_type == CONSTANTS.WS_ORDER_BOOKED:
                    new_state = OrderState.OPEN
                elif event_type == CONSTANTS.WS_ORDER_FILL:
                    # Check if fully filled
                    remaining_amount = Decimal(str(event_message.get("remaining_amount", "0")))
                    if remaining_amount == 0:
                        new_state = OrderState.FILLED
                    else:
                        new_state = OrderState.PARTIALLY_FILLED
                elif event_type == CONSTANTS.WS_ORDER_CANCELLED:
                    new_state = OrderState.CANCELED
                elif event_type == CONSTANTS.WS_ORDER_REJECTED:
                    new_state = OrderState.FAILED
                elif event_type == CONSTANTS.WS_ORDER_CLOSED:
                    # Check remaining amount to distinguish between filled and closed-other
                    remaining_amount = Decimal(str(event_message.get("remaining_amount", "0")))
                    if remaining_amount == 0:
                        new_state = OrderState.FILLED
                    else:
                        new_state = OrderState.CANCELED

                if new_state:
                    order_update = OrderUpdate(
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=event_message.get("timestampms", 0) * 1e-3,
                        new_state=new_state,
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=exchange_order_id,
                    )
                    self._order_tracker.process_order_update(order_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _update_order_status(self):
        """
        Poll for order status updates.

        IMPORTANT: Gemini's /v1/order/status requires POST.
        """
        # Get all tracked orders
        tracked_orders = list(self.in_flight_orders.values())

        if not tracked_orders:
            return

        # Query status for each order
        for tracked_order in tracked_orders:
            try:
                # Gemini requires POST with order_id or client_order_id
                api_params = {}
                if tracked_order.exchange_order_id:
                    api_params["order_id"] = tracked_order.exchange_order_id
                else:
                    api_params["client_order_id"] = tracked_order.client_order_id

                order_status = await self._api_post(
                    path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
                    data=api_params,
                    is_auth_required=True
                )

                # Update order state
                order_update = self._parse_order_status_update(order_status, tracked_order)
                self._order_tracker.process_order_update(order_update)

            except Exception:
                self.logger().exception(f"Error updating order status for {tracked_order.client_order_id}")

    def _parse_order_status_update(self, order_data: Dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Parse Gemini order status response into OrderUpdate.

        Gemini order response format:
        {
            "order_id": "123456",
            "client_order_id": "HBOT123",
            "symbol": "BTCUSD",
            "side": "buy",
            "type": "exchange limit",
            "price": "50000.00",
            "original_amount": "0.001",
            "executed_amount": "0.0005",
            "remaining_amount": "0.0005",
            "is_live": true,
            "is_cancelled": false,
            "was_forced": false,
            ...
        }
        """
        exchange_order_id = str(order_data["order_id"])

        # Determine order state
        is_live = order_data.get("is_live", False)
        is_cancelled = order_data.get("is_cancelled", False)
        executed_amount = Decimal(order_data.get("executed_amount", "0"))
        original_amount = Decimal(order_data.get("original_amount", "0"))

        if is_cancelled:
            new_state = CONSTANTS.ORDER_STATE["cancelled"]
        elif executed_amount >= original_amount and executed_amount > 0:
            new_state = CONSTANTS.ORDER_STATE["filled"]
        elif executed_amount > 0:
            new_state = CONSTANTS.ORDER_STATE["partially_filled"]
        elif is_live:
            new_state = CONSTANTS.ORDER_STATE["live"]
        else:
            new_state = tracked_order.current_state

        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._time_synchronizer.time(),
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
        )

        return order_update

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Fetch all trade updates (fills) for a specific order.

        IMPORTANT: Gemini uses POST for /v1/mytrades.

        :param order: The order to fetch trades for
        :return: List of TradeUpdate objects
        """
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                # Gemini requires POST with parameters
                api_params = {
                    "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair),
                    "limit_trades": 500  # Get up to 500 trades
                }

                all_fills_response = await self._api_post(
                    path_url=CONSTANTS.PAST_TRADES_PATH_URL,
                    data=api_params,
                    is_auth_required=True
                )

                # Filter trades for this specific order
                for trade in all_fills_response:
                    if str(trade.get("order_id")) == order.exchange_order_id:
                        # Parse fee information
                        fee_currency = trade.get("fee_currency", "USD")
                        fee_amount = Decimal(str(trade.get("fee_amount", "0")))

                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            percent_token=fee_currency,
                            flat_fees=[TokenAmount(amount=fee_amount, token=fee_currency)]
                        )

                        # Create trade update
                        fill_amount = Decimal(str(trade["amount"]))
                        fill_price = Decimal(str(trade["price"]))

                        trade_update = TradeUpdate(
                            trade_id=str(trade["tid"]),
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            trading_pair=order.trading_pair,
                            fee=fee,
                            fill_base_amount=fill_amount,
                            fill_quote_amount=fill_amount * fill_price,
                            fill_price=fill_price,
                            fill_timestamp=trade["timestampms"] * 1e-3,
                        )
                        trade_updates.append(trade_update)

            except Exception:
                self.logger().exception(f"Error fetching trade updates for order {order.client_order_id}")

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from Gemini.

        IMPORTANT: Gemini uses POST for /v1/order/status.

        :param tracked_order: The order to query
        :return: OrderUpdate object
        """
        # Gemini requires POST with order_id or client_order_id
        api_params = {}
        if tracked_order.exchange_order_id:
            api_params["order_id"] = tracked_order.exchange_order_id
        else:
            api_params["client_order_id"] = tracked_order.client_order_id

        updated_order_data = await self._api_post(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            data=api_params,
            is_auth_required=True
        )

        # Parse order state
        order_update = self._parse_order_status_update(updated_order_data, tracked_order)

        return order_update

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize trading pair symbol mappings from exchange info.

        Builds a bidirectional mapping between Hummingbot trading pairs (BTC-USD)
        and Gemini symbols (BTCUSD).

        :param exchange_info: Can be either:
            - List of symbol strings: ["btcusd", "ethusd", ...]
            - List of symbol detail dicts: [{"symbol": "btcusd", "base_currency": "BTC", ...}, ...]
        """
        mapping = bidict()

        if isinstance(exchange_info, list):
            for symbol_data in exchange_info:
                try:
                    # Handle case where exchange_info is just a list of symbol strings
                    if isinstance(symbol_data, str):
                        # Simple symbol string - use our conversion utility
                        gemini_symbol = symbol_data.upper()
                        try:
                            hb_trading_pair = gemini_utils.convert_from_gemini_symbol(gemini_symbol)
                            mapping[gemini_symbol] = hb_trading_pair
                        except ValueError:
                            # Skip symbols we can't parse
                            self.logger().debug(f"Skipping symbol {gemini_symbol} - unable to parse")
                            continue

                    # Handle case where exchange_info has full symbol details
                    elif isinstance(symbol_data, dict):
                        if gemini_utils.is_exchange_information_valid(symbol_data):
                            gemini_symbol = symbol_data["symbol"]
                            base = symbol_data["base_currency"]
                            quote = symbol_data["quote_currency"]

                            # Create Hummingbot trading pair format
                            hb_trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)

                            # Add to mapping
                            mapping[gemini_symbol] = hb_trading_pair

                except Exception:
                    self.logger().exception(f"Error processing symbol {symbol_data}")

        self._set_trading_pair_symbol_map(mapping)

    async def _update_balances(self):
        """
        Update account balances from Gemini.

        IMPORTANT: Gemini uses POST for /v1/balances.
        """
        # POST to get balances
        balances = await self._api_post(
            path_url=CONSTANTS.BALANCES_PATH_URL,
            data={},  # Empty params, but POST is required
            is_auth_required=True
        )

        # Process balance information
        self._account_available_balances.clear()
        self._account_balances.clear()

        for balance_entry in balances:
            asset = balance_entry["currency"]
            total_balance = Decimal(balance_entry["amount"])
            available_balance = Decimal(balance_entry["available"])

            self._account_balances[asset] = total_balance
            self._account_available_balances[asset] = available_balance

    async def _api_get(self, path_url: str, params: Optional[Dict[str, Any]] = None,
                       is_auth_required: bool = False, **kwargs) -> Dict:
        """
        Make GET request to Gemini API.

        Used for public endpoints only.

        :param path_url: Endpoint path
        :param params: Query parameters
        :param is_auth_required: Whether authentication is required
        :return: Response data
        """
        return await self._api_request(
            method=RESTMethod.GET,
            path_url=path_url,
            params=params,
            is_auth_required=is_auth_required,
            **kwargs
        )

    async def _api_post(self, path_url: str, data: Optional[Dict[str, Any]] = None,
                        is_auth_required: bool = False, **kwargs) -> Dict:
        """
        Make POST request to Gemini API.

        CRITICAL: Gemini uses POST for ALL private endpoints, including data retrieval.

        :param path_url: Endpoint path
        :param data: Request data (will be included in signed payload)
        :param is_auth_required: Whether authentication is required
        :return: Response data
        """
        return await self._api_request(
            method=RESTMethod.POST,
            path_url=path_url,
            data=data,
            is_auth_required=is_auth_required,
            **kwargs
        )

    async def _api_request(self, method: RESTMethod, path_url: str,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False,
                           throttler_limit_id: Optional[str] = None,
                           **kwargs) -> Dict:
        """
        Generic API request handler.

        :param method: HTTP method
        :param path_url: Endpoint path
        :param params: Query parameters (for GET)
        :param data: Request data (for POST)
        :param is_auth_required: Whether authentication is required
        :param throttler_limit_id: Rate limit ID (defaults to path_url)
        :return: Response data
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        # Build full URL
        if is_auth_required:
            url = web_utils.private_rest_url(path_url, self._domain)
        else:
            url = web_utils.public_rest_url(path_url, self._domain)

        response = await rest_assistant.execute_request(
            url=url,
            method=method,
            params=params,
            data=data,
            is_auth_required=is_auth_required,
            throttler_limit_id=throttler_limit_id or path_url,
            **kwargs
        )

        return response

    async def _make_trading_pairs_request(self) -> Any:
        """
        Fetch list of available trading pairs from Gemini.

        :return: List of symbol strings
        """
        symbols = await self._api_get(
            path_url=CONSTANTS.SYMBOLS_PATH_URL,
            is_auth_required=False
        )
        return symbols

    async def _make_trading_rules_request(self) -> Any:
        """
        Fetch trading rules for all symbols.

        For Gemini, this requires calling /v1/symbols/details/:symbol for each pair.

        :return: List of symbol details
        """
        # First get all symbols
        symbols = await self._make_trading_pairs_request()

        # Then get details for each symbol we're interested in
        symbol_details = []
        for symbol in symbols:
            try:
                # Only fetch details for symbols we're trading
                hb_trading_pair = gemini_utils.convert_from_gemini_symbol(symbol)
                if hb_trading_pair in self._trading_pairs:
                    detail_path = CONSTANTS.SYMBOL_DETAILS_PATH_URL.replace("{symbol}", symbol)
                    detail = await self._api_get(
                        path_url=detail_path,
                        is_auth_required=False,
                        throttler_limit_id=CONSTANTS.SYMBOL_DETAILS_PATH_URL
                    )
                    symbol_details.append(detail)
            except Exception:
                self.logger().exception(f"Failed to get details for symbol {symbol}")

        return symbol_details

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get the last traded price for a trading pair.

        :param trading_pair: Trading pair in Hummingbot format
        :return: Last traded price
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        ticker_path = CONSTANTS.TICKER_PATH_URL.replace("{symbol}", symbol)

        ticker = await self._api_get(
            path_url=ticker_path,
            is_auth_required=False,
            throttler_limit_id=CONSTANTS.TICKER_PATH_URL
        )

        return float(ticker["last"])

    def _get_exchange_trading_pair_from_market_info(self, market_info: Dict[str, Any]) -> str:
        """
        Extract exchange trading pair from market info.

        :param market_info: Market information dict
        :return: Exchange symbol (e.g., "BTCUSD")
        """
        if isinstance(market_info, dict):
            return market_info.get("symbol", "")
        return market_info

    def _get_exchange_base_quote_tokens_from_market_info(self, market_info: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """
        Extract base and quote currencies from market info.

        :param market_info: Market information dict
        :return: Tuple of (base_currency, quote_currency)
        """
        if isinstance(market_info, dict):
            base = market_info.get("base_currency")
            quote = market_info.get("quote_currency")
            if base and quote:
                return base, quote
        return None
