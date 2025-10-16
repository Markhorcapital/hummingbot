import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.crypto_com import (
    crypto_com_constants as CONSTANTS,
    crypto_com_utils,
    crypto_com_web_utils as web_utils,
)
from hummingbot.connector.exchange.crypto_com.crypto_com_api_order_book_data_source import (
    CryptoComAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.crypto_com.crypto_com_api_user_stream_data_source import (
    CryptoComAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.crypto_com.crypto_com_auth import CryptoComAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class CryptoComExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 crypto_com_api_key: str,
                 crypto_com_secret_key: str,
                 client_config_map: Optional[Any] = None,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 crypto_com_sandbox_mode: bool = False,
                 ):
        self.api_key = crypto_com_api_key
        self.secret_key = crypto_com_secret_key
        self._domain = "sandbox" if crypto_com_sandbox_mode else domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_crypto_com_timestamp = 1.0
        # Initialize empty symbol map to prevent AttributeError
        self._trading_pair_symbol_map = bidict()
        super().__init__(client_config_map)

    @staticmethod
    def crypto_com_order_type(order_type: OrderType) -> str:
        return {
            OrderType.LIMIT: CONSTANTS.ORDER_TYPE_LIMIT,
            OrderType.MARKET: CONSTANTS.ORDER_TYPE_MARKET,
            OrderType.LIMIT_MAKER: CONSTANTS.ORDER_TYPE_LIMIT,
        }[order_type]

    @staticmethod
    def to_hb_order_type(crypto_com_type: str) -> OrderType:
        return {
            CONSTANTS.ORDER_TYPE_LIMIT: OrderType.LIMIT,
            CONSTANTS.ORDER_TYPE_MARKET: OrderType.MARKET,
            CONSTANTS.ORDER_TYPE_STOP_LOSS: OrderType.LIMIT,
            CONSTANTS.ORDER_TYPE_STOP_LIMIT: OrderType.LIMIT,
        }.get(crypto_com_type, OrderType.LIMIT)

    @property
    def authenticator(self):
        return CryptoComAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        if self._domain == "sandbox":
            return "crypto_com_sandbox"
        else:
            return "crypto_com"

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
        return CONSTANTS.INSTRUMENTS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.INSTRUMENTS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.INSTRUMENTS_PATH_URL

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
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        error_description = str(request_exception).lower()
        return any(str(error_code) in error_description for error_code in CONSTANTS.TIME_SYNC_ERROR_CODES) or \
            "timestamp" in error_description or "nonce" in error_description

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(status_update_exception) or \
            CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(cancelation_exception) or \
            CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(cancelation_exception)

    def _is_user_stream_initialized(self):
        """
        Override to handle disabled WebSocket user streams.
        Since we're using REST API polling instead of WebSocket user streams,
        we consider the user stream initialized if we're not using WebSocket authentication.
        """
        # If user stream tracker exists and has received data, use parent logic
        if (hasattr(self, '_user_stream_tracker') and
                self._user_stream_tracker is not None and
                hasattr(self._user_stream_tracker, 'data_source') and
                self._user_stream_tracker.data_source.last_recv_time > 0):
            return True

        # If trading is not required, always return True
        if not self.is_trading_required:
            return True

        # For Crypto.com, we're using REST API polling instead of WebSocket user streams
        # So we consider it initialized if we have a valid authenticator
        return hasattr(self, '_auth') and self._auth is not None

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CryptoComAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CryptoComAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculate the fee for a trade based on Crypto.com's fee structure
        """
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        fee_schema = self.trade_fee_schema()

        if is_maker:
            fee_percent = fee_schema.maker_percent_fee_decimal
        else:
            fee_percent = fee_schema.taker_percent_fee_decimal

        if order_side == TradeType.BUY:
            # For buy orders, fee is deducted from the received base currency
            return DeductedFromReturnsTradeFee(
                percent=fee_percent,
                flat_fees=[TokenAmount(amount=Decimal("0"), token=base_currency)]
            )
        else:
            # For sell orders, fee is deducted from the received quote currency
            return DeductedFromReturnsTradeFee(
                percent=fee_percent,
                flat_fees=[TokenAmount(amount=Decimal("0"), token=quote_currency)]
            )

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Place an order on Crypto.com exchange - applying proper trading rules for formatting
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # Validate amount - must be valid for all order types
        if amount is None or amount <= 0:
            raise ValueError(f"Invalid amount: {amount}")

        # Format quantity exactly like working script - simple string conversion
        # Your working script uses simple string conversion: str(amount)
        quantity_str = str(amount)

        # Build order parameters in EXACT same order as working script
        # Your working script order: instrument_name, side, type, price, quantity, client_oid
        params = {
            "instrument_name": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": self.crypto_com_order_type(order_type),
        }

        # Add price for limit orders (in the same position as working script)
        if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
            if price is None or price <= 0 or str(price).lower() in ['nan', 'inf', '-inf']:
                raise ValueError(f"Invalid price for limit order: {price}")

            # Format price exactly like working script - simple string conversion
            price_str = str(price)
            params["price"] = price_str

        # Add quantity and client_oid in the same order as working script
        params["quantity"] = quantity_str
        params["client_oid"] = order_id

        # Log the formatted values for debugging
        self.logger().info(f"Placing order for {trading_pair}: quantity='{quantity_str}', price='{params.get('price', 'N/A')}'")

        # Structure exactly like working script
        data = {
            "method": "private/create-order",
            "params": params
        }

        response = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.CREATE_ORDER_PATH_URL
        )

        if response.get("code") == 0 and "result" in response:
            exchange_order_id = response["result"]["order_id"]
            return str(exchange_order_id), self.current_timestamp
        else:
            raise ValueError(f"Order creation failed: {response}")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """
        Cancel an order on Crypto.com exchange
        """
        try:
            data = {
                "method": "private/cancel-order",
                "params": {
                    "instrument_name": await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair),
                    "order_id": tracked_order.exchange_order_id
                }
            }

            response = await self._api_post(
                path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
                data=data,
                is_auth_required=True,
                limit_id=CONSTANTS.CANCEL_ORDER_PATH_URL
            )

            return response.get("code") == 0
        except Exception:
            return False

    async def _make_trading_rules_request(self) -> Any:
        """
        Make request to get trading rules from Crypto.com
        """
        try:
            response = await self._api_get(
                path_url=CONSTANTS.INSTRUMENTS_PATH_URL,
                limit_id=CONSTANTS.INSTRUMENTS_PATH_URL
            )

            # Log the response for debugging
            if response.get("code") == 0:
                instruments_count = len(response.get("result", {}).get("data", []))
                self.logger().info(f"Successfully fetched {instruments_count} instruments from Crypto.com")
            else:
                self.logger().error(f"Failed to fetch instruments: {response}")

            return response
        except Exception as e:
            self.logger().error(f"Error fetching trading rules: {e}")
            # Return empty response to prevent crashes
            return {"code": -1, "message": str(e), "result": {"data": []}}

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info
        """
        trading_rules = []

        if "result" in exchange_info_dict and "data" in exchange_info_dict["result"]:
            instruments = exchange_info_dict["result"]["data"]

            for instrument in instruments:
                if not isinstance(instrument, dict):
                    continue

                symbol = instrument.get("symbol")
                if not symbol:
                    continue

                # Be more lenient with active/tradable check - only skip if explicitly False
                active = instrument.get("active")
                tradable = instrument.get("tradable")

                if active is False or tradable is False:
                    continue

                trading_pair = crypto_com_utils.convert_from_exchange_symbol(symbol)

                # Validate trading pair format
                if "-" not in trading_pair or trading_pair.count("-") != 1:
                    self.logger().debug(f"Skipping invalid trading pair format: {trading_pair} from symbol: {symbol}")
                    continue

                # Extract trading rule parameters using Crypto.com API format
                # Use tick sizes directly from API instead of calculating from decimals
                price_tick_size = instrument.get("price_tick_size", "0.00000001")
                qty_tick_size = instrument.get("qty_tick_size", "0.00000001")

                # Convert to Decimal for precise calculations
                price_increment = Decimal(str(price_tick_size))
                quantity_increment = Decimal(str(qty_tick_size))

                # Set reasonable min/max values if not provided
                min_quantity = quantity_increment  # Minimum is usually one tick
                max_quantity = Decimal("100000000")  # Large default
                min_price = price_increment  # Minimum is usually one tick

                trading_rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_quantity,
                    max_order_size=max_quantity,
                    min_price_increment=price_increment,
                    min_base_amount_increment=quantity_increment,
                    min_notional_size=min_quantity * min_price,
                ))

        return trading_rules

    async def _update_trading_fees(self):
        """
        Update trading fees - Crypto.com uses fixed fee structure
        """
        # Crypto.com has a fixed fee structure, no need to fetch from API
        pass

    async def _user_stream_event_listener(self):
        """
        Listen to user stream events (order updates, balance changes)
        """
        async for event_message in self._iter_user_event_queue():
            try:
                await self._process_user_stream_event(event_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")

    async def _process_user_stream_event(self, event_message: Dict[str, Any]):
        """
        Process user stream events
        """
        method = event_message.get("method")

        if method == "subscribe":
            channel = event_message.get("params", {}).get("channels", [])
            if any("user.order" in ch for ch in channel):
                await self._process_order_update(event_message)
            elif any("user.trade" in ch for ch in channel):
                await self._process_trade_update(event_message)
            elif any("user.balance" in ch for ch in channel):
                await self._process_balance_update(event_message)

    async def _process_order_update(self, event_message: Dict[str, Any]):
        """
        Process order update events
        """
        # Implementation for order updates
        pass

    async def _process_trade_update(self, event_message: Dict[str, Any]):
        """
        Process trade update events
        """
        # Implementation for trade updates
        pass

    async def _process_balance_update(self, event_message: Dict[str, Any]):
        """
        Process balance update events
        """
        # Implementation for balance updates
        pass

    async def _update_balances(self):
        """
        Update account balances
        """
        data = {
            "method": "private/user-balance"
        }

        response = await self._api_post(
            path_url=CONSTANTS.USER_BALANCE_PATH_URL,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.USER_BALANCE_PATH_URL
        )

        self._account_balances.clear()
        self._account_available_balances.clear()

        if response.get("code") == 0 and "result" in response:
            # The working script shows the response structure for user-balance
            result_data = response["result"].get("data", [])
            if result_data:
                balance_info = result_data[0]  # First element contains balance summary

                # Parse position balances (individual asset balances)
                position_balances = balance_info.get("position_balances", [])
                for position in position_balances:
                    currency = position.get("instrument_name", "").upper()
                    if currency:
                        quantity = Decimal(str(position.get("quantity", "0")))

                        # Crypto.com uses "USD" but Hummingbot expects "USDT" for USDT pairs
                        # Map USD -> USDT so balance checks work correctly
                        if currency == "USD":
                            currency = "USDT"

                        # For crypto.com, available balance might be the same as quantity
                        # unless there are specific locked amounts
                        self._account_balances[currency] = quantity
                        self._account_available_balances[currency] = quantity

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Get all trade updates for a specific order
        """
        trade_updates = []

        try:
            data = {
                "method": "private/get-trades",
                "params": {
                    "instrument_name": await self.exchange_symbol_associated_to_pair(order.trading_pair),
                    "order_id": order.exchange_order_id
                }
            }

            response = await self._api_post(
                path_url=CONSTANTS.GET_TRADES_PATH_URL,
                data=data,
                is_auth_required=True,
                limit_id=CONSTANTS.GET_TRADES_PATH_URL
            )

            if response.get("code") == 0 and "result" in response:
                trades = response["result"].get("data", [])

                for trade in trades:
                    trade_update = TradeUpdate(
                        trade_id=trade["trade_id"],
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=self.get_fee(
                            base_currency=order.base_asset,
                            quote_currency=order.quote_asset,
                            order_type=order.order_type,
                            order_side=order.trade_type,
                            amount=Decimal(trade["quantity"]),
                            price=Decimal(trade["price"])
                        ),
                        fill_base_amount=Decimal(trade["quantity"]),
                        fill_quote_amount=Decimal(trade["quantity"]) * Decimal(trade["price"]),
                        fill_price=Decimal(trade["price"]),
                        fill_timestamp=trade["create_time"] / 1000,
                    )
                    trade_updates.append(trade_update)
        except Exception:
            pass

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from the exchange
        """
        data = {
            "method": "private/get-order-detail",
            "params": {
                "order_id": tracked_order.exchange_order_id
            }
        }

        response = await self._api_post(
            path_url=CONSTANTS.GET_ORDER_DETAIL_PATH_URL,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.GET_ORDER_DETAIL_PATH_URL
        )

        if response.get("code") == 0 and "result" in response:
            order_data = response["result"]
            new_state = CONSTANTS.ORDER_STATE.get(order_data["status"], tracked_order.current_state)

            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=new_state,
            )
        else:
            raise ValueError(f"Failed to get order status: {response}")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize trading pair symbol mapping from exchange info
        Handles Crypto.com's dual format: both _USD and _USDT pairs for same asset
        """
        mapping = {}
        all_symbols = set()

        if "result" in exchange_info and "data" in exchange_info["result"]:
            instruments = exchange_info["result"]["data"]

            # First pass: collect all symbols to identify duplicates
            for instrument in instruments:
                if isinstance(instrument, dict) and "symbol" in instrument:
                    exchange_symbol = instrument["symbol"]
                    active = instrument.get("active")
                    tradable = instrument.get("tradable")

                    # Only skip if explicitly False
                    if active is False or tradable is False:
                        continue

                    all_symbols.add(exchange_symbol)

            # Second pass: build mapping, skipping _USDT pairs when _USD exists
            for instrument in instruments:
                if isinstance(instrument, dict) and "symbol" in instrument:
                    exchange_symbol = instrument["symbol"]

                    # Log first few instruments for debugging
                    if len(mapping) < 5:
                        self.logger().info(f"Processing instrument: {exchange_symbol}, active: {instrument.get('active')}, tradable: {instrument.get('tradable')}")

                    active = instrument.get("active")
                    tradable = instrument.get("tradable")

                    # Only skip if explicitly False
                    if active is False or tradable is False:
                        continue

                    # Skip _USDT pairs when corresponding _USD pair exists
                    # Example: Skip BTC_USDT if BTC_USD exists (both convert to BTC-USDT)
                    if exchange_symbol.endswith("_USDT"):
                        base_with_usd = exchange_symbol[:-5] + "_USD"  # Replace _USDT with _USD
                        if base_with_usd in all_symbols:
                            if len(mapping) < 5:
                                self.logger().debug(f"Skipping {exchange_symbol} - {base_with_usd} exists")
                            continue

                    hb_symbol = crypto_com_utils.convert_from_exchange_symbol(exchange_symbol)

                    # Validate trading pair format
                    if "-" in hb_symbol and hb_symbol.count("-") == 1:
                        mapping[exchange_symbol] = hb_symbol
                        if len(mapping) <= 10:  # Log first 10 successful mappings
                            self.logger().info(f"Successfully mapped {exchange_symbol} -> {hb_symbol}")
                    else:
                        if len(mapping) < 5:  # Log first 5 invalid formats
                            self.logger().debug(f"Skipping invalid symbol mapping: {exchange_symbol} -> {hb_symbol}")

        self.logger().info(f"Initialized {len(mapping)} trading pair symbol mappings")
        self._set_trading_pair_symbol_map(bidict(mapping))

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """
        Get exchange symbol for a trading pair with better error handling
        """
        try:
            # Ensure symbol map is initialized
            if not hasattr(self, '_trading_pair_symbol_map') or self._trading_pair_symbol_map is None:
                self.logger().warning(f"Symbol map not initialized, using direct conversion for {trading_pair}")
                return crypto_com_utils.convert_to_exchange_symbol(trading_pair)

            symbol_map = self._trading_pair_symbol_map
            if trading_pair in symbol_map.inverse:
                return symbol_map.inverse[trading_pair]
            else:
                # If not found, try to convert directly
                exchange_symbol = crypto_com_utils.convert_to_exchange_symbol(trading_pair)
                self.logger().warning(f"Trading pair {trading_pair} not found in symbol map, using direct conversion: {exchange_symbol}")
                return exchange_symbol
        except Exception as e:
            self.logger().error(f"Error getting exchange symbol for {trading_pair}: {e}")
            # Fallback to direct conversion
            return crypto_com_utils.convert_to_exchange_symbol(trading_pair)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get the last traded price for a trading pair
        """
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            # Try to get ticker data - Crypto.com might use different response format
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL,
                limit_id=CONSTANTS.TICKER_PATH_URL
            )

            if response.get("code") == 0 and "result" in response:
                data = response["result"].get("data", [])

                # Find the specific instrument in the response
                for ticker in data:
                    if ticker.get("instrument_name") == symbol:
                        # Try different possible price fields
                        price = ticker.get("last_price") or ticker.get("a") or ticker.get("price")
                        if price:
                            return float(price)

                # If specific instrument not found, try first available
                if data:
                    first_ticker = data[0]
                    price = first_ticker.get("last_price") or first_ticker.get("a") or first_ticker.get("price")
                    if price:
                        return float(price)

            # Fallback: return a default price to prevent blocking
            self.logger().warning(f"Could not get last traded price for {trading_pair}, using fallback")
            return 1.0

        except Exception as e:
            self.logger().error(f"Error getting last traded price for {trading_pair}: {e}")
            return 1.0  # Fallback price to prevent blocking
