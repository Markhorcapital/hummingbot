import asyncio
import json
import re
import time
import unittest
from decimal import Decimal
from typing import Awaitable, Dict, NamedTuple, Optional
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.bing_x import bing_x_constants as CONSTANTS, bing_x_web_utils as web_utils
from hummingbot.connector.exchange.bing_x.bing_x_api_order_book_data_source import BingXAPIOrderBookDataSource
from hummingbot.connector.exchange.bing_x.bing_x_exchange import BingXExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketEvent, OrderCancelledEvent
from hummingbot.core.network_iterator import NetworkStatus


class TestBingXExchange(unittest.TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "AURA"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.api_key = "someKey"
        cls.api_passphrase = "somePassPhrase"
        cls.api_secret_key = "someSecretKey"

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.test_task: Optional[asyncio.Task] = None
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())

        self.exchange = BingXExchange(
            self.client_config_map,
            self.api_key,
            self.api_secret_key,
            trading_pairs=[self.trading_pair]
        )

        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(0)
        self.exchange._time_synchronizer.logger().setLevel(1)
        self.exchange._time_synchronizer.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

        self._initialize_event_loggers()

        BingXAPIOrderBookDataSource._trading_pair_symbol_map = {
            CONSTANTS.DEFAULT_DOMAIN: bidict(
                {self.ex_trading_pair: self.trading_pair})
        }

    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        BingXAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.buy_order_created_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_failure_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.sell_order_created_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.BuyOrderCreated, self.buy_order_created_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFailure, self.order_failure_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.SellOrderCreated, self.sell_order_created_logger)]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_exchange_rules_mock(self) -> Dict:
        exchange_rules = {
            "code": 0,
            "msg": "",
            "debugMsg": "",
            "data": {
                "symbols": [
                    {
                        "symbol": "AURA-USDT",
                        "minQty": 74,
                        "maxQty": 296331.5,
                        "minNotional": 5,
                        "maxNotional": 20000,
                        "status": 1,
                        "tickSize": 0.000001,
                        "stepSize": 0.1
                    }
                ]
            }
        }
        return exchange_rules

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal(str(0.01)),
                min_price_increment=Decimal(str(0.0001)),
                min_base_amount_increment=Decimal(str(0.000001)),
            )
        }

    def _validate_auth_credentials_present(self, request_call_tuple: NamedTuple):
        request_headers = request_call_tuple.kwargs["headers"]
        request_params = request_call_tuple.kwargs["params"]
        self.assertIn("Content-Type", request_headers)
        self.assertEqual("application/json", request_headers["Content-Type"])
        self.assertIn("X-BX-APIKEY", request_headers)
        self.assertIn("timestamp", request_params)
        self.assertIn("signature", request_params)

    def test_supported_order_types(self):
        supported_types = self.exchange.supported_order_types()
        self.assertIn(OrderType.MARKET, supported_types)
        self.assertIn(OrderType.LIMIT, supported_types)
        # self.assertIn(OrderType.LIMIT_MAKER, supported_types)

    @aioresponses()
    def test_check_network_success(self, mock_api):
        url = web_utils.rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL)
        resp = {"code": 0, "msg": "", "data": {"serverTime": 1698895668179}}
        mock_api.get(url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(NetworkStatus.CONNECTED, ret)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        mock_api.get(url, status=500)

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(ret, NetworkStatus.NOT_CONNECTED)

    @aioresponses()
    def test_check_network_raises_cancel_exception(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)

        mock_api.get(url, exception=asyncio.CancelledError)

        self.assertRaises(asyncio.CancelledError, self.async_run_with_timeout, self.exchange.check_network())

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = web_utils.rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        resp = self.get_exchange_rules_mock()
        mock_api.get(url, body=json.dumps(resp))

        get_last_traded_price_url = web_utils.rest_url(CONSTANTS.LAST_TRADED_PRICE_PATH)
        get_last_traded_price_url_regex_url = re.compile(f"^{get_last_traded_price_url}".replace(".", r"\.").replace("?", r"\?"))
        resp = {
            "data": [
                {
                    "lastPrice": 0.00001
                }
            ]
        }
        mock_api.get(get_last_traded_price_url_regex_url, body=json.dumps(resp))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertTrue(self.trading_pair in self.exchange._trading_rules)

    @aioresponses()
    def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = web_utils.rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        exchange_rules = {
            "code": 0,
            "msg": "",
            "debugMsg": "",
            "data": {
                "symbols": [
                    {
                        "symbol": "AURA-USDT"
                    }
                ]
            }
        }
        mock_api.get(url, body=json.dumps(exchange_rules))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertEqual(0, len(self.exchange._trading_rules))

    def test_initial_status_dict(self):
        BingXAPIOrderBookDataSource._trading_pair_symbol_map = {}

        status_dict = self.exchange.status_dict

        expected_initial_dict = {
            "symbols_mapping_initialized": False,
            "order_books_initialized": False,
            "account_balance": False,
            "trading_rule_initialized": False,
            "user_stream_initialized": False,
        }

        self.assertEqual(expected_initial_dict, status_dict)
        self.assertFalse(self.exchange.ready)

    def test_get_fee_returns_fee_from_exchange_if_available_and_default_if_not(self):
        fee = self.exchange.get_fee(
            base_currency="SOME",
            quote_currency="OTHER",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("10"),
            price=Decimal("20"),
        )

        self.assertEqual(Decimal("0.001"), fee.percent)  # default fee

    # @patch("hummingbot.connector.utils.get_tracking_nonce")
    # def test_client_order_id_on_order(self, mocked_nonce):
    #     mocked_nonce.return_value = 9

    #     result = self.exchange.buy(
    #         trading_pair=self.trading_pair,
    #         amount=Decimal("1"),
    #         order_type=OrderType.LIMIT,
    #         price=Decimal("2"),
    #     )
    #     expected_client_order_id = get_new_client_order_id(
    #         is_buy=True, trading_pair=self.trading_pair,
    #         hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
    #         max_id_len=CONSTANTS.MAX_ORDER_ID_LEN
    #     )

    #     self.assertEqual(result, expected_client_order_id)

    #     result = self.exchange.sell(
    #         trading_pair=self.trading_pair,
    #         amount=Decimal("1"),
    #         order_type=OrderType.LIMIT,
    #         price=Decimal("2"),
    #     )
    #     expected_client_order_id = get_new_client_order_id(
    #         is_buy=False, trading_pair=self.trading_pair,
    #         hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
    #         max_id_len=CONSTANTS.MAX_ORDER_ID_LEN
    #     )

    #     self.assertEqual(result, expected_client_order_id)

    def test_restore_tracking_states_only_registers_open_orders(self):
        orders = []
        orders.append(InFlightOrder(
            client_order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
        ))
        orders.append(InFlightOrder(
            client_order_id="OID2",
            exchange_order_id="EOID2",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.CANCELED
        ))
        orders.append(InFlightOrder(
            client_order_id="OID3",
            exchange_order_id="EOID3",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        ))
        orders.append(InFlightOrder(
            client_order_id="OID4",
            exchange_order_id="EOID4",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FAILED
        ))

        tracking_states = {order.client_order_id: order.to_json() for order in orders}

        self.exchange.restore_tracking_states(tracking_states)

        self.assertIn("OID1", self.exchange.in_flight_orders)
        self.assertNotIn("OID2", self.exchange.in_flight_orders)
        self.assertNotIn("OID3", self.exchange.in_flight_orders)
        self.assertNotIn("OID4", self.exchange.in_flight_orders)

    @aioresponses()
    def test_create_limit_order_successfully(self, mock_api):
        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)
        url = web_utils.rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        creation_response = {
            "code": 0,
            "msg": "",
            "debugMsg": "",
            "data": {
                "symbol": "AURA-USDT",
                "orderId": 1719980066923872256,
                "transactTime": 1698910178296,
                "price": "0.05",
                "origQty": "100",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "status": "PENDING",
                "type": "LIMIT",
                "side": "SELL"
            }
        }
        tradingrule_url = web_utils.rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        resp = self.get_exchange_rules_mock()
        mock_api.get(tradingrule_url, body=json.dumps(resp))
        mock_api.post(regex_url,
                      body=json.dumps(creation_response),
                      callback=lambda *args, **kwargs: request_sent_event.set())

        self.test_task = asyncio.get_event_loop().create_task(
            self.exchange._create_order(trade_type=TradeType.BUY,
                                        order_id="OID1",
                                        trading_pair=self.trading_pair,
                                        amount=Decimal("100"),
                                        order_type=OrderType.LIMIT,
                                        price=Decimal("0.05")))
        self.async_run_with_timeout(request_sent_event.wait())

        order_request = next(((key, value) for key, value in mock_api.requests.items()
                              if key[1].human_repr().startswith(url)))
        self._validate_auth_credentials_present(order_request[1][0])
        request_params = order_request[1][0].kwargs["params"]
        self.assertEqual("AURA-USDT", request_params["symbol"])
        self.assertEqual("BUY", request_params["side"])
        self.assertEqual("LIMIT", request_params["type"])
        self.assertEqual(Decimal("100"), Decimal(request_params["quantity"]))
        self.assertEqual(Decimal("0.05"), Decimal(request_params["price"]))
        self.assertEqual("OID1", request_params["newClientOrderId"])

        self.assertIn("OID1", self.exchange.in_flight_orders)
        create_event: BuyOrderCreatedEvent = self.buy_order_created_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, create_event.timestamp)
        self.assertEqual(self.trading_pair, create_event.trading_pair)
        self.assertEqual(OrderType.LIMIT, create_event.type)
        self.assertEqual(Decimal("100"), create_event.amount)
        self.assertEqual(Decimal("0.05"), create_event.price)
        self.assertEqual("OID1", create_event.order_id)
        self.assertEqual(str(creation_response["data"]["orderId"]), create_event.exchange_order_id)

        self.assertTrue(
            self._is_logged(
                "INFO",
                f"""Created LIMIT BUY order {request_params["newClientOrderId"]} for {Decimal(request_params["quantity"])} {self.trading_pair} at {Decimal(request_params["price"])}."""
            )
        )

    def test_parse_spot_order_response_raises_on_api_error(self):
        with self.assertRaises(IOError) as ctx:
            BingXExchange._parse_spot_order_response(
                {"code": 80014, "msg": "timestamp is invalid", "data": {}},
                "place order",
                "ALI-USDT",
            )
        self.assertIn("80014", str(ctx.exception))
        self.assertIn("timestamp is invalid", str(ctx.exception))
        self.assertNotIn("'data'", str(ctx.exception))

    def test_parse_spot_order_response_success(self):
        order_id, transact_time = BingXExchange._parse_spot_order_response(
            {
                "code": 0,
                "msg": "",
                "data": {
                    "orderId": 1719980066923872256,
                    "transactTime": 1698910178296000,
                },
            },
            "place order",
            "ALI-USDT",
        )
        self.assertEqual("1719980066923872256", order_id)
        self.assertEqual(1698910178296.0, transact_time)

    def test_parse_spot_order_response_missing_data_raises_clear_error(self):
        with self.assertRaises(IOError) as ctx:
            BingXExchange._parse_spot_order_response(
                {"code": 0, "msg": "", "data": {}},
                "place order",
                "ALI-USDT",
            )
        self.assertIn("missing orderId", str(ctx.exception))
        self.assertNotIn("KeyError", str(ctx.exception))

    @aioresponses()
    def test_cancel_order_successfully(self, mock_api):
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="4",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn("OID1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
            "code": 0,
            "msg": "",
            "debugMsg": "",
            "data": {
                "symbol": "AURA-USDT",
                "orderId": 1719980066923872256,
                "clientOrderID": "OID1",
                "price": "0.05",
                "origQty": "100",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "status": "CANCELED",
                "type": "LIMIT",
                "side": "SELL"
            }
        }

        mock_api.post(regex_url,
                      body=json.dumps(response),
                      callback=lambda *args, **kwargs: request_sent_event.set())

        self.exchange.cancel(client_order_id="OID1", trading_pair=self.trading_pair)
        self.async_run_with_timeout(request_sent_event.wait())

        cancel_request = next(((key, value) for key, value in mock_api.requests.items()
                               if key[1].human_repr().startswith(url)))
        self._validate_auth_credentials_present(cancel_request[1][0])

        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order.client_order_id, cancel_event.order_id)

        self.assertTrue(
            self._is_logged(
                "INFO",
                f"Successfully canceled order {order.client_order_id}."
            )
        )

    @aioresponses()
    def test_request_order_status_uses_query_endpoint(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="1735965009395131234",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.rest_url(CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, body=json.dumps({
            "code": 0,
            "msg": "",
            "data": {
                "symbol": self.trading_pair,
                "orderId": 1735965009395131234,
                "status": "NEW",
                "updateTime": 1698910178296,
            },
        }))

        update = self.async_run_with_timeout(self.exchange._request_order_status(order))

        self.assertEqual(update.new_state, OrderState.OPEN)
        self.assertEqual(update.exchange_order_id, "1735965009395131234")
        order_request = next(((key, value) for key, value in mock_api.requests.items()
                              if key[1].human_repr().startswith(url)))
        self._validate_auth_credentials_present(order_request[1][0])
        request_params = order_request[1][0].kwargs["params"]
        self.assertEqual(request_params["symbol"], self.trading_pair)
        self.assertEqual(request_params["orderId"], "1735965009395131234")

    @aioresponses()
    def test_request_order_status_list_data_payload(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="1735965009395131234",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.rest_url(CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, body=json.dumps({
            "code": 0,
            "msg": "",
            "data": [{
                "symbol": self.trading_pair,
                "orderId": 1735965009395131234,
                "status": "NEW",
                "updateTime": 1698910178296,
            }],
        }))

        update = self.async_run_with_timeout(self.exchange._request_order_status(order))
        self.assertEqual(update.new_state, OrderState.OPEN)

    @aioresponses()
    def test_request_order_status_missing_data_marks_canceled(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="1735965009395131234",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.rest_url(CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, body=json.dumps({"code": 0, "msg": "", "data": []}))

        update = self.async_run_with_timeout(self.exchange._request_order_status(order))
        self.assertEqual(update.new_state, OrderState.CANCELED)

    @aioresponses()
    def test_all_trade_updates_for_order_handles_list_and_empty(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="1735965009395131234",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.rest_url(CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, body=json.dumps({"code": 0, "msg": "", "data": []}))
        empty_updates = self.async_run_with_timeout(self.exchange._all_trade_updates_for_order(order))
        self.assertEqual(empty_updates, [])

        mock_api.get(regex_url, body=json.dumps({
            "code": 0,
            "msg": "",
            "data": [{
                "orderId": 1735965009395131234,
                "price": "0.05",
                "executedQty": "10",
                "fee": "0.001",
                "feeAsset": "USDT",
                "updateTime": 1698910178296,
            }],
        }))
        list_updates = self.async_run_with_timeout(self.exchange._all_trade_updates_for_order(order))
        self.assertEqual(1, len(list_updates))
        self.assertEqual(Decimal("10"), list_updates[0].fill_base_amount)
    def test_cancel_all_open_orders_bulk_success_marks_tracked_orders_canceled(self, mock_sleep):
        self.exchange._set_current_timestamp(1640780000)
        for idx, order_id in enumerate(["OID1", "OID2"], start=1):
            self.exchange.start_tracking_order(
                order_id=order_id,
                exchange_order_id=str(idx),
                trading_pair=self.trading_pair,
                trade_type=TradeType.BUY,
                price=Decimal("0.05"),
                amount=Decimal("100"),
                order_type=OrderType.LIMIT,
            )

        self.exchange._api_post = AsyncMock(return_value={"code": 0, "msg": "", "data": {"orders": []}})

        async def _set_balances():
            self.exchange._account_available_balances["AURA"] = Decimal("500")
            self.exchange._account_balances["AURA"] = Decimal("500")

        self.exchange._update_balances = AsyncMock(side_effect=_set_balances)

        results = self.async_run_with_timeout(
            self.exchange.cancel_all_open_orders_for_trading_pair(self.trading_pair)
        )

        self.assertEqual(2, len(results))
        self.assertTrue(all(result.success for result in results))
        self.assertTrue(all(order.is_done for order in self.exchange.in_flight_orders.values()))
        self.assertEqual(Decimal("500"), self.exchange.available_balances["AURA"])
        mock_sleep.assert_awaited_once_with(CONSTANTS.POST_CANCEL_BALANCE_DELAY_SECONDS)
        self.exchange._update_balances.assert_awaited_once()

    @patch("hummingbot.connector.exchange.bing_x.bing_x_exchange.asyncio.sleep", new_callable=AsyncMock)
    def test_cancel_all_open_orders_bulk_success_marks_tracked_and_refreshes_balances(self, mock_sleep):
        self.exchange._set_current_timestamp(1640780000)
        for idx, order_id in enumerate(["OID1", "OID2"], start=1):
            self.exchange.start_tracking_order(
                order_id=order_id,
                exchange_order_id=str(idx),
                trading_pair=self.trading_pair,
                trade_type=TradeType.BUY,
                price=Decimal("0.05"),
                amount=Decimal("100"),
                order_type=OrderType.LIMIT,
            )

        self.exchange._api_post = AsyncMock(return_value={"code": 0, "msg": "", "data": {"orders": []}})
        self.exchange._update_balances = AsyncMock()
        call_order = []

        async def _sleep(*_a, **_k):
            call_order.append("sleep")

        async def _balances(*_a, **_k):
            call_order.append("balances")

        mock_sleep.side_effect = _sleep
        self.exchange._update_balances.side_effect = _balances

        results = self.async_run_with_timeout(
            self.exchange.cancel_all_open_orders_for_trading_pair(self.trading_pair)
        )

        self.assertEqual(2, len(results))
        self.assertTrue(all(result.success for result in results))
        self.assertTrue(all(order.is_done for order in self.exchange.in_flight_orders.values()))
        self.exchange._api_post.assert_awaited_once()
        self.assertEqual(
            CONSTANTS.CANCEL_OPEN_ORDERS_PATH_URL,
            self.exchange._api_post.await_args.kwargs["path_url"],
        )
        self.assertEqual({"symbol": self.trading_pair}, self.exchange._api_post.await_args.kwargs["params"])
        mock_sleep.assert_awaited_once_with(CONSTANTS.POST_CANCEL_BALANCE_DELAY_SECONDS)
        self.exchange._update_balances.assert_awaited_once()
        self.assertEqual(["sleep", "balances"], call_order)

    def test_note_order_endpoint_rate_limit_parses_unblock_timestamp(self):
        err = IOError(
            "BingX place order for AURA-USDT failed: code=100410 "
            "msg=code:100410:The endpoint trigger frequency limit rule is currently "
            "in the disabled period and will be unblocked after 1786014979004"
        )
        self.exchange._note_order_endpoint_rate_limit(err)
        self.assertEqual(1786014979.004, self.exchange._order_endpoint_unblocked_at)

    def test_is_order_rate_limit_error(self):
        self.assertTrue(self.exchange._is_order_rate_limit_error(IOError("code=100410 msg=rate limited")))
        self.assertTrue(self.exchange._is_order_rate_limit_error(IOError("disabled period")))
        self.assertFalse(self.exchange._is_order_rate_limit_error(IOError("code=100202 insufficient")))

    @patch("hummingbot.connector.exchange.bing_x.bing_x_exchange.asyncio.sleep", new_callable=AsyncMock)
    def test_place_order_waits_and_retries_after_100410(self, mock_sleep):
        unblock_ms = int((time.time() + 30) * 1000)
        rate_limited = {
            "code": 100410,
            "msg": (
                "code:100410:The endpoint trigger frequency limit rule is currently "
                f"in the disabled period and will be unblocked after {unblock_ms}"
            ),
            "data": {},
        }
        success = {
            "code": 0,
            "msg": "",
            "data": {"orderId": 12345, "transactTime": 1640780000000},
        }
        self.exchange._api_post = AsyncMock(side_effect=[rate_limited, success])

        exchange_order_id, _ = self.async_run_with_timeout(
            self.exchange._place_order(
                order_id="OID-RATE",
                trading_pair=self.trading_pair,
                amount=Decimal("10"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("1"),
            ),
            timeout=5,
        )

        self.assertEqual("12345", exchange_order_id)
        self.assertEqual(2, self.exchange._api_post.await_count)
        self.assertTrue(mock_sleep.await_count >= 1)

    def test_cancel_all_open_orders_falls_back_when_bulk_fails(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.SELL,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.exchange._api_post = AsyncMock(return_value={"code": 100500, "msg": "System busy", "data": {}})
        self.exchange._place_cancel = AsyncMock(return_value=True)

        results = self.async_run_with_timeout(
            self.exchange.cancel_all_open_orders_for_trading_pair(self.trading_pair)
        )

        self.assertEqual(1, len(results))
        self.assertTrue(results[0].success)
        self.assertTrue(
            self._is_logged(
                "WARNING",
                f"BingX cancelOpenOrders for {self.trading_pair} returned code=100500 msg=System busy",
            )
        )
        self.exchange._place_cancel.assert_awaited()

    def test_place_cancel_uses_client_order_id_casing(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id=None,
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )
        order = self.exchange.in_flight_orders["OID1"]
        self.exchange._api_post = AsyncMock(return_value={
            "code": 0,
            "msg": "",
            "data": {"symbol": self.trading_pair, "orderId": 99, "clientOrderID": "OID1", "status": "CANCELED"},
        })

        cancelled = self.async_run_with_timeout(self.exchange._place_cancel("OID1", order))
        self.assertTrue(cancelled)
        params = self.exchange._api_post.await_args.kwargs["params"]
        self.assertEqual(params["clientOrderID"], "OID1")
        self.assertNotIn("clientOrderId", params)

    def test_place_cancel_treats_already_canceled_as_success(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="123",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("0.05"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )
        order = self.exchange.in_flight_orders["OID1"]
        self.exchange._api_post = AsyncMock(return_value={
            "code": 100404,
            "msg": "Order does not exist",
            "data": {},
        })

        cancelled = self.async_run_with_timeout(self.exchange._place_cancel("OID1", order))
        self.assertTrue(cancelled)
        self.assertTrue(order.is_done)

    @aioresponses()
    def test_update_balances(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.ACCOUNTS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {"code": 0, "msg": "", "debugMsg": "", "data": {"balances": [{"asset": "AURA", "free": "1000", "locked": "0"}]}}

        mock_api.get(regex_url, body=json.dumps(response))
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        # total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("1000"), available_balances["AURA"])

        response = {"code": 0, "msg": "", "debugMsg": "", "data": {"balances": [{"asset": "AURA", "free": "2000", "locked": "0"}]}}

        mock_api.get(regex_url, body=json.dumps(response))
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances

        self.assertEqual(Decimal("2000"), available_balances["AURA"])

    # @aioresponses()
    # def test_update_order_status_when_filled(self, mock_api):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           10 - 1)

    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

    #     url = web_utils.rest_url(CONSTANTS.ORDER_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     order_status = {
    #         "ret_code": 0,
    #         "ret_msg": "",
    #         "ext_code": None,
    #         "ext_info": None,
    #         "result": {
    #             "accountId": "1054",
    #             "exchangeId": "301",
    #             "symbol": self.trading_pair,
    #             "symbolName": "ETHUSDT",
    #             "orderLinkId": order.client_order_id,
    #             "orderId": order.exchange_order_id,
    #             "price": "20000",
    #             "origQty": "1",
    #             "executedQty": "1",
    #             "cummulativeQuoteQty": "1",
    #             "avgPrice": "1000",
    #             "status": "FILLED",
    #             "timeInForce": "GTC",
    #             "type": "LIMIT",
    #             "side": order.trade_type.name,
    #             "stopPrice": "0.0",
    #             "icebergQty": "0.0",
    #             "time": "1620811601728",
    #             "updateTime": "1620811601743",
    #             "isWorking": True
    #         }
    #     }

    #     mock_api.get(regex_url, body=json.dumps(order_status))

    #     # Simulate the order has been filled with a TradeUpdate
    #     order.completely_filled_event.set()
    #     self.async_run_with_timeout(self.exchange._update_order_status())
    #     self.async_run_with_timeout(order.wait_until_completely_filled())

    #     order_request = next(((key, value) for key, value in mock_api.requests.items()
    #                           if key[1].human_repr().startswith(url)))
    #     self._validate_auth_credentials_present(order_request[1][0])

    #     self.assertTrue(order.is_filled)
    #     self.assertTrue(order.is_done)

    #     buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
    #     self.assertEqual(order.client_order_id, buy_event.order_id)
    #     self.assertEqual(order.base_asset, buy_event.base_asset)
    #     self.assertEqual(order.quote_asset, buy_event.quote_asset)
    #     self.assertEqual(Decimal(0), buy_event.base_asset_amount)
    #     self.assertEqual(Decimal(0), buy_event.quote_asset_amount)
    #     self.assertEqual(order.order_type, buy_event.order_type)
    #     self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
    #     self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
    #     self.assertTrue(
    #         self._is_logged(
    #             "INFO",
    #             f"BUY order {order.client_order_id} completely filled."
    #         )
    #     )

    # @aioresponses()
    # def test_update_order_status_when_cancelled(self, mock_api):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           10 - 1)

    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="100234",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order = self.exchange.in_flight_orders["OID1"]

    #     url = web_utils.rest_url(CONSTANTS.ORDER_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     order_status = {
    #         "ret_code": 0,
    #         "ret_msg": "",
    #         "ext_code": None,
    #         "ext_info": None,
    #         "result": {
    #             "accountId": "1054",
    #             "exchangeId": "301",
    #             "symbol": self.trading_pair,
    #             "symbolName": "ETHUSDT",
    #             "orderLinkId": order.client_order_id,
    #             "orderId": order.exchange_order_id,
    #             "price": "10000",
    #             "origQty": "1",
    #             "executedQty": "1",
    #             "cummulativeQuoteQty": "1",
    #             "avgPrice": "1000",
    #             "status": "CANCELED",
    #             "timeInForce": "GTC",
    #             "type": "LIMIT",
    #             "side": order.trade_type.name,
    #             "stopPrice": "0.0",
    #             "icebergQty": "0.0",
    #             "time": "1620811601728",
    #             "updateTime": "1620811601743",
    #             "isWorking": True
    #         }
    #     }

    #     mock_api.get(regex_url, body=json.dumps(order_status))

    #     self.async_run_with_timeout(self.exchange._update_order_status())

    #     order_request = next(((key, value) for key, value in mock_api.requests.items()
    #                           if key[1].human_repr().startswith(url)))
    #     self._validate_auth_credentials_present(order_request[1][0])

    #     cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
    #     self.assertEqual(order.client_order_id, cancel_event.order_id)
    #     self.assertEqual(order.exchange_order_id, cancel_event.exchange_order_id)
    #     self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
    #     self.assertTrue(
    #         self._is_logged("INFO", f"Successfully canceled order {order.client_order_id}.")
    #     )

    # @aioresponses()
    # def test_update_order_status_when_order_has_not_changed(self, mock_api):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           10 - 1)

    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

    #     url = web_utils.rest_url(CONSTANTS.ORDER_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     order_status = {
    #         "ret_code": 0,
    #         "ret_msg": "",
    #         "ext_code": None,
    #         "ext_info": None,
    #         "result": {
    #             "accountId": "1054",
    #             "exchangeId": "301",
    #             "symbol": self.trading_pair,
    #             "symbolName": "ETHUSDT",
    #             "orderLinkId": order.client_order_id,
    #             "orderId": order.exchange_order_id,
    #             "price": "10000",
    #             "origQty": "1",
    #             "executedQty": "1",
    #             "cummulativeQuoteQty": "1",
    #             "avgPrice": "1000",
    #             "status": "NEW",
    #             "timeInForce": "GTC",
    #             "type": "LIMIT",
    #             "side": order.trade_type.name,
    #             "stopPrice": "0.0",
    #             "icebergQty": "0.0",
    #             "time": "1620811601728",
    #             "updateTime": "1620811601743",
    #             "isWorking": True
    #         }
    #     }

    #     mock_response = order_status
    #     mock_api.get(regex_url, body=json.dumps(mock_response))

    #     self.assertTrue(order.is_open)

    #     self.async_run_with_timeout(self.exchange._update_order_status())

    #     order_request = next(((key, value) for key, value in mock_api.requests.items()
    #                           if key[1].human_repr().startswith(url)))
    #     self._validate_auth_credentials_present(order_request[1][0])

    #     self.assertTrue(order.is_open)
    #     self.assertFalse(order.is_filled)
    #     self.assertFalse(order.is_done)

    # @aioresponses()
    # def test_update_order_status_when_request_fails_marks_order_as_not_found(self, mock_api):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           10 - 1)

    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

    #     url = web_utils.rest_url(CONSTANTS.ORDER_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     mock_api.get(regex_url, status=404)

    #     self.async_run_with_timeout(self.exchange._update_order_status())

    #     order_request = next(((key, value) for key, value in mock_api.requests.items()
    #                           if key[1].human_repr().startswith(url)))
    #     self._validate_auth_credentials_present(order_request[1][0])

    #     self.assertTrue(order.is_open)
    #     self.assertFalse(order.is_filled)
    #     self.assertFalse(order.is_done)

    #     self.assertEqual(1, self.exchange._order_tracker._order_not_found_records[order.client_order_id])

    # def test_user_stream_update_for_new_order_does_not_update_status(self):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order = self.exchange.in_flight_orders["OID1"]

    #     event_message = {
    #         "e": "executionReport",
    #         "E": "1499405658658",
    #         "s": order.trading_pair,
    #         "c": order.client_order_id,
    #         "S": order.trade_type.name,
    #         "o": "LIMIT",
    #         "f": "GTC",
    #         "q": "1.00000000",
    #         "p": "0.10264410",
    #         "X": "NEW",
    #         "i": order.exchange_order_id,
    #         "M": "0",
    #         "l": "0.00000000",
    #         "z": "0.00000000",
    #         "L": "0.00000000",
    #         "n": "0",
    #         "N": "COINALPHA",
    #         "u": True,
    #         "w": True,
    #         "m": False,
    #         "O": "1499405658657",
    #         "Z": "473.199",
    #         "A": "0",
    #         "C": False,
    #         "v": "0"
    #     }

    #     mock_queue = AsyncMock()
    #     mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
    #     self.exchange._user_stream_tracker._user_stream = mock_queue

    #     try:
    #         self.async_run_with_timeout(self.exchange._user_stream_event_listener())
    #     except asyncio.CancelledError:
    #         pass

    #     event: BuyOrderCreatedEvent = self.buy_order_created_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, event.timestamp)
    #     self.assertEqual(order.order_type, event.type)
    #     self.assertEqual(order.trading_pair, event.trading_pair)
    #     self.assertEqual(order.amount, event.amount)
    #     self.assertEqual(order.price, event.price)
    #     self.assertEqual(order.client_order_id, event.order_id)
    #     self.assertEqual(order.exchange_order_id, event.exchange_order_id)
    #     self.assertTrue(order.is_open)

    #     self.assertTrue(
    #         self._is_logged(
    #             "INFO",
    #             f"Created {order.order_type.name.upper()} {order.trade_type.name.upper()} order "
    #             f"{order.client_order_id} for {order.amount} {order.trading_pair}."
    #         )
    #     )

    # def test_user_stream_update_for_cancelled_order(self):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order = self.exchange.in_flight_orders["OID1"]

    #     event_message = {
    #         "e": "executionReport",
    #         "E": "1499405658658",
    #         "s": order.trading_pair,
    #         "c": order.client_order_id,
    #         "S": order.trade_type.name,
    #         "o": "LIMIT",
    #         "f": "GTC",
    #         "q": "1.00000000",
    #         "p": "0.10264410",
    #         "X": "CANCELED",
    #         "i": order.exchange_order_id,
    #         "M": "0",
    #         "l": "0.00000000",
    #         "z": "0.00000000",
    #         "L": "0.00000000",
    #         "n": "0",
    #         "N": "COINALPHA",
    #         "u": True,
    #         "w": True,
    #         "m": False,
    #         "O": "1499405658657",
    #         "Z": "473.199",
    #         "A": "0",
    #         "C": False,
    #         "v": "0"
    #     }

    #     mock_queue = AsyncMock()
    #     mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
    #     self.exchange._user_stream_tracker._user_stream = mock_queue

    #     try:
    #         self.async_run_with_timeout(self.exchange._user_stream_event_listener())
    #     except asyncio.CancelledError:
    #         pass

    #     cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
    #     self.assertEqual(order.client_order_id, cancel_event.order_id)
    #     self.assertEqual(order.exchange_order_id, cancel_event.exchange_order_id)
    #     self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
    #     self.assertTrue(order.is_cancelled)
    #     self.assertTrue(order.is_done)

    #     self.assertTrue(
    #         self._is_logged("INFO", f"Successfully canceled order {order.client_order_id}.")
    #     )

    # def test_user_stream_update_for_order_partial_fill(self):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order = self.exchange.in_flight_orders["OID1"]

    #     event_message = {
    #         "e": "executionReport",
    #         "t": "1499405658658",
    #         "E": "1499405658658",
    #         "s": order.trading_pair,
    #         "c": order.client_order_id,
    #         "S": order.trade_type.name,
    #         "o": "LIMIT",
    #         "f": "GTC",
    #         "q": order.amount,
    #         "p": order.price,
    #         "X": "PARTIALLY_FILLED",
    #         "i": order.exchange_order_id,
    #         "M": "0",
    #         "l": "0.50000000",
    #         "z": "0.50000000",
    #         "L": "0.10250000",
    #         "n": "0.003",
    #         "N": self.base_asset,
    #         "u": True,
    #         "w": True,
    #         "m": False,
    #         "O": "1499405658657",
    #         "Z": "473.199",
    #         "A": "0",
    #         "C": False,
    #         "v": "0"
    #     }

    #     mock_queue = AsyncMock()
    #     mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
    #     self.exchange._user_stream_tracker._user_stream = mock_queue

    #     try:
    #         self.async_run_with_timeout(self.exchange._user_stream_event_listener())
    #     except asyncio.CancelledError:
    #         pass

    #     self.assertTrue(order.is_open)
    #     self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

    #     fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
    #     self.assertEqual(order.client_order_id, fill_event.order_id)
    #     self.assertEqual(order.trading_pair, fill_event.trading_pair)
    #     self.assertEqual(order.trade_type, fill_event.trade_type)
    #     self.assertEqual(order.order_type, fill_event.order_type)
    #     self.assertEqual(Decimal(event_message["L"]), fill_event.price)
    #     self.assertEqual(Decimal(event_message["l"]), fill_event.amount)

    #     self.assertEqual([TokenAmount(amount=Decimal(event_message["n"]), token=(event_message["N"]))],
    #                      fill_event.trade_fee.flat_fees)

    #     self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

    #     self.assertTrue(
    #         self._is_logged("INFO", f"The {order.trade_type.name} order {order.client_order_id} amounting to "
    #                                 f"{fill_event.amount}/{order.amount} {order.base_asset} has been filled.")
    #     )

    # def test_user_stream_update_for_order_fill(self):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="EOID1",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order = self.exchange.in_flight_orders["OID1"]

    #     event_message = {
    #         "e": "executionReport",
    #         "t": "1499405658658",
    #         "E": "1499405658658",
    #         "s": order.trading_pair,
    #         "c": order.client_order_id,
    #         "S": order.trade_type.name,
    #         "o": "LIMIT",
    #         "f": "GTC",
    #         "q": order.amount,
    #         "p": order.price,
    #         "X": "FILLED",
    #         "i": order.exchange_order_id,
    #         "M": "0",
    #         "l": order.amount,
    #         "z": "0.50000000",
    #         "L": order.price,
    #         "n": "0.003",
    #         "N": self.base_asset,
    #         "u": True,
    #         "w": True,
    #         "m": False,
    #         "O": "1499405658657",
    #         "Z": "473.199",
    #         "A": "0",
    #         "C": False,
    #         "v": "0"
    #     }

    #     filled_event = {
    #         "e": "ticketInfo",
    #         "E": "1621912542359",
    #         "s": self.ex_trading_pair,
    #         "q": "0.001639",
    #         "t": "1621912542314",
    #         "p": "61000.0",
    #         "T": "899062000267837441",
    #         "o": "899048013515737344",
    #         "c": "1621910874883",
    #         "O": "899062000118679808",
    #         "a": "10043",
    #         "A": "10024",
    #         "m": True
    #     }

    #     mock_queue = AsyncMock()
    #     mock_queue.get.side_effect = [event_message, filled_event, asyncio.CancelledError]
    #     self.exchange._user_stream_tracker._user_stream = mock_queue

    #     try:
    #         self.async_run_with_timeout(self.exchange._user_stream_event_listener())
    #     except asyncio.CancelledError:
    #         pass

    #     fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
    #     self.assertEqual(order.client_order_id, fill_event.order_id)
    #     self.assertEqual(order.trading_pair, fill_event.trading_pair)
    #     self.assertEqual(order.trade_type, fill_event.trade_type)
    #     self.assertEqual(order.order_type, fill_event.order_type)
    #     match_price = Decimal(event_message["L"])
    #     match_size = Decimal(event_message["l"])
    #     self.assertEqual(match_price, fill_event.price)
    #     self.assertEqual(match_size, fill_event.amount)
    #     self.assertEqual([TokenAmount(amount=Decimal(event_message["n"]), token=(event_message["N"]))],
    #                      fill_event.trade_fee.flat_fees)

    #     buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
    #     self.assertEqual(order.client_order_id, buy_event.order_id)
    #     self.assertEqual(order.base_asset, buy_event.base_asset)
    #     self.assertEqual(order.quote_asset, buy_event.quote_asset)
    #     self.assertEqual(order.amount, buy_event.base_asset_amount)
    #     self.assertEqual(order.amount * match_price, buy_event.quote_asset_amount)
    #     self.assertEqual(order.order_type, buy_event.order_type)
    #     self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
    #     self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
    #     self.assertTrue(order.is_filled)
    #     self.assertTrue(order.is_done)

    #     self.assertTrue(
    #         self._is_logged(
    #             "INFO",
    #             f"BUY order {order.client_order_id} completely filled."
    #         )
    #     )

    # def test_user_stream_balance_update(self):
    #     self.exchange._set_current_timestamp(1640780000)

    #     event_message = {
    #         "e": "outboundAccountInfo",
    #         "E": "1629969654753",
    #         "T": True,
    #         "W": True,
    #         "D": True,
    #         "B": [
    #             {
    #                 "a": self.base_asset,
    #                 "f": "10000",
    #                 "l": "500"
    #             }
    #         ]
    #     }

    #     mock_queue = AsyncMock()
    #     mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
    #     self.exchange._user_stream_tracker._user_stream = mock_queue

    #     try:
    #         self.async_run_with_timeout(self.exchange._user_stream_event_listener())
    #     except asyncio.CancelledError:
    #         pass

    #     self.assertEqual(Decimal("10000"), self.exchange.available_balances["COINALPHA"])
    #     self.assertEqual(Decimal("10500"), self.exchange.get_balance("COINALPHA"))

    def test_user_stream_raises_cancel_exception(self):
        self.exchange._set_current_timestamp(1640780000)

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError
        self.exchange._user_stream_tracker._user_stream = mock_queue

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout,
            self.exchange._user_stream_event_listener())

    # @patch("hummingbot.connector.exchange.bing_x.bing_x_exchange.BingXExchange._sleep")
    # def test_user_stream_logs_errors(self, _):
    #     self.exchange._set_current_timestamp(1640780000)

    #     incomplete_event = {
    #         "e": "outboundAccountInfo",
    #         "E": "1629969654753",
    #         "T": True,
    #         "W": True,
    #         "D": True,
    #     }

    #     mock_queue = AsyncMock()
    #     mock_queue.get.side_effect = [incomplete_event, asyncio.CancelledError]
    #     self.exchange._user_stream_tracker._user_stream = mock_queue

    #     try:
    #         self.async_run_with_timeout(self.exchange._user_stream_event_listener())
    #     except asyncio.CancelledError:
    #         pass

    #     self.assertTrue(
    #         self._is_logged(
    #             "ERROR",
    #             "Unexpected error in user stream listener loop."
    #         )
    #     )
