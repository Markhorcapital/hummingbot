import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.gemini import gemini_constants as CONSTANTS, gemini_web_utils as web_utils
from hummingbot.connector.exchange.gemini.gemini_auth import GeminiAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gemini.gemini_exchange import GeminiExchange


class GeminiAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Data source for Gemini user stream (order events and account updates).

    IMPORTANT: Gemini's private WebSocket is fundamentally different from Binance:
    - No "listen key" system
    - Authentication happens during WebSocket handshake via headers
    - Connection URL: wss://api.gemini.com/v1/order/events
    - Events: initial, accepted, rejected, booked, fill, cancelled, cancel_rejected
    """

    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: GeminiAuth,
                 trading_pairs: List[str],
                 connector: 'GeminiExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Initialize Gemini user stream data source.

        :param auth: Authentication handler
        :param trading_pairs: List of trading pairs to monitor
        :param connector: Parent exchange connector
        :param api_factory: Web assistants factory
        :param domain: Domain
        """
        super().__init__()
        self._auth: GeminiAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._trading_pairs = trading_pairs
        self._ws_assistant: Optional[WSAssistant] = None

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Create WebSocket assistant with authentication.

        CRITICAL: Gemini requires authentication during the WebSocket handshake.

        :return: Authenticated WebSocket assistant
        """
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Connect to Gemini's private WebSocket with authentication.

        Gemini authenticates the WebSocket connection via headers during handshake:
        - X-GEMINI-APIKEY: API key
        - X-GEMINI-NONCE: Current timestamp (seconds)
        - X-GEMINI-SIGNATURE: HMAC-SHA384 signature
        - X-GEMINI-PAYLOAD: Base64-encoded nonce

        :return: Connected and authenticated WebSocket assistant
        """
        import base64
        import hashlib
        import hmac
        import json

        self._ws_assistant = await self._api_factory.get_ws_assistant()
        ws = self._ws_assistant

        # Build WebSocket URL
        ws_url = web_utils.wss_private_url(self._domain)

        # Generate authentication headers directly
        # Create payload with request path and nonce
        nonce = int(self._auth.time_provider.time())
        payload = {
            "request": "/v1/order/events",
            "nonce": nonce
        }

        # JSON encode → UTF-8 encode → Base64 encode
        json_payload = json.dumps(payload)
        b64_payload = base64.b64encode(json_payload.encode('utf-8')).decode('utf-8')

        # Sign the base64 payload with HMAC-SHA384
        signature = hmac.new(
            self._auth.secret_key,
            b64_payload.encode('utf-8'),
            hashlib.sha384
        ).hexdigest()

        # Create authentication headers
        auth_headers = {
            CONSTANTS.HEADER_API_KEY: self._auth.api_key,
            CONSTANTS.HEADER_PAYLOAD: b64_payload,
            CONSTANTS.HEADER_SIGNATURE: signature
        }

        # Connect with authentication headers
        await ws.connect(
            ws_url=ws_url,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
            message_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
            ws_headers=auth_headers
        )

        return ws

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Listen to Gemini order events WebSocket and process messages.

        Gemini order event types:
        - initial: Initial subscription confirmation
        - accepted: Order accepted by exchange
        - rejected: Order rejected
        - booked: Order placed on the order book
        - fill: Order filled (partial or complete)
        - cancelled: Order cancelled
        - cancel_rejected: Cancellation rejected
        - closed: Order closed

        :param output: Queue to output parsed messages
        """
        ws = None
        while True:
            try:
                ws = await self._connected_websocket_assistant()

                # Wait for initial message
                async for ws_response in ws.iter_messages():
                    data = ws_response.data

                    if isinstance(data, dict):
                        event_type = data.get("type", "")

                        # Skip heartbeat and initial messages
                        if event_type == CONSTANTS.WS_HEARTBEAT_EVENT:
                            continue
                        elif event_type == CONSTANTS.WS_ORDER_INITIAL:
                            self.logger().info("Gemini order events WebSocket connected and authenticated")
                            continue

                        # Process order events
                        elif event_type in [
                            CONSTANTS.WS_ORDER_ACCEPTED,
                            CONSTANTS.WS_ORDER_REJECTED,
                            CONSTANTS.WS_ORDER_BOOKED,
                            CONSTANTS.WS_ORDER_FILL,
                            CONSTANTS.WS_ORDER_CANCELLED,
                            CONSTANTS.WS_ORDER_CANCEL_REJECTED,
                            CONSTANTS.WS_ORDER_CLOSED
                        ]:
                            output.put_nowait(data)
                        else:
                            self.logger().debug(f"Unknown message type from Gemini user stream: {event_type}")

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error while listening to user stream. Retrying after 5 seconds...",
                    exc_info=True
                )
                await self._sleep(5.0)
            finally:
                if ws is not None:
                    await ws.disconnect()

    async def _sleep(self, delay: float):
        """
        Sleep for specified duration.

        :param delay: Sleep duration in seconds
        """
        await asyncio.sleep(delay)
