import base64
import hashlib
import hmac
import json
from typing import Any

from hummingbot.connector.exchange.gemini import gemini_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class GeminiAuth(AuthBase):
    """
    Authentication class for Gemini Exchange API.

    Gemini uses HMAC-SHA384 authentication with a unique approach:
    1. Create a payload dict with the request path and nonce
    2. JSON encode → UTF-8 encode → Base64 encode the payload
    3. Sign the base64-encoded payload with HMAC-SHA384
    4. Include signature and payload in custom headers

    This is significantly different from most exchanges that sign query parameters.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        """
        Initialize Gemini authentication.

        :param api_key: Gemini API key
        :param secret_key: Gemini API secret
        :param time_provider: Time synchronizer for generating nonces
        """
        self.api_key = api_key
        self.secret_key = secret_key.encode('utf-8')
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication headers to REST API requests.

        CRITICAL: Gemini uses POST for ALL private endpoints (even data retrieval).
        The authentication payload includes the request path itself, along with a nonce
        and any additional parameters.

        :param request: the request to be configured for authenticated interaction
        :return: authenticated request
        """
        # Extract the endpoint path from the full URL
        # e.g., "https://api.gemini.com/v1/balances" → "/v1/balances"
        endpoint = self._extract_endpoint_from_url(request.url)

        # Generate payload and signature
        b64_payload, signature = self._generate_signature(endpoint, request.data)

        # Set authentication headers
        headers = {
            "Content-Type": CONSTANTS.CONTENT_TYPE_TEXT,
            "Content-Length": "0",
            CONSTANTS.HEADER_API_KEY: self.api_key,
            CONSTANTS.HEADER_PAYLOAD: b64_payload,
            CONSTANTS.HEADER_SIGNATURE: signature,
            "Cache-Control": "no-cache"
        }

        # Merge with existing headers if any
        if request.headers is not None:
            headers.update(request.headers)

        request.headers = headers

        # Gemini expects an empty body for POST requests
        # (all data is in the signed payload)
        request.data = ""

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Authenticate WebSocket connection.

        Gemini's private WebSocket (order events) requires authentication
        during the handshake via custom headers.

        The authentication process is:
        1. Create JSON payload: {"request": "/v1/order/events", "nonce": timestamp}
        2. Base64 encode the JSON payload
        3. Sign the base64 payload with HMAC-SHA384
        4. Send headers: X-GEMINI-APIKEY, X-GEMINI-PAYLOAD, X-GEMINI-SIGNATURE

        :param request: the WebSocket request to be configured
        :return: authenticated WebSocket request
        """
        # Create payload with request path and nonce (milliseconds timestamp)
        nonce = int(self.time_provider.time())
        payload = {
            "request": "/v1/order/events",
            "nonce": nonce
        }

        # JSON encode → UTF-8 encode → Base64 encode
        json_payload = json.dumps(payload)
        b64_payload = base64.b64encode(json_payload.encode('utf-8')).decode('utf-8')

        # Sign the base64 payload with HMAC-SHA384
        signature = hmac.new(
            self.secret_key,
            b64_payload.encode('utf-8'),
            hashlib.sha384
        ).hexdigest()

        # Add authentication headers for WebSocket handshake
        headers = {
            CONSTANTS.HEADER_API_KEY: self.api_key,
            CONSTANTS.HEADER_PAYLOAD: b64_payload,
            CONSTANTS.HEADER_SIGNATURE: signature
        }

        if request.headers is not None:
            headers.update(request.headers)

        request.headers = headers

        return request

    def _generate_signature(self, request_path: str, params: Any = None) -> tuple:
        """
        Generate Gemini API signature.

        This follows the exact process from the user's working script:
        1. Create payload dict with request path and nonce
        2. Add any additional parameters
        3. JSON encode → UTF-8 encode → Base64 encode
        4. Sign the base64 string with HMAC-SHA384

        :param request_path: Full API endpoint path (e.g., "/v1/balances")
        :param params: Optional additional parameters (dict, str, or None)
        :return: Tuple of (base64_payload_string, signature_hex_string)
        """
        # Create base payload with request path and nonce
        # IMPORTANT: nonce is a float timestamp in seconds (not milliseconds!)
        payload = {
            "request": request_path,
            "nonce": self.time_provider.time()
        }

        # Add any additional parameters
        if params:
            if isinstance(params, dict):
                payload.update(params)
            elif isinstance(params, str) and params:
                # If params is a JSON string, parse it
                try:
                    param_dict = json.loads(params)
                    if isinstance(param_dict, dict):
                        payload.update(param_dict)
                except (json.JSONDecodeError, ValueError):
                    pass

        # Step 1: JSON encode the payload
        json_payload = json.dumps(payload)

        # Step 2: UTF-8 encode
        utf8_payload = json_payload.encode('utf-8')

        # Step 3: Base64 encode
        b64_payload = base64.b64encode(utf8_payload).decode('utf-8')

        # Step 4: Generate HMAC-SHA384 signature of the base64 string
        signature = hmac.new(
            self.secret_key,
            b64_payload.encode('utf-8'),
            hashlib.sha384
        ).hexdigest()

        return b64_payload, signature

    def _extract_endpoint_from_url(self, url: str) -> str:
        """
        Extract the endpoint path from a full URL.

        Examples:
        - "https://api.gemini.com/v1/balances" → "/v1/balances"
        - "/v1/balances" → "/v1/balances"

        :param url: Full URL or path
        :return: Endpoint path starting with "/"
        """
        if url.startswith("http"):
            # Extract path from full URL
            # Find the position after the domain
            parts = url.split("/", 3)
            if len(parts) >= 4:
                return "/" + parts[3]
            return "/"
        else:
            # Already a path
            return url if url.startswith("/") else "/" + url
