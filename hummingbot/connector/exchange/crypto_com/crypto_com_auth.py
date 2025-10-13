import hashlib
import hmac
import json
import threading
from typing import Any, Dict

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CryptoComAuth(AuthBase):
    """
    Authentication class for Crypto.com exchange
    Implements HMAC-SHA256 signing as required by Crypto.com API
    """

    # Class-level nonce counter to ensure uniqueness across instances
    _global_nonce_counter = 0
    _nonce_lock = threading.Lock()

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    def _params_to_string(self, params: Dict[str, Any], level: int = 0) -> str:
        """Convert parameters to string format for signature generation."""
        if level >= 3:  # Max recursion depth
            return str(params)

        result = ""
        for key in sorted(params.keys()):
            result += key
            value = params[key]

            if value is None:
                result += 'null'
            elif isinstance(value, list):
                for item in value:
                    result += self._params_to_string(item, level + 1)
            else:
                result += str(value)

        return result

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication headers and signature to the request
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)

        # Generate request ID and nonce using milliseconds (matching working script)
        import time as time_module
        request_id = int(time_module.time() * 1000) % 1000000
        nonce = int(time_module.time() * 1000)

        # Create the request data in Crypto.com JSON-RPC format
        if request.data:
            if isinstance(request.data, str):
                request_data = json.loads(request.data)
            else:
                request_data = request.data.copy()
        else:
            request_data = {}

        # Extract method and params from request data
        method = request_data.get("method", "")
        params = request_data.get("params", {})

        # Generate signature using the working script's method
        signature = self._generate_signature(method, request_id, params, nonce)

        # Structure the payload exactly like the working script
        payload = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }

        # Update headers
        headers.update({
            "Content-Type": "application/json",
        })

        request.headers = headers
        request.data = json.dumps(payload)

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Authenticate WebSocket request for Crypto.com
        """
        # Generate request ID and nonce using milliseconds (matching working script)
        import time as time_module
        request_id = int(time_module.time() * 1000) % 1000000
        nonce = int(time_module.time() * 1000)

        method = "public/auth"
        params = {}  # Empty params for auth

        # Generate signature using the working script's method
        signature = self._generate_signature(method, request_id, params, nonce)

        # Structure the payload exactly like the working script
        request.payload = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }

        return request

    def _generate_signature(self, method: str, request_id: int,
                            params: Dict[str, Any], nonce: int) -> str:
        """Generate HMAC-SHA256 signature for API authentication."""
        param_str = self._params_to_string(params) if params else ""
        payload = f"{method}{request_id}{self.api_key}{param_str}{nonce}"

        return hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def get_auth_headers(self, method: str = "POST", params: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Generate authentication headers for a request
        :param method: HTTP method
        :param params: Request parameters
        :return: Dictionary of authentication headers
        """
        if params is None:
            params = {}

        # Generate request ID and nonce using milliseconds (matching working script)
        import time as time_module
        request_id = int(time_module.time() * 1000) % 1000000
        nonce = int(time_module.time() * 1000)

        # Generate signature using the working script's method
        signature = self._generate_signature("private/dummy", request_id, params, nonce)

        return {
            "Content-Type": "application/json",
            "X-CDC-NONCE": str(nonce),
            "X-CDC-SIGNATURE": signature,
            "X-CDC-API-KEY": self.api_key,
        }
