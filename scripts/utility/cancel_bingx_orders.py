#!/usr/bin/env python3
"""
Cancel open BingX spot limit orders (standalone — no Hummingbot strategy required).

Uses the same REST paths as hummingbot/connector/exchange/bing_x/bing_x_exchange.py.

Setup:
  export BINGX_API_KEY="your_key"
  export BINGX_API_SECRET="your_secret"

Examples:
  # List open ALI-USDT orders (dry-run)
  python scripts/utility/cancel_bingx_orders.py --symbol ALI-USDT

  # Cancel ALL open orders for ALI-USDT (fastest for orphan cleanup)
  python scripts/utility/cancel_bingx_orders.py --symbol ALI-USDT --cancel-all --confirm

  # Cancel one order by exchange order id
  python scripts/utility/cancel_bingx_orders.py --symbol ALI-USDT --order-id 2062086248876474368 --confirm

  # Cancel all open orders on the account (every symbol)
  python scripts/utility/cancel_bingx_orders.py --cancel-all --confirm
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import sys
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

BASE_URL = "https://open-api.bingx.com"
OPEN_ORDERS_PATH = "/openApi/spot/v1/trade/openOrders"
CANCEL_ORDER_PATH = "/openApi/spot/v1/trade/cancel"
CANCEL_OPEN_ORDERS_PATH = "/openApi/spot/v1/trade/cancelOpenOrders"


def sign_params(params: Dict[str, Any], secret: str) -> Dict[str, Any]:
    payload = OrderedDict(sorted({**params, "timestamp": str(int(time.time() * 1000))}.items()))
    query = urlencode(payload)
    payload["signature"] = hmac.new(
        secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return payload


def bingx_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = sign_params(params or {}, api_secret)
    headers = {
        "X-BX-APIKEY": api_key,
        "X-SOURCE-KEY": "Hummingbot",
    }
    url = f"{BASE_URL}{path}"
    if method.upper() == "GET":
        resp = requests.get(url, params=params, headers=headers, timeout=30)
    else:
        resp = requests.post(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response: {data!r}")
    if data.get("code") != 0:
        raise RuntimeError(
            f"BingX error code={data.get('code')} msg={data.get('msg')} debug={data.get('debugMsg')}"
        )
    return data


def normalize_orders(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = data.get("data")
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("orders", "orderList", "openOrders"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def fetch_open_orders(api_key: str, api_secret: str, symbol: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if symbol:
        params["symbol"] = symbol
    data = bingx_request("GET", OPEN_ORDERS_PATH, api_key, api_secret, params)
    return normalize_orders(data)


def cancel_one(
    api_key: str,
    api_secret: str,
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {"symbol": symbol}
    if order_id:
        params["orderId"] = order_id
    elif client_order_id:
        params["clientOrderId"] = client_order_id
    else:
        raise ValueError("Need --order-id or --client-order-id")
    return bingx_request("POST", CANCEL_ORDER_PATH, api_key, api_secret, params)


def cancel_all_for_symbol(api_key: str, api_secret: str, symbol: str) -> Dict[str, Any]:
    return bingx_request("POST", CANCEL_OPEN_ORDERS_PATH, api_key, api_secret, {"symbol": symbol})


def print_orders(orders: List[Dict[str, Any]]) -> None:
    if not orders:
        print("No open orders.")
        return
    print(f"Found {len(orders)} open order(s):\n")
    for i, order in enumerate(orders, 1):
        print(
            f"  [{i}] symbol={order.get('symbol')} side={order.get('side')} "
            f"price={order.get('price')} qty={order.get('origQty') or order.get('quantity')} "
            f"orderId={order.get('orderId')} clientOrderId={order.get('clientOrderId')} "
            f"status={order.get('status')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="List / cancel BingX spot open orders")
    parser.add_argument(
        "--api-key",
        default=os.getenv("BINGX_API_KEY"),
        help="BingX API key (or env BINGX_API_KEY)",
    )
    parser.add_argument(
        "--api-secret",
        default=os.getenv("BINGX_API_SECRET"),
        help="BingX API secret (or env BINGX_API_SECRET)",
    )
    parser.add_argument(
        "--symbol",
        default="ALI-USDT",
        help="Trading pair, e.g. ALI-USDT. Omit when using --cancel-all for entire account.",
    )
    parser.add_argument("--order-id", help="Exchange order id to cancel")
    parser.add_argument("--client-order-id", help="Client order id to cancel (e.g. BING_X_...)")
    parser.add_argument(
        "--cancel-all",
        action="store_true",
        help="Cancel all open orders (for --symbol, or entire account if no symbol)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required to actually cancel (otherwise list only)",
    )
    args = parser.parse_args()

    if not args.api_key or not args.api_secret:
        print(
            "Error: set BINGX_API_KEY and BINGX_API_SECRET, or pass --api-key / --api-secret",
            file=sys.stderr,
        )
        return 1

    symbol = args.symbol.strip() if args.symbol else None

    try:
        if args.cancel_all and not args.order_id and not args.client_order_id:
            orders = fetch_open_orders(args.api_key, args.api_secret, symbol)
            print_orders(orders)
            if not args.confirm:
                print("\nDry run. Re-run with --confirm to cancel.")
                return 0
            if symbol:
                print(f"\nCancelling ALL open orders for {symbol}...")
                result = cancel_all_for_symbol(args.api_key, args.api_secret, symbol)
                print(f"Done: {result.get('msg') or 'ok'}")
            else:
                if not orders:
                    return 0
                symbols = sorted({o.get("symbol") for o in orders if o.get("symbol")})
                for sym in symbols:
                    print(f"Cancelling all open orders for {sym}...")
                    cancel_all_for_symbol(args.api_key, args.api_secret, sym)
                print("Done.")
            return 0

        if args.order_id or args.client_order_id:
            if not symbol:
                print("Error: --symbol is required when cancelling a specific order", file=sys.stderr)
                return 1
            if not args.confirm:
                print(f"Dry run: would cancel order on {symbol}")
                print(f"  orderId={args.order_id} clientOrderId={args.client_order_id}")
                print("Re-run with --confirm")
                return 0
            result = cancel_one(
                args.api_key,
                args.api_secret,
                symbol,
                order_id=args.order_id,
                client_order_id=args.client_order_id,
            )
            print(f"Cancelled: {result}")
            return 0

        orders = fetch_open_orders(args.api_key, args.api_secret, symbol)
        print_orders(orders)
        if orders:
            print("\nTo cancel all for this symbol:")
            print(f"  python {sys.argv[0]} --symbol {symbol or 'ALI-USDT'} --cancel-all --confirm")
        return 0

    except requests.RequestException as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"API error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
