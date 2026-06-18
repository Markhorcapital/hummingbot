#!/usr/bin/env python3
"""
Cancel open Gate.io spot orders (standalone — no Hummingbot strategy required).

Uses the same REST paths as hummingbot/connector/exchange/gate_io/gate_io_exchange.py.

Setup:
  export GATE_API_KEY="your_key"
  export GATE_API_SECRET="your_secret"

Examples:
  # List open ALI-USDT orders (dry-run)
  python scripts/utility/cancel_gate_orders.py --pair ALI-USDT

  # Cancel ALL open orders for ALI-USDT
  python scripts/utility/cancel_gate_orders.py --pair ALI-USDT --cancel-all --confirm

  # Cancel one order by exchange order id
  python scripts/utility/cancel_gate_orders.py --pair ALI-USDT --order-id 123456789 --confirm

  # List / cancel open orders on every pair
  python scripts/utility/cancel_gate_orders.py --cancel-all --confirm
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests

BASE_URL = "https://api.gateio.ws/api/v4"
API_PREFIX = "/api/v4"
OPEN_ORDERS_PATH = f"{API_PREFIX}/spot/open_orders"
CANCEL_ALL_PATH = f"{API_PREFIX}/spot/orders"
CANCEL_ONE_PATH = f"{API_PREFIX}/spot/orders/{{order_id}}"

EMPTY_BODY_HASH = hashlib.sha512().hexdigest()


def hb_pair_to_gate(currency_pair: str) -> str:
    """ALI-USDT -> ALI_USDT"""
    return currency_pair.replace("-", "_").upper()


def gate_pair_to_hb(currency_pair: str) -> str:
    return currency_pair.replace("_", "-")


def build_query_string(params: Dict[str, Any]) -> str:
    if not params:
        return ""
    return "&".join(f"{k}={v}" for k, v in params.items())


def sign_request(method: str, path: str, query_string: str, secret: str) -> tuple[str, str]:
    ts = time.time()
    payload = f"{method.upper()}\n{path}\n{query_string}\n{EMPTY_BODY_HASH}\n{ts}"
    sign = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return sign, str(ts)


def gate_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    params = params or {}
    query_string = build_query_string(params)
    url = f"{BASE_URL}{path[len(API_PREFIX):]}"
    if query_string:
        url = f"{url}?{query_string}"

    sign, ts = sign_request(method, path, query_string, api_secret)
    headers = {
        "KEY": api_key,
        "SIGN": sign,
        "Timestamp": ts,
        "Content-Type": "application/json",
        "X-Gate-Channel-Id": "hummingbot",
    }

    resp = requests.request(method.upper(), url, headers=headers, timeout=30)
    resp.raise_for_status()
    if not resp.text:
        return {}
    data = resp.json()
    if isinstance(data, dict) and data.get("label"):
        raise RuntimeError(f"Gate.io error label={data.get('label')} message={data.get('message')}")
    return data


def flatten_open_orders(raw: Any) -> List[Dict[str, Any]]:
    """Gate returns [{currency_pair, total, orders: [...]}, ...]"""
    orders: List[Dict[str, Any]] = []
    if not isinstance(raw, list):
        return orders
    for group in raw:
        if not isinstance(group, dict):
            continue
        for order in group.get("orders") or []:
            if isinstance(order, dict):
                orders.append(order)
    return orders


def fetch_open_orders(api_key: str, api_secret: str, gate_pair: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if gate_pair:
        params["currency_pair"] = gate_pair
    raw = gate_request("GET", OPEN_ORDERS_PATH, api_key, api_secret, params)
    return flatten_open_orders(raw)


def cancel_all_for_pair(api_key: str, api_secret: str, gate_pair: str) -> List[Dict[str, Any]]:
    return gate_request("DELETE", CANCEL_ALL_PATH, api_key, api_secret, {"currency_pair": gate_pair})


def cancel_one(api_key: str, api_secret: str, gate_pair: str, order_id: str) -> Dict[str, Any]:
    path = CANCEL_ONE_PATH.format(order_id=order_id)
    return gate_request("DELETE", path, api_key, api_secret, {"currency_pair": gate_pair})


def print_orders(orders: List[Dict[str, Any]]) -> None:
    if not orders:
        print("No open orders.")
        return
    print(f"Found {len(orders)} open order(s):\n")
    for i, order in enumerate(orders, 1):
        pair = order.get("currency_pair", "?")
        print(
            f"  [{i}] pair={gate_pair_to_hb(pair)} side={order.get('side')} "
            f"price={order.get('price')} amount={order.get('amount')} left={order.get('left')} "
            f"id={order.get('id')} text={order.get('text')} status={order.get('status')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="List / cancel Gate.io spot open orders")
    parser.add_argument("--api-key", default=os.getenv("GATE_API_KEY"), help="Gate API key")
    parser.add_argument("--api-secret", default=os.getenv("GATE_API_SECRET"), help="Gate API secret")
    parser.add_argument("--pair", default="ALI-USDT", help="Hummingbot pair, e.g. ALI-USDT")
    parser.add_argument("--order-id", help="Exchange order id to cancel")
    parser.add_argument("--cancel-all", action="store_true", help="Cancel all open orders")
    parser.add_argument("--confirm", action="store_true", help="Required to actually cancel")
    args = parser.parse_args()

    if not args.api_key or not args.api_secret:
        print(
            "Error: set GATE_API_KEY and GATE_API_SECRET, or pass --api-key / --api-secret",
            file=sys.stderr,
        )
        return 1

    gate_pair = hb_pair_to_gate(args.pair) if args.pair else None

    try:
        if args.order_id:
            if not gate_pair:
                print("Error: --pair is required when cancelling a specific order", file=sys.stderr)
                return 1
            if not args.confirm:
                print(f"Dry run: would cancel order {args.order_id} on {args.pair}")
                print("Re-run with --confirm")
                return 0
            result = cancel_one(args.api_key, args.api_secret, gate_pair, args.order_id)
            print(f"Cancelled: {json.dumps(result, indent=2)}")
            return 0

        if args.cancel_all:
            if gate_pair:
                orders = fetch_open_orders(args.api_key, args.api_secret, gate_pair)
            else:
                orders = fetch_open_orders(args.api_key, args.api_secret, None)

            print_orders(orders)
            if not args.confirm:
                print("\nDry run. Re-run with --confirm to cancel.")
                return 0
            if not orders:
                return 0

            if gate_pair:
                print(f"\nCancelling ALL open orders for {args.pair} ({gate_pair})...")
                result = cancel_all_for_pair(args.api_key, args.api_secret, gate_pair)
                count = len(result) if isinstance(result, list) else "?"
                print(f"Done. Cancelled {count} order(s).")
            else:
                pairs = sorted({o.get("currency_pair") for o in orders if o.get("currency_pair")})
                for sym in pairs:
                    print(f"Cancelling all open orders for {gate_pair_to_hb(sym)}...")
                    cancel_all_for_pair(args.api_key, args.api_secret, sym)
                print("Done.")
            return 0

        orders = fetch_open_orders(args.api_key, args.api_secret, gate_pair)
        print_orders(orders)
        if orders:
            print(f"\nTo cancel all for {args.pair}:")
            print(f"  python {sys.argv[0]} --pair {args.pair} --cancel-all --confirm")
        return 0

    except requests.RequestException as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        if getattr(exc, "response", None) is not None:
            print(exc.response.text, file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"API error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
