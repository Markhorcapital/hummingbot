#!/usr/bin/env python3
"""
Phase 0 validation for PMM Dynamic DEX/CEX feed.

Checks:
  - Uniswap V3 pool token0/token1, decimals, slot0, observe()
  - Implied ALI/USDT from pool TWAP/spot × ETH/USDT (CEX or optional RPC)
  - MEXC public ticker for ALI-USDT and ETH-USDT (no API keys)

Usage:
  export DEX_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/KEY"   # or WEB3_PROVIDER
  python scripts/utility/phase0_dex_cex_validate.py

  python scripts/utility/phase0_dex_cex_validate.py --twap-seconds 180
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal
from typing import Any, Dict, Optional

import requests

# Known references (uniswap-v3-twap-oracle.html)
DEFAULT_POOL = "0xF260d15e8eBe54D210ef53F5b61Cb46bD9Aa29EE"
ALI_ADDRESS = "0x6B0b3a982b4634aC68dD83a4DBF02311cE324181"
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

POOL_ABI = [
    {
        "inputs": [{"name": "secondsAgos", "type": "uint32[]"}],
        "name": "observe",
        "outputs": [
            {"name": "tickCumulatives", "type": "int56[]"},
            {"name": "secondsPerLiquidityCumulativeX128s", "type": "uint160[]"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {"inputs": [], "name": "token0", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {"inputs": [], "name": "fee", "outputs": [{"type": "uint24"}], "stateMutability": "view", "type": "function"},
]

ERC20_ABI = [
    {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
]

MEXC_TICKER_URL = "https://api.mexc.com/api/v3/ticker/bookTicker"


def _norm_addr(addr: str) -> str:
    return addr.lower()


def tick_to_price_0_in_1(tick: float, decimals0: int, decimals1: int) -> Decimal:
    raw = Decimal(str(1.0001 ** tick))
    return raw * Decimal(10) ** (decimals0 - decimals1)


def price_0_in_1_from_sqrt(sqrt_price_x96: int, decimals0: int, decimals1: int) -> Decimal:
    sqrt = Decimal(sqrt_price_x96) / Decimal(2**96)
    ratio = sqrt * sqrt
    return ratio * Decimal(10) ** (decimals0 - decimals1)


def ali_in_weth_from_price_0_in_1(
    price_0_in_1: Decimal,
    token0_addr: str,
    token1_addr: str,
) -> Decimal:
    """WETH per 1 ALI (human units)."""
    ali0 = _norm_addr(token0_addr) == _norm_addr(ALI_ADDRESS)
    weth1 = _norm_addr(token1_addr) == _norm_addr(WETH_ADDRESS)
    if ali0 and weth1:
        return price_0_in_1
    weth0 = _norm_addr(token0_addr) == _norm_addr(WETH_ADDRESS)
    ali1 = _norm_addr(token1_addr) == _norm_addr(ALI_ADDRESS)
    if weth0 and ali1:
        return Decimal(1) / price_0_in_1
    raise ValueError(
        f"Pool tokens do not match ALI/WETH references: token0={token0_addr}, token1={token1_addr}"
    )


def fetch_mexc_mid(symbol: str) -> Decimal:
    r = requests.get(MEXC_TICKER_URL, params={"symbol": symbol.replace("-", "")}, timeout=15)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        row = data[0] if data else {}
    else:
        row = data
    bid = Decimal(str(row["bidPrice"]))
    ask = Decimal(str(row["askPrice"]))
    return (bid + ask) / 2


def get_web3(rpc_url: str):
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise ConnectionError(f"RPC not connected: {rpc_url[:48]}...")
    return w3


def read_pool(
    w3,
    pool_address: str,
    twap_seconds: int,
) -> Dict[str, Any]:
    pool = w3.eth.contract(address=w3.to_checksum_address(pool_address), abi=POOL_ABI)
    token0_addr = pool.functions.token0().call()
    token1_addr = pool.functions.token1().call()
    t0 = w3.eth.contract(address=token0_addr, abi=ERC20_ABI)
    t1 = w3.eth.contract(address=token1_addr, abi=ERC20_ABI)
    symbol0, dec0 = t0.functions.symbol().call(), t0.functions.decimals().call()
    symbol1, dec1 = t1.functions.symbol().call(), t1.functions.decimals().call()
    slot0 = pool.functions.slot0().call()
    fee = pool.functions.fee().call()
    sqrt_price_x96 = int(slot0[0])
    spot_tick = int(slot0[1])
    obs_index = int(slot0[2])
    obs_card = int(slot0[3])
    obs_card_next = int(slot0[4])

    spot_p01 = price_0_in_1_from_sqrt(sqrt_price_x96, dec0, dec1)
    spot_ali_weth = ali_in_weth_from_price_0_in_1(spot_p01, token0_addr, token1_addr)

    observe_ok = True
    observe_error: Optional[str] = None
    twap_ali_weth: Optional[Decimal] = None
    avg_tick: Optional[float] = None
    try:
        tick_cumulatives, _ = pool.functions.observe([twap_seconds, 0]).call()
        tick_delta = int(tick_cumulatives[1]) - int(tick_cumulatives[0])
        avg_tick = tick_delta / twap_seconds
        twap_p01 = tick_to_price_0_in_1(avg_tick, dec0, dec1)
        twap_ali_weth = ali_in_weth_from_price_0_in_1(twap_p01, token0_addr, token1_addr)
    except Exception as e:
        observe_ok = False
        observe_error = str(e)

    return {
        "pool_address": pool_address,
        "fee_tier_bps": int(fee) / 100,
        "token0": {"address": token0_addr, "symbol": symbol0, "decimals": dec0},
        "token1": {"address": token1_addr, "symbol": symbol1, "decimals": dec1},
        "observation_index": obs_index,
        "observation_cardinality": obs_card,
        "observation_cardinality_next": obs_card_next,
        "slot0_tick": spot_tick,
        "sqrt_price_x96": str(sqrt_price_x96),
        "spot_price_0_in_1": str(spot_p01),
        "spot_ali_in_weth": str(spot_ali_weth),
        "twap_seconds": twap_seconds,
        "observe_ok": observe_ok,
        "observe_error": observe_error,
        "avg_tick": avg_tick,
        "twap_ali_in_weth": str(twap_ali_weth) if twap_ali_weth is not None else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 0 DEX/CEX validation")
    parser.add_argument("--pool", default=DEFAULT_POOL)
    parser.add_argument("--twap-seconds", type=int, default=180)
    parser.add_argument("--rpc-url", default=os.getenv("DEX_RPC_URL") or os.getenv("WEB3_PROVIDER"))
    parser.add_argument("--connector", default="mexc")
    parser.add_argument("--ali-pair", default="ALI-USDT")
    parser.add_argument("--eth-pair", default="ETH-USDT")
    parser.add_argument("--json-out", default=None, help="Write full report JSON to path")
    args = parser.parse_args()

    report: Dict[str, Any] = {
        "phase": 0,
        "pool_address": args.pool,
        "twap_seconds": args.twap_seconds,
        "references": {"ALI": ALI_ADDRESS, "WETH": WETH_ADDRESS},
    }

    errors = []

    # CEX mids (public API)
    try:
        ali_mid = fetch_mexc_mid(args.ali_pair)
        eth_mid = fetch_mexc_mid(args.eth_pair)
        report["cex"] = {
            "connector": args.connector,
            "ali_usdt_mid": str(ali_mid),
            "eth_usdt_mid": str(eth_mid),
        }
    except Exception as e:
        errors.append(f"CEX ticker failed: {e}")
        report["cex"] = {"error": str(e)}

    # On-chain pool
    if not args.rpc_url:
        errors.append("DEX_RPC_URL or WEB3_PROVIDER not set — skipping on-chain checks")
        report["on_chain"] = {"skipped": True, "hint": "export DEX_RPC_URL=https://..."}
    else:
        try:
            w3 = get_web3(args.rpc_url)
            report["on_chain"] = read_pool(w3, args.pool, args.twap_seconds)
            oc = report["on_chain"]
            if report.get("cex") and "eth_usdt_mid" in report["cex"]:
                eth = Decimal(report["cex"]["eth_usdt_mid"])
                if oc.get("twap_ali_in_weth"):
                    twap_dex = Decimal(oc["twap_ali_in_weth"]) * eth
                    report["implied_ali_usdt_twap"] = str(twap_dex)
                if oc.get("spot_ali_in_weth"):
                    spot_dex = Decimal(oc["spot_ali_in_weth"]) * eth
                    report["implied_ali_usdt_spot"] = str(spot_dex)
                if "ali_usdt_mid" in report["cex"]:
                    cex_ali = Decimal(report["cex"]["ali_usdt_mid"])
                    if report.get("implied_ali_usdt_twap"):
                        twap_d = Decimal(report["implied_ali_usdt_twap"])
                        basis = (cex_ali - twap_d) / twap_d * 100
                        report["basis_pct_vs_twap_dex_fair"] = f"{basis:.4f}"
        except Exception as e:
            errors.append(f"On-chain failed: {e}")
            report["on_chain"] = {"error": str(e)}

    report["errors"] = errors
    report["phase0_pass"] = len(errors) == 0 and report.get("on_chain", {}).get("observe_ok", False)

    print(json.dumps(report, indent=2))

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nWrote {args.json_out}", file=sys.stderr)

    print("\n--- Phase 0 summary ---", file=sys.stderr)
    if report.get("on_chain") and not report["on_chain"].get("skipped"):
        oc = report["on_chain"]
        if oc.get("error"):
            print(f"on-chain error: {oc['error']}", file=sys.stderr)
        elif "token0" in oc:
            print(f"token0: {oc['token0']['symbol']} ({oc['token0']['address']})", file=sys.stderr)
            print(f"token1: {oc['token1']['symbol']} ({oc['token1']['address']})", file=sys.stderr)
            print(f"observe({args.twap_seconds}s): {'OK' if oc['observe_ok'] else 'FAIL'}", file=sys.stderr)
            if oc.get("twap_ali_in_weth"):
                print(f"TWAP WETH/ALI: {oc['twap_ali_in_weth']}", file=sys.stderr)
    if report.get("cex") and "ali_usdt_mid" in report.get("cex", {}):
        print(f"MEXC {args.ali_pair} mid: {report['cex']['ali_usdt_mid']}", file=sys.stderr)
        print(f"MEXC {args.eth_pair} mid: {report['cex']['eth_usdt_mid']}", file=sys.stderr)
    if report.get("implied_ali_usdt_twap"):
        print(f"Implied ALI/USDT (TWAP×ETH): {report['implied_ali_usdt_twap']}", file=sys.stderr)
    if report.get("basis_pct_vs_twap_dex_fair"):
        print(f"Basis vs CEX mid: {report['basis_pct_vs_twap_dex_fair']}%", file=sys.stderr)
    if errors:
        print("Errors:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)

    return 0 if report.get("phase0_pass") else 1


if __name__ == "__main__":
    sys.exit(main())
