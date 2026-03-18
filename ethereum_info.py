#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import logging
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

getcontext().prec = 50


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: Decimal = Decimal("1000000000000000000")

    TIMEOUT: int = 10
    MAX_RETRIES: int = 5
    BACKOFF_BASE: float = 1.2
    BACKOFF_MAX: float = 20.0
    JITTER: float = 0.2
    RATE_LIMIT_DELAY: float = 0.2

    PAGE_SIZE: int = 100
    MAX_PAGES: int = 10_000

    CSV_FIELDS: Tuple[str, ...] = (
        "hash",
        "blockNumber",
        "timeStamp",
        "from",
        "to",
        "value_eth",
        "gas",
        "gasPrice",
        "isError",
    )


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


# -------------------------------------------------------------------
# Errors
# -------------------------------------------------------------------
class APIError(RuntimeError): ...
class RateLimitError(APIError): ...
class ValidationError(ValueError): ...


# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------
def sleep_with_jitter(delay: float) -> None:
    time.sleep(min(delay * random.uniform(0.8, 1.2), Config.BACKOFF_MAX))


def safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(0)


def iso_utc(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""


def validate_address(address: str) -> str:
    if not ADDRESS_RE.fullmatch(address):
        raise ValidationError(f"Invalid address: {address}")
    return address.lower()


# -------------------------------------------------------------------
# Client
# -------------------------------------------------------------------
class EtherscanClient:
    def __init__(self, api_key: str):
        if not api_key or len(api_key) < 10:
            raise ValidationError("Invalid API key")

        self.api_key = api_key
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _request(self, params: Dict[str, str]) -> Dict[str, Any]:
        delay = Config.BACKOFF_BASE

        for attempt in range(Config.MAX_RETRIES):
            try:
                time.sleep(Config.RATE_LIMIT_DELAY)

                r = self.session.get(
                    Config.BASE_URL,
                    params={**params, "apikey": self.api_key},
                    timeout=Config.TIMEOUT,
                )

                if r.status_code == 429:
                    raise RateLimitError()

                r.raise_for_status()
                data = r.json()

                if not isinstance(data, dict):
                    raise APIError("Bad JSON")

                status = str(data.get("status"))
                message = str(data.get("message", "")).lower()

                # Etherscan quirks handling
                if status == "0":
                    if "rate limit" in message:
                        raise RateLimitError(message)
                    if "no transactions" in message:
                        return {"result": []}
                    if "ok" not in message:
                        raise APIError(message)

                return data

            except RateLimitError:
                logging.warning("Rate limited (%d)", attempt + 1)
            except Exception as e:
                logging.warning("Request error (%d): %s", attempt + 1, e)

            sleep_with_jitter(delay)
            delay *= 2

        raise APIError("Max retries exceeded")

    # ---------------- Cached endpoints ----------------
    @lru_cache(maxsize=32)
    def get_balance(self, address: str) -> Decimal:
        r = self._request({
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
        })
        return safe_decimal(r["result"]) / Config.WEI_TO_ETH

    @lru_cache(maxsize=4)
    def get_eth_price(self) -> Decimal:
        r = self._request({
            "module": "stats",
            "action": "ethprice",
        })
        return safe_decimal(r["result"]["ethusd"])

    def get_transactions(self, address: str, limit: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        for page in range(1, Config.MAX_PAGES + 1):
            if len(out) >= limit:
                break

            r = self._request({
                "module": "account",
                "action": "txlist",
                "address": address,
                "page": str(page),
                "offset": str(Config.PAGE_SIZE),
                "sort": "desc",
            })

            batch = r.get("result", [])
            if not batch:
                break

            out.extend(batch)

            if len(batch) < Config.PAGE_SIZE:
                break  # last page

        return out[:limit]


# -------------------------------------------------------------------
# CSV (TRUE atomic)
# -------------------------------------------------------------------
def atomic_write_csv(
    path: Path,
    rows: Iterable[Dict[str, Any]],
):
    existing: Set[str] = set()

    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            existing = {r["hash"] for r in csv.DictReader(f)}

    new_rows = [r for r in rows if r.get("hash") not in existing]

    if not new_rows:
        logging.info("No new rows")
        return

    all_rows = []

    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            all_rows.extend(list(csv.DictReader(f)))

    for tx in new_rows:
        val = (safe_decimal(tx["value"]) / Config.WEI_TO_ETH).quantize(
            Decimal("0.00000001")
        )

        all_rows.append({
            "hash": tx.get("hash", ""),
            "blockNumber": tx.get("blockNumber", ""),
            "timeStamp": iso_utc(tx.get("timeStamp")),
            "from": tx.get("from", ""),
            "to": tx.get("to", ""),
            "value_eth": str(val),
            "gas": tx.get("gas", ""),
            "gasPrice": tx.get("gasPrice", ""),
            "isError": tx.get("isError", "0"),
        })

    tmp = path.with_suffix(".tmp")

    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=Config.CSV_FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)

    os.replace(tmp, path)

    logging.info("CSV updated: +%d rows", len(new_rows))


# -------------------------------------------------------------------
# Analytics
# -------------------------------------------------------------------
def calculate_totals(txs, address):
    recv = Decimal(0)
    sent = Decimal(0)

    for tx in txs:
        if tx.get("isError") == "1":
            continue

        val = safe_decimal(tx["value"]) / Config.WEI_TO_ETH

        if tx.get("to", "").lower() == address:
            recv += val
        elif tx.get("from", "").lower() == address:
            sent += val

    return recv, sent


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def run(address, key, count, csv_out):
    address = validate_address(address)

    with EtherscanClient(key) as c:
        bal = c.get_balance(address)
        price = c.get_eth_price()
        txs = c.get_transactions(address, count)

    recv, sent = calculate_totals(txs, address)

    logging.info(f"Balance: {bal} ETH")
    logging.info(f"Price  : ${price}")
    logging.info(f"Txs    : {len(txs)}")
    logging.info(f"In     : {recv}")
    logging.info(f"Out    : {sent}")

    if csv_out:
        atomic_write_csv(Path(csv_out), txs)


# -------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("address")
    p.add_argument("apikey")
    p.add_argument("--count", type=int, default=50)
    p.add_argument("--csv")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    signal.signal(signal.SIGINT, lambda *_: sys.exit(1))

    try:
        run(args.address, args.apikey, args.count, args.csv)
    except Exception:
        logging.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
