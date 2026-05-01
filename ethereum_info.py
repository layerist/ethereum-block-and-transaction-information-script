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
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# High precision for ETH math
getcontext().prec = 50

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: Decimal = Decimal("1000000000000000000")

    TIMEOUT: int = 10
    MAX_RETRIES: int = 6
    BACKOFF_BASE: float = 1.5
    BACKOFF_MAX: float = 20.0
    JITTER: float = 0.3

    RATE_LIMIT_DELAY: float = 0.22  # ~5 req/sec safe

    PAGE_SIZE: int = 100
    MAX_WORKERS: int = 4

    CSV_FIELDS = (
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
    jitter = random.uniform(1 - Config.JITTER, 1 + Config.JITTER)
    time.sleep(min(delay * jitter, Config.BACKOFF_MAX))


def safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
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
# Rate limiter (thread-safe)
# -------------------------------------------------------------------
class RateLimiter:
    def __init__(self, delay: float):
        self.delay = delay
        self.lock = threading.Lock()
        self.last_call = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_call = time.time()


# -------------------------------------------------------------------
# Client
# -------------------------------------------------------------------
class EtherscanClient:
    def __init__(self, api_key: str):
        if not api_key or len(api_key) < 10:
            raise ValidationError("Invalid API key")

        self.api_key = api_key
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

        self.rate_limiter = RateLimiter(Config.RATE_LIMIT_DELAY)

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ---------------- Core request ----------------
    def _request(self, params: Dict[str, str]) -> Dict[str, Any]:
        delay = Config.BACKOFF_BASE

        for attempt in range(Config.MAX_RETRIES):
            try:
                self.rate_limiter.wait()

                r = self.session.get(
                    Config.BASE_URL,
                    params={**params, "apikey": self.api_key},
                    timeout=Config.TIMEOUT,
                )

                if r.status_code == 429:
                    raise RateLimitError("HTTP 429")

                if r.status_code >= 500:
                    raise APIError(f"Server error {r.status_code}")

                r.raise_for_status()
                data = r.json()

                if not isinstance(data, dict):
                    raise APIError("Invalid JSON")

                status = str(data.get("status"))
                message = str(data.get("message", "")).lower()

                if status == "0":
                    if "rate limit" in message:
                        raise RateLimitError(message)
                    if "no transactions" in message:
                        return {"result": []}
                    raise APIError(message)

                return data

            except RateLimitError:
                logging.warning("Rate limited (attempt %d)", attempt + 1)
            except (requests.RequestException, APIError) as e:
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

    # ---------------- Transactions ----------------
    def _fetch_page(self, address: str, page: int) -> List[Dict[str, Any]]:
        r = self._request({
            "module": "account",
            "action": "txlist",
            "address": address,
            "page": str(page),
            "offset": str(Config.PAGE_SIZE),
            "sort": "desc",
        })
        return r.get("result", [])

    def get_transactions(self, address: str, limit: int) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        stop_flag = threading.Event()

        def worker(page: int):
            if stop_flag.is_set():
                return None
            return self._fetch_page(address, page)

        page = 1
        futures = set()

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            # prime initial batch
            for _ in range(Config.MAX_WORKERS):
                futures.add(executor.submit(worker, page))
                page += 1

            while futures:
                for future in as_completed(futures):
                    futures.remove(future)

                    batch = future.result()
                    if not batch:
                        stop_flag.set()
                        continue

                    results.extend(batch)

                    if len(results) >= limit:
                        stop_flag.set()
                        break

                    if not stop_flag.is_set():
                        futures.add(executor.submit(worker, page))
                        page += 1

                if stop_flag.is_set():
                    break

        return results[:limit]


# -------------------------------------------------------------------
# CSV (optimized append)
# -------------------------------------------------------------------
def append_csv(path: Path, rows: Iterable[Dict[str, Any]]):
    file_exists = path.exists()
    existing_hashes: Set[str] = set()

    if file_exists:
        with path.open("r", encoding="utf-8") as f:
            existing_hashes = {r["hash"] for r in csv.DictReader(f)}

    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=Config.CSV_FIELDS)

        if not file_exists:
            writer.writeheader()

        new_count = 0

        for tx in rows:
            h = tx.get("hash")
            if not h or h in existing_hashes:
                continue

            val = (safe_decimal(tx["value"]) / Config.WEI_TO_ETH).quantize(
                Decimal("0.00000001")
            )

            writer.writerow({
                "hash": h,
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": iso_utc(tx.get("timeStamp")),
                "from": tx.get("from", ""),
                "to": tx.get("to", ""),
                "value_eth": str(val),
                "gas": tx.get("gas", ""),
                "gasPrice": tx.get("gasPrice", ""),
                "isError": tx.get("isError", "0"),
            })
            new_count += 1

    logging.info("CSV appended: +%d rows", new_count)


# -------------------------------------------------------------------
# Analytics
# -------------------------------------------------------------------
def calculate_totals(txs: List[Dict[str, Any]], address: str):
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
# Main logic
# -------------------------------------------------------------------
def run(address: str, key: str, count: int, csv_out: Optional[str]):

    address = validate_address(address)

    with EtherscanClient(key) as client:
        start = time.time()

        balance = client.get_balance(address)
        price = client.get_eth_price()
        txs = client.get_transactions(address, count)

        elapsed = time.time() - start

    recv, sent = calculate_totals(txs, address)

    logging.info("Balance: %.6f ETH (~$%.2f)", balance, balance * price)
    logging.info("Price  : $%s", price)
    logging.info("Txs    : %d", len(txs))
    logging.info("In     : %.6f ETH", recv)
    logging.info("Out    : %.6f ETH", sent)
    logging.info("Time   : %.2fs", elapsed)

    if csv_out:
        append_csv(Path(csv_out), txs)


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Etherscan advanced client")

    parser.add_argument("address")
    parser.add_argument("apikey")

    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--csv")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

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
