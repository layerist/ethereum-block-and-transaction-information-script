#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import logging
import os
import random
import signal
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, getcontext
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re

# ============================================================
# Precision
# ============================================================

getcontext().prec = 50

# ============================================================
# Config
# ============================================================

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"

    WEI_TO_ETH: Decimal = Decimal("1000000000000000000")

    # Network
    TIMEOUT_CONNECT: int = 5
    TIMEOUT_READ: int = 20

    # Retry
    MAX_RETRIES: int = 10
    BACKOFF_BASE: float = 1.5
    BACKOFF_MAX: float = 30.0
    JITTER: float = 0.25

    # Etherscan free tier safe rate
    REQUESTS_PER_SECOND: float = 4.8

    # Transactions
    PAGE_SIZE: int = 100
    MAX_WORKERS: int = min(12, (os.cpu_count() or 4) * 2)
    MAX_EMPTY_PAGES: int = 2

    # HTTP Pool
    HTTP_POOL_CONNECTIONS: int = 32
    HTTP_POOL_MAXSIZE: int = 32

    # CSV
    CSV_BUFFER_SIZE: int = 1024 * 1024

    CSV_FIELDS = (
        "hash",
        "blockNumber",
        "timeStamp",
        "from",
        "to",
        "value_eth",
        "gas",
        "gasPrice",
        "gasUsed",
        "txreceipt_status",
        "isError",
        "confirmations",
    )


# ============================================================
# Regex
# ============================================================

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


# ============================================================
# Exceptions
# ============================================================

class APIError(RuntimeError):
    pass


class RateLimitError(APIError):
    pass


class ValidationError(ValueError):
    pass


# ============================================================
# Utility
# ============================================================

def safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def wei_to_eth(value: Any) -> Decimal:
    return safe_decimal(value) / Config.WEI_TO_ETH


def iso_utc(timestamp: Any) -> str:
    try:
        return datetime.fromtimestamp(
            int(timestamp),
            tz=timezone.utc
        ).isoformat()
    except Exception:
        return ""


def validate_address(address: str) -> str:
    address = address.strip()

    if not ADDRESS_RE.fullmatch(address):
        raise ValidationError(f"Invalid Ethereum address: {address}")

    return address.lower()


def quantize_eth(value: Decimal) -> Decimal:
    return value.quantize(
        Decimal("0.00000001"),
        rounding=ROUND_DOWN
    )


def exponential_backoff(attempt: int) -> float:
    delay = min(
        Config.BACKOFF_BASE * (2 ** attempt),
        Config.BACKOFF_MAX,
    )

    jitter = random.uniform(
        1.0 - Config.JITTER,
        1.0 + Config.JITTER,
    )

    return delay * jitter


# ============================================================
# Thread-safe token bucket limiter
# ============================================================

class RateLimiter:
    """
    Accurate token bucket rate limiter.
    """

    def __init__(self, rate_per_sec: float):
        self.capacity = rate_per_sec
        self.tokens = rate_per_sec
        self.rate = rate_per_sec

        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated

                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.rate
                )

                self.updated = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

            time.sleep(0.005)


# ============================================================
# Etherscan Client
# ============================================================

class EtherscanClient:
    def __init__(self, api_key: str):
        api_key = api_key.strip()

        if len(api_key) < 10:
            raise ValidationError("Invalid API key")

        self.api_key = api_key
        self.rate_limiter = RateLimiter(Config.REQUESTS_PER_SECOND)

        self.session = requests.Session()

        retry = Retry(
            total=0,
            connect=0,
            read=0,
            redirect=0,
            backoff_factor=0,
        )

        adapter = HTTPAdapter(
            pool_connections=Config.HTTP_POOL_CONNECTIONS,
            pool_maxsize=Config.HTTP_POOL_MAXSIZE,
            max_retries=retry,
        )

        self.session.mount("https://", adapter)

        self.session.headers.update({
            "User-Agent": "Advanced-Etherscan-Client/3.0",
            "Accept": "application/json",
            "Connection": "keep-alive",
        })

    # --------------------------------------------------------

    def close(self) -> None:
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # --------------------------------------------------------
    # Core request
    # --------------------------------------------------------

    def _request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        final_params = dict(params)
        final_params["apikey"] = self.api_key

        last_error: Optional[Exception] = None

        for attempt in range(Config.MAX_RETRIES):
            try:
                self.rate_limiter.acquire()

                response = self.session.get(
                    Config.BASE_URL,
                    params=final_params,
                    timeout=(
                        Config.TIMEOUT_CONNECT,
                        Config.TIMEOUT_READ,
                    ),
                )

                status_code = response.status_code

                if status_code == 429:
                    raise RateLimitError("HTTP 429")

                if status_code >= 500:
                    raise APIError(f"HTTP {status_code}")

                response.raise_for_status()

                data = response.json()

                if not isinstance(data, dict):
                    raise APIError("Invalid JSON response")

                status = str(data.get("status", ""))
                message = str(data.get("message", "")).lower()
                result = data.get("result")

                # Etherscan weird behavior handling
                if status == "0":
                    if isinstance(result, str):
                        result_lower = result.lower()

                        if "rate limit" in result_lower:
                            raise RateLimitError(result)

                        if "no transactions found" in result_lower:
                            return {"result": []}

                    if "rate limit" in message:
                        raise RateLimitError(message)

                    if result in ("No transactions found", []):
                        return {"result": []}

                    raise APIError(
                        f"Etherscan API error: {result}"
                    )

                return data

            except (
                requests.RequestException,
                APIError,
                ValueError,
            ) as e:

                last_error = e

                logging.warning(
                    "Request failed (attempt %d/%d): %s",
                    attempt + 1,
                    Config.MAX_RETRIES,
                    e,
                )

                if attempt + 1 >= Config.MAX_RETRIES:
                    break

                time.sleep(exponential_backoff(attempt))

        raise APIError(
            f"Max retries exceeded: {last_error}"
        )

    # --------------------------------------------------------
    # Cached methods
    # --------------------------------------------------------

    @lru_cache(maxsize=128)
    def get_balance(self, address: str) -> Decimal:
        data = self._request({
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
        })

        return wei_to_eth(data["result"])

    @lru_cache(maxsize=8)
    def get_eth_price(self) -> Decimal:
        data = self._request({
            "module": "stats",
            "action": "ethprice",
        })

        result = data.get("result", {})
        return safe_decimal(result.get("ethusd"))


    @lru_cache(maxsize=1)
    def get_gas_oracle(self) -> Dict[str, Any]:
        try:
            data = self._request({
                "module": "gastracker",
                "action": "gasoracle",
            })
            return data.get("result", {})
        except Exception:
            return {}

    # --------------------------------------------------------
    # Transactions
    # --------------------------------------------------------

    def _fetch_page(
        self,
        address: str,
        page: int,
    ) -> List[Dict[str, Any]]:

        data = self._request({
            "module": "account",
            "action": "txlist",
            "address": address,
            "page": page,
            "offset": Config.PAGE_SIZE,
            "sort": "desc",
        })

        result = data.get("result", [])

        if not isinstance(result, list):
            return []

        return result

    def get_transactions(
        self,
        address: str,
        limit: int,
    ) -> List[Dict[str, Any]]:

        if limit <= 0:
            return []

        results: List[Dict[str, Any]] = []
        seen_hashes: Set[str] = set()

        next_page = 1
        empty_pages = 0

        with ThreadPoolExecutor(
            max_workers=Config.MAX_WORKERS
        ) as executor:

            futures = {}

            # Initial batch
            for _ in range(Config.MAX_WORKERS):
                future = executor.submit(
                    self._fetch_page,
                    address,
                    next_page,
                )

                futures[future] = next_page
                next_page += 1

            while futures and len(results) < limit:
                done, _ = wait(
                    futures,
                    return_when=FIRST_COMPLETED,
                )

                for future in done:
                    page = futures.pop(future)

                    try:
                        batch = future.result()

                    except Exception as e:
                        logging.error(
                            "Page %d failed: %s",
                            page,
                            e,
                        )
                        continue

                    if not batch:
                        empty_pages += 1

                        if (
                            empty_pages >=
                            Config.MAX_EMPTY_PAGES
                        ):
                            futures.clear()
                            break

                        continue

                    empty_pages = 0

                    for tx in batch:
                        tx_hash = tx.get("hash")

                        if (
                            not tx_hash or
                            tx_hash in seen_hashes
                        ):
                            continue

                        seen_hashes.add(tx_hash)
                        results.append(tx)

                        if len(results) >= limit:
                            break

                    if len(results) >= limit:
                        futures.clear()
                        break

                    future = executor.submit(
                        self._fetch_page,
                        address,
                        next_page,
                    )

                    futures[future] = next_page
                    next_page += 1

        return results[:limit]


# ============================================================
# CSV
# ============================================================

def load_existing_hashes(path: Path) -> Set[str]:
    hashes: Set[str] = set()

    if not path.exists():
        return hashes

    try:
        with path.open(
            "r",
            encoding="utf-8",
            newline="",
            buffering=Config.CSV_BUFFER_SIZE,
        ) as f:

            reader = csv.DictReader(f)

            for row in reader:
                h = row.get("hash")
                if h:
                    hashes.add(h)

    except Exception as e:
        logging.warning(
            "Failed loading existing CSV hashes: %s",
            e,
        )

    return hashes


def append_csv(
    path: Path,
    rows: Iterable[Dict[str, Any]],
) -> int:

    file_exists = path.exists()

    existing_hashes = load_existing_hashes(path)

    written = 0

    with path.open(
        "a",
        encoding="utf-8",
        newline="",
        buffering=Config.CSV_BUFFER_SIZE,
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=Config.CSV_FIELDS,
        )

        if not file_exists:
            writer.writeheader()

        for tx in rows:
            tx_hash = tx.get("hash")

            if (
                not tx_hash or
                tx_hash in existing_hashes
            ):
                continue

            existing_hashes.add(tx_hash)

            writer.writerow({
                "hash": tx_hash,
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": iso_utc(
                    tx.get("timeStamp")
                ),
                "from": tx.get("from", ""),
                "to": tx.get("to", ""),
                "value_eth": str(
                    quantize_eth(
                        wei_to_eth(tx.get("value"))
                    )
                ),
                "gas": tx.get("gas", ""),
                "gasPrice": tx.get("gasPrice", ""),
                "gasUsed": tx.get("gasUsed", ""),
                "txreceipt_status": tx.get(
                    "txreceipt_status",
                    "",
                ),
                "isError": tx.get("isError", ""),
                "confirmations": tx.get(
                    "confirmations",
                    "",
                ),
            })

            written += 1

    return written


# ============================================================
# Analytics
# ============================================================

@dataclass
class TxStats:
    total_received: Decimal
    total_sent: Decimal
    total_fees: Decimal
    successful: int
    failed: int


def calculate_stats(
    txs: List[Dict[str, Any]],
    address: str,
) -> TxStats:

    address = address.lower()

    received = Decimal(0)
    sent = Decimal(0)
    fees = Decimal(0)

    successful = 0
    failed = 0

    for tx in txs:
        is_error = str(tx.get("isError", "0")) == "1"

        if is_error:
            failed += 1
        else:
            successful += 1

        value_eth = wei_to_eth(tx.get("value"))

        tx_from = str(
            tx.get("from", "")
        ).lower()

        tx_to = str(
            tx.get("to", "")
        ).lower()

        if tx_to == address:
            received += value_eth

        if tx_from == address:
            sent += value_eth

            gas_used = safe_decimal(
                tx.get("gasUsed")
            )

            gas_price = safe_decimal(
                tx.get("gasPrice")
            )

            fee_wei = gas_used * gas_price
            fees += wei_to_eth(fee_wei)

    return TxStats(
        total_received=received,
        total_sent=sent,
        total_fees=fees,
        successful=successful,
        failed=failed,
    )


# ============================================================
# Display
# ============================================================

def print_summary(
    balance: Decimal,
    price: Decimal,
    txs: List[Dict[str, Any]],
    stats: TxStats,
    elapsed: float,
) -> None:

    usd_value = balance * price

    logging.info("-" * 60)

    logging.info(
        "Balance          : %.8f ETH (~$%.2f)",
        balance,
        usd_value,
    )

    logging.info(
        "ETH Price        : $%s",
        price,
    )

    logging.info(
        "Transactions     : %d",
        len(txs),
    )

    logging.info(
        "Successful TXs   : %d",
        stats.successful,
    )

    logging.info(
        "Failed TXs       : %d",
        stats.failed,
    )

    logging.info(
        "Total Received   : %.8f ETH",
        stats.total_received,
    )

    logging.info(
        "Total Sent       : %.8f ETH",
        stats.total_sent,
    )

    logging.info(
        "Estimated Fees   : %.8f ETH",
        stats.total_fees,
    )

    logging.info(
        "Execution Time   : %.2fs",
        elapsed,
    )

    logging.info("-" * 60)


# ============================================================
# Main Logic
# ============================================================

def run(
    address: str,
    api_key: str,
    count: int,
    csv_output: Optional[str],
) -> None:

    address = validate_address(address)

    started = time.perf_counter()

    with EtherscanClient(api_key) as client:

        balance = client.get_balance(address)

        try:
            eth_price = client.get_eth_price()
        except Exception:
            eth_price = Decimal(0)

        txs = client.get_transactions(
            address=address,
            limit=count,
        )

    elapsed = time.perf_counter() - started

    stats = calculate_stats(txs, address)

    print_summary(
        balance=balance,
        price=eth_price,
        txs=txs,
        stats=stats,
        elapsed=elapsed,
    )

    if csv_output:
        csv_path = Path(csv_output)

        written = append_csv(
            csv_path,
            txs,
        )

        logging.info(
            "CSV updated: +%d new rows -> %s",
            written,
            csv_path,
        )


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Advanced multithreaded Etherscan client",
    )

    parser.add_argument(
        "address",
        help="Ethereum address",
    )

    parser.add_argument(
        "apikey",
        help="Etherscan API key",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of transactions to fetch",
    )

    parser.add_argument(
        "--csv",
        help="CSV output file",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    return parser


# ============================================================
# Entry
# ============================================================

def main() -> None:

    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | "
            "%(levelname)-8s | "
            "%(message)s"
        ),
        datefmt="%H:%M:%S",
    )

    signal.signal(
        signal.SIGINT,
        lambda *_: sys.exit(130),
    )

    try:
        run(
            address=args.address,
            api_key=args.apikey,
            count=max(1, args.count),
            csv_output=args.csv,
        )

    except KeyboardInterrupt:
        logging.error("Interrupted")
        sys.exit(130)

    except Exception as e:
        logging.exception(
            "Fatal error: %s",
            e,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
