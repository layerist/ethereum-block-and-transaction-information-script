#!/usr/bin/env python3
"""
Hardened Etherscan ETH Tracker (v3.2)
-----------------------------------
Improvements:
- Strict Etherscan response validation
- Safer numeric parsing
- Pagination guardrails
- Centralized retry/backoff logic
- Truly atomic CSV append
- Cleaner logging and typing
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18

    TIMEOUT: int = 10
    MAX_RETRIES: int = 5
    BACKOFF_BASE: float = 1.5
    BACKOFF_MAX: float = 30.0
    JITTER: float = 0.25
    RATE_LIMIT_DELAY: float = 0.25

    PAGE_SIZE: int = 100  # Etherscan hard limit

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
class APIError(RuntimeError):
    """Etherscan logical or HTTP error."""


class RateLimitError(APIError):
    """Explicit rate-limit error."""


class ValidationError(ValueError):
    """User input validation error."""


# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------
def sleep_with_jitter(delay: float) -> None:
    jitter = random.uniform(1 - Config.JITTER, 1 + Config.JITTER)
    time.sleep(min(delay * jitter, Config.BACKOFF_MAX))


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def iso_utc(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(safe_int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""


def validate_address(address: str) -> str:
    if not ADDRESS_RE.fullmatch(address):
        raise ValidationError(f"Invalid Ethereum address: {address}")
    return address.lower()


# -------------------------------------------------------------------
# Etherscan Client
# -------------------------------------------------------------------
class EtherscanClient:
    def __init__(self, api_key: str) -> None:
        if not api_key or len(api_key) < 10:
            raise ValidationError("Invalid Etherscan API key")

        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "ETH-Tracker/3.2"})

    def _request(self, params: Dict[str, str]) -> Dict[str, Any]:
        delay = Config.BACKOFF_BASE

        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                time.sleep(Config.RATE_LIMIT_DELAY)

                resp = self.session.get(
                    Config.BASE_URL,
                    params={**params, "apikey": self.api_key},
                    timeout=Config.TIMEOUT,
                )

                if resp.status_code == 429:
                    raise RateLimitError("HTTP 429 rate limit")

                resp.raise_for_status()
                data = resp.json()

                if not isinstance(data, dict):
                    raise APIError("Non-dict JSON response")

                status = data.get("status")
                message = str(data.get("message", ""))

                if status == "0":
                    if "rate limit" in message.lower():
                        raise RateLimitError(message)
                    if message not in ("OK", "No transactions found"):
                        raise APIError(message)

                if "result" not in data:
                    raise APIError("Missing result field")

                return data

            except RateLimitError as e:
                logging.warning("Rate limited (%d/%d): %s", attempt, Config.MAX_RETRIES, e)
            except (requests.RequestException, APIError) as e:
                logging.warning("API error (%d/%d): %s", attempt, Config.MAX_RETRIES, e)

            if attempt < Config.MAX_RETRIES:
                sleep_with_jitter(delay)
                delay *= 2

        raise APIError("Etherscan request failed after retries")

    # ---------------- API wrappers ----------------
    def get_balance(self, address: str) -> float:
        data = self._request({
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
        })
        return safe_int(data["result"]) / Config.WEI_TO_ETH

    def get_eth_price(self) -> float:
        data = self._request({
            "module": "stats",
            "action": "ethprice",
        })
        return float(data["result"]["ethusd"])

    def get_transactions(self, address: str, limit: int) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        page = 1
        seen_pages = 0

        while len(results) < limit:
            data = self._request({
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": "0",
                "endblock": "99999999",
                "page": str(page),
                "offset": str(Config.PAGE_SIZE),
                "sort": "desc",
            })

            batch = data.get("result", [])
            if not batch:
                break

            results.extend(batch)
            seen_pages += 1

            if len(batch) < Config.PAGE_SIZE:
                break

            if seen_pages > 10_000:
                raise APIError("Pagination safety limit exceeded")

            page += 1

        return results[:limit]


# -------------------------------------------------------------------
# CSV Handling
# -------------------------------------------------------------------
def load_existing_hashes(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            return {row["hash"] for row in csv.DictReader(f) if row.get("hash")}
    except Exception:
        logging.warning("Failed to read CSV, deduplication disabled")
        return set()


def append_csv(txs: Iterable[Dict[str, Any]], path: Path) -> None:
    existing = load_existing_hashes(path)
    rows = [tx for tx in txs if tx.get("hash") not in existing]

    if not rows:
        logging.info("No new transactions to append")
        return

    tmp_path = path.with_suffix(".tmp")
    write_header = not path.exists()

    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=Config.CSV_FIELDS)
        writer.writeheader()

        for tx in rows:
            writer.writerow({
                "hash": tx.get("hash", ""),
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": iso_utc(tx.get("timeStamp")),
                "from": tx.get("from", ""),
                "to": tx.get("to", ""),
                "value_eth": round(safe_int(tx.get("value")) / Config.WEI_TO_ETH, 8),
                "gas": tx.get("gas", ""),
                "gasPrice": tx.get("gasPrice", ""),
                "isError": tx.get("isError", "0"),
            })

    mode = "a" if path.exists() else "w"
    with path.open(mode, encoding="utf-8", newline="") as out, \
         tmp_path.open("r", encoding="utf-8") as inp:
        if not write_header:
            next(inp)
        out.write(inp.read())

    tmp_path.unlink(missing_ok=True)
    logging.info("Appended %d transactions → %s", len(rows), path)


# -------------------------------------------------------------------
# Analytics
# -------------------------------------------------------------------
def calculate_totals(
    txs: Iterable[Dict[str, Any]],
    address: str,
) -> Tuple[float, float]:
    received = sent = 0.0

    for tx in txs:
        if tx.get("isError") == "1":
            continue

        value = safe_int(tx.get("value")) / Config.WEI_TO_ETH
        if tx.get("to", "").lower() == address:
            received += value
        elif tx.get("from", "").lower() == address:
            sent += value

    return received, sent


# -------------------------------------------------------------------
# Main Logic
# -------------------------------------------------------------------
def run(
    address: str,
    api_key: str,
    count: int,
    csv_out: Optional[str],
) -> None:
    address = validate_address(address)
    client = EtherscanClient(api_key)

    logging.info("Fetching data for %s", address)

    balance = client.get_balance(address)
    price = client.get_eth_price()
    txs = client.get_transactions(address, count)

    received, sent = calculate_totals(txs, address)

    logging.info("Balance      : %.6f ETH", balance)
    logging.info("ETH price    : $%.2f", price)
    logging.info("Transactions : %d", len(txs))
    logging.info("Total in     : %.6f ETH", received)
    logging.info("Total out    : %.6f ETH", sent)

    if csv_out:
        append_csv(txs, Path(csv_out))


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Hardened Etherscan ETH tracker")
    parser.add_argument("address", help="Ethereum address")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--csv", type=str)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        run(args.address, args.apikey, args.count, args.csv)
    except Exception:
        logging.exception("Fatal error")
        raise


if __name__ == "__main__":
    main()
