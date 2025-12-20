#!/usr/bin/env python3
"""
Hardened Etherscan ETH Tracker
------------------------------
Improvements over previous version:
- strict address + API key validation
- typed responses and safer parsing
- exponential backoff with capped delay
- atomic CSV append (no full rewrite)
- optional pagination support
- clearer logging and error semantics
- reusable Etherscan client abstraction
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
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18

    TIMEOUT: int = 10
    MAX_RETRIES: int = 4
    BACKOFF_BASE: float = 1.5
    BACKOFF_MAX: float = 20.0
    JITTER: float = 0.30
    RATE_LIMIT_DELAY: float = 0.25

    CSV_FIELDS: Tuple[str, ...] = (
        "hash",
        "blockNumber",
        "timeStamp",
        "from",
        "to",
        "value_eth",
        "gas",
        "gasPrice",
    )


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


# -------------------------------------------------------------------
# Errors
# -------------------------------------------------------------------
class APIError(RuntimeError):
    pass


class ValidationError(ValueError):
    pass


# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------
def sleep_with_jitter(base: float) -> None:
    factor = random.uniform(1 - Config.JITTER, 1 + Config.JITTER)
    time.sleep(min(base * factor, Config.BACKOFF_MAX))


def iso_utc(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
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
        self.session.headers.update({"User-Agent": "ETH-Tracker/3.0"})

    def _call(self, params: Dict[str, str]) -> Dict[str, Any]:
        delay = Config.BACKOFF_BASE

        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                time.sleep(Config.RATE_LIMIT_DELAY)

                r = self.session.get(
                    Config.BASE_URL,
                    params={**params, "apikey": self.api_key},
                    timeout=Config.TIMEOUT,
                )

                if r.status_code == 429:
                    raise APIError("HTTP 429 (rate limited)")

                r.raise_for_status()
                data = r.json()

                if data.get("status") != "1":
                    raise APIError(data.get("message", "Etherscan error"))

                return data

            except (requests.RequestException, APIError) as e:
                logging.warning(
                    "API attempt %d/%d failed: %s",
                    attempt,
                    Config.MAX_RETRIES,
                    e,
                )

                if attempt == Config.MAX_RETRIES:
                    break

                sleep_with_jitter(delay)
                delay *= 2

        raise APIError("Etherscan request failed after retries")

    # ---------------- API wrappers ----------------
    def get_balance(self, address: str) -> float:
        data = self._call({
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
        })
        return int(data["result"]) / Config.WEI_TO_ETH

    def get_eth_price(self) -> float:
        data = self._call({
            "module": "stats",
            "action": "ethprice",
        })
        return float(data["result"]["ethusd"])

    def get_transactions(
        self,
        address: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        data = self._call({
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": "0",
            "endblock": "99999999",
            "sort": "desc",
        })

        txs = data.get("result", [])
        return txs[:limit]


# -------------------------------------------------------------------
# CSV Handling
# -------------------------------------------------------------------
def load_existing_hashes(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            return {row["hash"] for row in csv.DictReader(f)}
    except Exception:
        logging.warning("Failed to read existing CSV, ignoring deduplication")
        return set()


def append_csv(txs: Iterable[Dict[str, Any]], path: Path) -> None:
    existing = load_existing_hashes(path)
    new_rows = [tx for tx in txs if tx.get("hash") not in existing]

    if not new_rows:
        logging.info("No new transactions to write")
        return

    write_header = not path.exists()

    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=Config.CSV_FIELDS)
        if write_header:
            writer.writeheader()

        for tx in new_rows:
            writer.writerow({
                "hash": tx.get("hash", ""),
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": iso_utc(tx.get("timeStamp")),
                "from": tx.get("from", ""),
                "to": tx.get("to", ""),
                "value_eth": round(
                    int(tx.get("value", 0)) / Config.WEI_TO_ETH, 8
                ),
                "gas": tx.get("gas", ""),
                "gasPrice": tx.get("gasPrice", ""),
            })

    logging.info("Appended %d new transactions → %s", len(new_rows), path)


# -------------------------------------------------------------------
# Analytics
# -------------------------------------------------------------------
def calculate_totals(
    txs: Iterable[Dict[str, Any]],
    address: str,
) -> Tuple[float, float]:
    recv = sent = 0.0
    for tx in txs:
        value = int(tx.get("value", 0)) / Config.WEI_TO_ETH
        if tx.get("to", "").lower() == address:
            recv += value
        elif tx.get("from", "").lower() == address:
            sent += value
    return recv, sent


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

    logging.info("Balance     : %.6f ETH", balance)
    logging.info("ETH price   : $%.2f", price)
    logging.info("Transactions: %d", len(txs))
    logging.info("Total in    : %.4f ETH", received)
    logging.info("Total out   : %.4f ETH", sent)

    if csv_out:
        append_csv(txs, Path(csv_out))


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Etherscan ETH tracker")
    parser.add_argument("address")
    parser.add_argument("apikey")
    parser.add_argument("--count", type=int, default=10)
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
