#!/usr/bin/env python3
"""
Improved Etherscan ETH Tracker
------------------------------
Fast + safe ETH tracker with:
- persistent session
- built-in rate limiting
- robust retry logic
- strict API validation
- CSV export with deduplication
"""

import argparse
import csv
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    TIMEOUT: int = 10
    MAX_RETRIES: int = 3
    BACKOFF: float = 2.0
    JITTER: float = 0.30
    RATE_LIMIT_DELAY: float = 0.25

    CSV_FIELDS: Tuple[str, ...] = (
        "hash", "blockNumber", "timeStamp",
        "from", "to", "value", "gas", "gasPrice"
    )


session = requests.Session()
session.headers.update({"User-Agent": "ETH-Tracker/2.0"})


# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------
class APIError(Exception):
    pass


def sleep_with_jitter(base: float) -> None:
    """Sleep with jitter to avoid rate spikes."""
    j = random.uniform(1 - Config.JITTER, 1 + Config.JITTER)
    time.sleep(base * j)


def format_timestamp(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""


# -------------------------------------------------------------------
# Etherscan API Layer
# -------------------------------------------------------------------
def etherscan_call(params: Dict[str, str]) -> Dict[str, Any]:
    """Universal safe API caller with retries + 429 handling."""
    delay = Config.BACKOFF

    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            # global rate limit
            time.sleep(Config.RATE_LIMIT_DELAY)

            resp = session.get(
                Config.BASE_URL,
                params=params,
                timeout=Config.TIMEOUT,
            )

            if resp.status_code == 429:
                logging.warning("Rate limited (HTTP 429). Waiting…")
                sleep_with_jitter(delay)
                delay *= 2
                continue

            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "1":
                raise APIError(data.get("message", "Unknown error"))

            return data

        except (requests.RequestException, APIError) as e:
            logging.warning(f"[Attempt {attempt}] API call failed: {e}")

            if attempt < Config.MAX_RETRIES:
                sleep_with_jitter(delay)
                delay *= 2

    raise APIError("All retry attempts failed")


# -------------------------------------------------------------------
# API Wrappers
# -------------------------------------------------------------------
def get_eth_balance(address: str, api_key: str) -> float:
    data = etherscan_call({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    })
    return int(data["result"]) / Config.WEI_TO_ETH


def get_eth_price(api_key: str) -> float:
    data = etherscan_call({
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key,
    })
    return float(data["result"]["ethusd"])


def get_transactions(address: str, api_key: str, limit: int) -> List[Dict[str, Any]]:
    data = etherscan_call({
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key,
    })

    txs = data["result"]
    return txs[:limit]


# -------------------------------------------------------------------
# CSV Handling
# -------------------------------------------------------------------
def save_csv(txs: List[Dict[str, Any]], path: str) -> None:
    if not txs:
        logging.info("No transactions to store.")
        return

    path = Path(path)
    existing = set()

    if path.exists():
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                existing = {row["hash"] for row in csv.DictReader(f)}
        except Exception:
            logging.warning("Could not read old CSV, rewriting.")

    new_txs = [tx for tx in txs if tx["hash"] not in existing]
    if not new_txs:
        logging.info("No new transactions to append.")
        return

    temp = path.with_suffix(".tmp")

    with temp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=Config.CSV_FIELDS)
        writer.writeheader()

        for tx in new_txs:
            writer.writerow({
                "hash": tx.get("hash", ""),
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": format_timestamp(tx.get("timeStamp")),
                "from": tx.get("from", ""),
                "to": tx.get("to", ""),
                "value": round(int(tx.get("value", 0)) / Config.WEI_TO_ETH, 8),
                "gas": tx.get("gas", ""),
                "gasPrice": tx.get("gasPrice", ""),
            })

    temp.replace(path)
    logging.info(f"Saved {len(new_txs)} new transactions → {path}")


# -------------------------------------------------------------------
# High-Level Logic
# -------------------------------------------------------------------
def calculate_totals(txs: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    address = address.lower()

    received = sum(
        int(tx["value"]) / Config.WEI_TO_ETH
        for tx in txs if tx["to"].lower() == address
    )
    sent = sum(
        int(tx["value"]) / Config.WEI_TO_ETH
        for tx in txs if tx["from"].lower() == address
    )
    return received, sent


def run(address: str, api_key: str, count: int, csv_out: Optional[str]) -> None:
    logging.info(f"→ Fetching data for: {address}")

    balance = get_eth_balance(address, api_key)
    logging.info(f"Balance: {balance:.6f} ETH")

    price = get_eth_price(api_key)
    logging.info(f"ETH Price: ${price:.2f}")

    txs = get_transactions(address, api_key, count)
    logging.info(f"Loaded {len(txs)} transactions")

    received, sent = calculate_totals(txs, address)
    logging.info(f"Total In:  {received:.4f} ETH")
    logging.info(f"Total Out: {sent:.4f} ETH")

    if csv_out:
        save_csv(txs, csv_out)


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("address")
    p.add_argument("apikey")
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--csv", type=str, default=None)
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        run(args.address, args.apikey, args.count, args.csv)
    except Exception as e:
        logging.exception(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
