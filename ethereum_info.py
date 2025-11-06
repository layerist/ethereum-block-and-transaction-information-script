#!/usr/bin/env python3
"""
Etherscan ETH Tracker
---------------------
Fetch ETH balances, prices, and transactions for a given address
with retry logic, rate limiting, CSV export, and detailed logging.

Author: YourName
License: MIT
"""

import argparse
import csv
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_TRANSACTION_COUNT: int = 10
    DEFAULT_CSV_FILENAME: str = "etherscan_tx.csv"
    TIMEOUT: int = 10  # seconds
    RETRY_COUNT: int = 3
    RETRY_DELAY: float = 2.0  # seconds (base delay)
    JITTER: float = 0.3  # ±30% random jitter for delay
    RATE_LIMIT_DELAY: float = 0.25  # Etherscan: 5 calls/sec
    CSV_FIELDS: Tuple[str, ...] = (
        "hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"
    )


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
def setup_logger(verbose: bool = False) -> None:
    """Configure root logger with optional verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s UTC | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# -------------------------------------------------------------------
# Retry Decorator
# -------------------------------------------------------------------
def retry_request(func: Callable[..., Optional[Dict[str, Any]]]) -> Callable[..., Optional[Dict[str, Any]]]:
    """Retry a function performing HTTP requests with exponential backoff and jitter."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        delay = Config.RETRY_DELAY
        for attempt in range(1, Config.RETRY_COUNT + 1):
            try:
                time.sleep(Config.RATE_LIMIT_DELAY)  # basic rate limiting
                result = func(*args, **kwargs)
                if result is not None:
                    return result
            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt}/{Config.RETRY_COUNT} failed: {e}")

            if attempt < Config.RETRY_COUNT:
                jitter = random.uniform(1 - Config.JITTER, 1 + Config.JITTER)
                sleep_time = delay * jitter
                logging.debug(f"Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                delay *= 2  # exponential backoff
        logging.error("All retry attempts failed.")
        return None
    return wrapper


# -------------------------------------------------------------------
# API Request
# -------------------------------------------------------------------
@retry_request
def make_request(params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Perform a GET request to the Etherscan API with retries."""
    response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, dict):
        logging.error(f"Invalid JSON response: {response.text[:200]}")
        return None

    if data.get("status") != "1" or "result" not in data:
        msg = data.get("message", "Unknown API error")
        logging.error(f"Etherscan API error: {msg} | Params: {params}")
        return None

    return data


# -------------------------------------------------------------------
# Etherscan API Wrappers
# -------------------------------------------------------------------
def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    }
    data = make_request(params)
    try:
        return int(data["result"]) / Config.WEI_TO_ETH if data else None
    except (TypeError, KeyError, ValueError):
        logging.exception("Failed to parse balance.")
        return None


def get_eth_price(api_key: str) -> Optional[float]:
    params = {"module": "stats", "action": "ethprice", "apikey": api_key}
    data = make_request(params)
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (TypeError, KeyError, ValueError):
        logging.exception("Failed to parse ETH price.")
        return None


def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict[str, Any]]:
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key,
    }
    data = make_request(params)
    if not data:
        return []
    txs = data.get("result", [])
    if not isinstance(txs, list):
        logging.error("Unexpected response format for transactions.")
        return []
    return txs[:count]


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def calculate_transaction_totals(transactions: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    addr = address.lower()
    received = sum(
        int(tx.get("value", 0)) / Config.WEI_TO_ETH
        for tx in transactions if tx.get("to", "").lower() == addr
    )
    sent = sum(
        int(tx.get("value", 0)) / Config.WEI_TO_ETH
        for tx in transactions if tx.get("from", "").lower() == addr
    )
    return round(received, 8), round(sent, 8)


def format_timestamp(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""


def save_transactions_to_csv(transactions: List[Dict[str, Any]], filename: str) -> None:
    """Safely save new transactions to CSV, avoiding duplicates."""
    if not transactions:
        logging.info("No transactions to save.")
        return

    path = Path(filename)
    existing_hashes = set()

    if path.exists():
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                existing_hashes = {row["hash"] for row in reader if "hash" in row}
        except Exception:
            logging.warning("Failed to read existing CSV; rewriting file.")

    new_txs = [tx for tx in transactions if tx.get("hash") not in existing_hashes]
    if not new_txs:
        logging.info("No new transactions to append.")
        return

    write_header = not path.exists() or path.stat().st_size == 0
    temp_file = path.with_suffix(".tmp")

    with temp_file.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=Config.CSV_FIELDS)
        if write_header:
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

    temp_file.replace(path)
    logging.info(f"Saved {len(new_txs)} new transactions → '{path.resolve()}'")


# -------------------------------------------------------------------
# Main Logic
# -------------------------------------------------------------------
def run(address: str, api_key: str, count: int, csv_file: Optional[str]) -> None:
    logging.info(f"Fetching data for address: {address}")

    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.6f} ETH")
    else:
        logging.error("Failed to fetch balance.")

    transactions = get_last_transactions(address, api_key, count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, address)
        logging.info(f"Total Received: {total_in:.4f} ETH | Total Sent: {total_out:.4f} ETH")

        csv_name = csv_file or f"{address[:8]}_{datetime.now().strftime('%Y%m%d')}.csv"
        save_transactions_to_csv(transactions, csv_name)


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ETH balance, price, and transactions via Etherscan API.")
    parser.add_argument("address", help="Ethereum wallet address")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of recent transactions")
    parser.add_argument("--csv", type=str, default=None, help="Output CSV filename (optional)")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    setup_logger(args.verbose)

    try:
        run(args.address, args.apikey, args.count, args.csv)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    except Exception as e:
        logging.exception(f"Unhandled error: {e}")


# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
