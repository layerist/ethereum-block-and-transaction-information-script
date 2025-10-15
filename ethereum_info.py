#!/usr/bin/env python3
"""
Etherscan ETH Tracker
Fetch ETH balances, prices, and transactions for a given address,
with retry logic, CSV export, and detailed logging.

Author: YourName
License: MIT
"""

import argparse
import csv
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional, Tuple, Callable
import requests
from functools import wraps

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------
def setup_logger(verbose: bool = False) -> None:
    """Initialize logger with optional debug verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s UTC | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds
    RETRY_COUNT: int = 3
    RETRY_DELAY: float = 2.0  # seconds
    JITTER: float = 0.3  # ±30% random jitter for delay
    CSV_FIELDS: Tuple[str, ...] = (
        "hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"
    )

# -------------------------------------------------------------------
# Retry Decorator
# -------------------------------------------------------------------
def retry_request(func: Callable) -> Callable:
    """Retry an HTTP request function with exponential backoff and jitter."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        delay = Config.RETRY_DELAY
        for attempt in range(1, Config.RETRY_COUNT + 1):
            try:
                return func(*args, **kwargs)
            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt}/{Config.RETRY_COUNT} failed: {e}")
                if attempt < Config.RETRY_COUNT:
                    jitter = random.uniform(1 - Config.JITTER, 1 + Config.JITTER)
                    sleep_time = delay * jitter
                    logging.debug(f"Retrying in {sleep_time:.2f}s...")
                    sleep(sleep_time)
                    delay *= 2
        logging.error("Max retries reached; aborting request.")
        return None
    return wrapper

# -------------------------------------------------------------------
# API Request Helper
# -------------------------------------------------------------------
@retry_request
def make_request(params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Send a GET request to the Etherscan API with retries."""
    response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
    response.raise_for_status()
    data = response.json()

    if not data or data.get("status") != "1" or "result" not in data:
        msg = data.get("message", "Unknown API error")
        logging.error(f"Etherscan API error: {msg} | Params: {params}")
        return None

    return data

# -------------------------------------------------------------------
# Etherscan API Wrappers
# -------------------------------------------------------------------
def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Fetch ETH balance (in ETH) for a wallet address."""
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    }
    data = make_request(params)
    if not data:
        return None
    try:
        return int(data["result"]) / Config.WEI_TO_ETH
    except (ValueError, KeyError) as e:
        logging.exception(f"Failed to parse balance: {e}")
        return None

def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict[str, Any]]:
    """Fetch the latest transactions for a wallet address."""
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
    return data["result"][:count] if data and "result" in data else []

def get_eth_price(api_key: str) -> Optional[float]:
    """Fetch current ETH/USD price."""
    params = {"module": "stats", "action": "ethprice", "apikey": api_key}
    data = make_request(params)
    if not data:
        return None
    try:
        return float(data["result"]["ethusd"])
    except (ValueError, KeyError) as e:
        logging.exception(f"Failed to parse ETH price: {e}")
        return None

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def calculate_transaction_totals(transactions: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    """Calculate total ETH received and sent by the address."""
    addr = address.lower()
    received = sum(
        int(tx.get("value", 0)) / Config.WEI_TO_ETH
        for tx in transactions
        if tx.get("to", "").lower() == addr
    )
    sent = sum(
        int(tx.get("value", 0)) / Config.WEI_TO_ETH
        for tx in transactions
        if tx.get("from", "").lower() == addr
    )
    return received, sent

def format_timestamp(ts: Any) -> str:
    """Convert UNIX timestamp to ISO8601 UTC string."""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return ""

def save_transactions_to_csv(transactions: List[Dict[str, Any]], filename: str) -> None:
    """Append transactions to a CSV file, avoiding duplicates."""
    if not transactions:
        logging.warning("No transactions to save.")
        return

    path = Path(filename)
    existing_hashes = set()

    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            existing_hashes = {row["hash"] for row in reader if "hash" in row}

    new_transactions = [tx for tx in transactions if tx.get("hash") not in existing_hashes]
    if not new_transactions:
        logging.info("No new transactions to append.")
        return

    with path.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=Config.CSV_FIELDS)
        if not path.exists() or path.stat().st_size == 0:
            writer.writeheader()
        for tx in new_transactions:
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
    logging.info(f"Saved {len(new_transactions)} new transactions → '{path.resolve()}'")

# -------------------------------------------------------------------
# Main Logic
# -------------------------------------------------------------------
def run(address: str, api_key: str, count: int, csv_file: str) -> None:
    """Core logic for querying and displaying wallet data."""
    logging.info(f"Fetching data for address: {address}")

    balance = get_eth_balance(address, api_key)
    logging.info(f"ETH Balance: {balance:.4f} ETH" if balance is not None else "Failed to fetch balance.")

    transactions = get_last_transactions(address, api_key, count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, address)
        logging.info(f"Total Received: {total_in:.4f} ETH | Total Sent: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, csv_file)

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ETH balance, price, and transactions via Etherscan API.")
    parser.add_argument("address", help="Ethereum wallet address")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of recent transactions")
    parser.add_argument("--csv", type=str, default=Config.DEFAULT_CSV_FILENAME, help="CSV file for transaction export")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    setup_logger(verbose=args.verbose)

    try:
        run(args.address, args.apikey, args.count, args.csv)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    except Exception as e:
        logging.exception(f"Unhandled error: {e}")

# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
