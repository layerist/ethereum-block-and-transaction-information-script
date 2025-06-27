import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds
    RETRY_COUNT: int = 3
    RETRY_DELAY: float = 2.0  # seconds


def make_request(params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Sends a GET request to the Etherscan API with retry logic.
    """
    for attempt in range(1, Config.RETRY_COUNT + 1):
        try:
            response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if data.get("status") != "1" or "result" not in data:
                logging.error(f"API error: {data.get('message', 'Unknown')} | Params: {params}")
                return None
            return data
        except requests.RequestException as e:
            logging.warning(f"Request failed (attempt {attempt}/{Config.RETRY_COUNT}): {e}")
            if attempt < Config.RETRY_COUNT:
                sleep(Config.RETRY_DELAY)
            else:
                logging.error("Max retries reached.")
    return None


def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """
    Returns the ETH balance of a specified address.
    """
    data = make_request({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    })
    if data:
        try:
            return int(data["result"]) / Config.WEI_TO_ETH
        except (ValueError, KeyError, TypeError) as e:
            logging.exception(f"Error parsing balance: {e}")
    return None


def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict[str, Any]]:
    """
    Returns the most recent transactions for the specified address.
    """
    data = make_request({
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "apikey": api_key,
    })
    return data.get("result", [])[:count] if data else []


def get_eth_price(api_key: str) -> Optional[float]:
    """
    Returns the current ETH price in USD.
    """
    data = make_request({
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key,
    })
    if data:
        try:
            return float(data["result"]["ethusd"])
        except (ValueError, KeyError, TypeError) as e:
            logging.exception(f"Error parsing ETH price: {e}")
    return None


def calculate_transaction_totals(transactions: List[Dict[str, Any]], address: str) -> Tuple[float, float]:
    """
    Calculates total ETH received and sent for the given address.
    """
    addr = address.lower()
    total_in = sum(
        int(tx.get("value", 0)) / Config.WEI_TO_ETH
        for tx in transactions if tx.get("to", "").lower() == addr
    )
    total_out = sum(
        int(tx.get("value", 0)) / Config.WEI_TO_ETH
        for tx in transactions if tx.get("from", "").lower() == addr
    )
    return total_in, total_out


def save_transactions_to_csv(transactions: List[Dict[str, Any]], filename: str) -> None:
    """
    Saves the transactions to a CSV file.
    """
    if not transactions:
        logging.warning("No transactions to save.")
        return

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"]
    path = Path(filename)

    try:
        with path.open(mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for tx in transactions:
                try:
                    writer.writerow({
                        "hash": tx.get("hash", ""),
                        "blockNumber": tx.get("blockNumber", ""),
                        "timeStamp": datetime.utcfromtimestamp(
                            int(tx["timeStamp"])
                        ).isoformat() if "timeStamp" in tx else "",
                        "from": tx.get("from", ""),
                        "to": tx.get("to", ""),
                        "value": round(int(tx.get("value", 0)) / Config.WEI_TO_ETH, 8),
                        "gas": tx.get("gas", ""),
                        "gasPrice": tx.get("gasPrice", ""),
                    })
                except Exception as e:
                    logging.warning(f"Skipping transaction due to error: {e}")
        logging.info(f"Saved {len(transactions)} transactions to '{path.resolve()}'")
    except IOError as e:
        logging.error(f"Failed to write CSV file: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ETH balance, recent transactions, and save to CSV via Etherscan"
    )
    parser.add_argument("address", help="Ethereum wallet address")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument(
        "--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT,
        help=f"Number of recent transactions to fetch (default: {Config.DEFAULT_TRANSACTION_COUNT})"
    )
    parser.add_argument(
        "--csv", type=str, default=Config.DEFAULT_CSV_FILENAME,
        help=f"CSV output filename (default: {Config.DEFAULT_CSV_FILENAME})"
    )

    args = parser.parse_args()

    logging.info(f"Processing address: {args.address}")

    balance = get_eth_balance(args.address, args.apikey)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Unable to fetch ETH balance.")

    transactions = get_last_transactions(args.address, args.apikey, args.count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    eth_price = get_eth_price(args.apikey)
    if eth_price is not None:
        logging.info(f"ETH Price: ${eth_price:.2f}")
    else:
        logging.warning("ETH price unavailable.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, args.address)
        logging.info(f"Total In: {total_in:.4f} ETH | Total Out: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)


if __name__ == "__main__":
    main()
