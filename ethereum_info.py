import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds

def make_request(params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Send a GET request to the Etherscan API with the given parameters."""
    try:
        response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "1" or "result" not in data:
            logging.error(f"API error: {data.get('message', 'Unknown')} | Params: {params}")
            return None
        return data
    except requests.RequestException as e:
        logging.error(f"Request failed: {e} | Params: {params}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Return ETH balance for a given address."""
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
    """Return the most recent transactions for an address."""
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
    """Return current ETH price in USD."""
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
    """Calculate total ETH received and sent for the address."""
    addr = address.lower()
    total_in = sum(
        int(tx["value"]) / Config.WEI_TO_ETH
        for tx in transactions if tx.get("to", "").lower() == addr
    )
    total_out = sum(
        int(tx["value"]) / Config.WEI_TO_ETH
        for tx in transactions if tx.get("from", "").lower() == addr
    )
    return total_in, total_out

def save_transactions_to_csv(transactions: List[Dict[str, Any]], filename: str) -> None:
    """Save transactions to a CSV file."""
    if not transactions:
        logging.warning("No transactions to save.")
        return

    fieldnames = ["hash", "blockNumber", "timeStamp", "from", "to", "value", "gas", "gasPrice"]

    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
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
                    logging.warning(f"Skipping transaction due to format error: {e}")
        logging.info(f"Saved {len(transactions)} transactions to '{filename}'")
    except IOError as e:
        logging.error(f"Failed to write to CSV file: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ETH balance, transactions, and save to CSV using Etherscan.")
    parser.add_argument("address", help="Ethereum wallet address")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of recent transactions")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME, help="CSV output filename")
    args = parser.parse_args()

    address, api_key = args.address, args.apikey
    logging.info(f"Processing address: {address}")

    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Unable to fetch ETH balance.")

    transactions = get_last_transactions(address, api_key, args.count)
    logging.info(f"Fetched {len(transactions)} transactions.")

    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f"ETH Price: ${eth_price:.2f}")
    else:
        logging.warning("ETH price unavailable.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, address)
        logging.info(f"Total In: {total_in:.4f} ETH | Total Out: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
