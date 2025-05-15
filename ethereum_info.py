import requests
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass(frozen=True)
class Config:
    BASE_URL: str = "https://api.etherscan.io/api"
    WEI_TO_ETH: int = 10**18
    DEFAULT_CSV_FILENAME: str = "transactions.csv"
    DEFAULT_TRANSACTION_COUNT: int = 10
    TIMEOUT: int = 10  # seconds

def make_request(params: Dict[str, str]) -> Optional[Dict[str, any]]:
    """Make a GET request to the Etherscan API and return the result."""
    try:
        response = requests.get(Config.BASE_URL, params=params, timeout=Config.TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "1" or "result" not in data:
            logging.error(f"Etherscan API error: {data.get('message', 'Unknown error')} | Params: {params}")
            return None
        return data
    except requests.RequestException as e:
        logging.error(f"HTTP request failed: {e} | Params: {params}")
        return None

def get_eth_balance(address: str, api_key: str) -> Optional[float]:
    """Retrieve ETH balance for the specified address."""
    data = make_request({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    })
    try:
        return int(data["result"]) / Config.WEI_TO_ETH if data else None
    except (ValueError, KeyError, TypeError) as e:
        logging.exception(f"Failed to parse ETH balance: {e}")
        return None

def get_last_transactions(address: str, api_key: str, count: int) -> List[Dict[str, any]]:
    """Retrieve the most recent transactions for the address."""
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
    """Retrieve current ETH price in USD."""
    data = make_request({
        "module": "stats",
        "action": "ethprice",
        "apikey": api_key,
    })
    try:
        return float(data["result"]["ethusd"]) if data else None
    except (ValueError, KeyError, TypeError) as e:
        logging.exception(f"Failed to parse ETH price: {e}")
        return None

def calculate_transaction_totals(transactions: List[Dict[str, any]], address: str) -> Tuple[float, float]:
    """Calculate total ETH received and sent for the address."""
    addr = address.casefold()
    total_in = sum(int(tx["value"]) / Config.WEI_TO_ETH for tx in transactions if tx.get("to", "").casefold() == addr)
    total_out = sum(int(tx["value"]) / Config.WEI_TO_ETH for tx in transactions if tx.get("from", "").casefold() == addr)
    return total_in, total_out

def save_transactions_to_csv(transactions: List[Dict[str, any]], filename: str) -> None:
    """Save transaction list to a CSV file."""
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
                    row = {
                        "hash": tx.get("hash", ""),
                        "blockNumber": tx.get("blockNumber", ""),
                        "timeStamp": datetime.utcfromtimestamp(int(tx["timeStamp"])).isoformat() if "timeStamp" in tx else "",
                        "from": tx.get("from", ""),
                        "to": tx.get("to", ""),
                        "value": round(int(tx.get("value", 0)) / Config.WEI_TO_ETH, 8),
                        "gas": tx.get("gas", ""),
                        "gasPrice": tx.get("gasPrice", "")
                    }
                    writer.writerow(row)
                except Exception as e:
                    logging.warning(f"Skipping transaction due to formatting error: {e}")

        logging.info(f"Transactions saved to '{filename}'")
    except IOError as e:
        logging.error(f"Failed to write CSV file: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ETH balance, recent transactions, and save to CSV using Etherscan API."
    )
    parser.add_argument("address", help="Ethereum address to query")
    parser.add_argument("apikey", help="Etherscan API key")
    parser.add_argument("--count", type=int, default=Config.DEFAULT_TRANSACTION_COUNT, help="Number of recent transactions to fetch")
    parser.add_argument("--csv", default=Config.DEFAULT_CSV_FILENAME, help="Output CSV filename")
    args = parser.parse_args()

    address = args.address
    api_key = args.apikey

    logging.info(f"Fetching data for Ethereum address: {address}")

    balance = get_eth_balance(address, api_key)
    if balance is not None:
        logging.info(f"ETH Balance: {balance:.4f} ETH")
    else:
        logging.error("Failed to retrieve ETH balance.")

    transactions = get_last_transactions(address, api_key, args.count)
    logging.info(f"Retrieved {len(transactions)} transactions.")

    eth_price = get_eth_price(api_key)
    if eth_price is not None:
        logging.info(f"Current ETH Price: ${eth_price:.2f}")
    else:
        logging.error("Failed to retrieve ETH price.")

    if transactions:
        total_in, total_out = calculate_transaction_totals(transactions, address)
        logging.info(f"Total Incoming: {total_in:.4f} ETH | Total Outgoing: {total_out:.4f} ETH")
        save_transactions_to_csv(transactions, args.csv)

if __name__ == "__main__":
    main()
