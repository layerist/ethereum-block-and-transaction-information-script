import requests
import json
import sys
import os
import logging
from typing import Optional, Dict, Any

# Constants
ETHERSCAN_API_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10
ETHERSCAN_API_MODULE = "proxy"

ETHERSCAN_ACTIONS = {
    "latest_block": "eth_blockNumber",
    "transaction_count": "eth_getBlockTransactionCountByNumber",
    "block_by_number": "eth_getBlockByNumber",
}

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_api_key() -> str:
    """Retrieve the Etherscan API key from environment variables."""
    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        logging.critical("Etherscan API key not found. Set the 'ETHERSCAN_API_KEY' environment variable.")
        raise EnvironmentError("Etherscan API key not set.")
    return api_key

def fetch_data_from_etherscan(params: Dict[str, str], api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch data from the Etherscan API with error handling."""
    params["apikey"] = api_key
    try:
        response = requests.get(ETHERSCAN_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "0":
            logging.error(f"Etherscan API error: {data.get('message', 'Unknown error')}")
            return None
        
        return data
    except (requests.Timeout, requests.RequestException) as e:
        logging.error(f"Network error while accessing Etherscan API: {e}")
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON response from Etherscan API.")
    return None

def get_latest_block_number(api_key: str) -> Optional[int]:
    """Retrieve the latest Ethereum block number."""
    params = {"module": ETHERSCAN_API_MODULE, "action": ETHERSCAN_ACTIONS["latest_block"]}
    data = fetch_data_from_etherscan(params, api_key)
    
    try:
        return int(data["result"], 16) if data and "result" in data else None
    except ValueError:
        logging.error("Failed to parse block number from Etherscan API response.")
        return None

def get_block_transaction_count(block_number: int, api_key: str) -> Optional[int]:
    """Retrieve the number of transactions in a specific block."""
    params = {
        "module": ETHERSCAN_API_MODULE,
        "action": ETHERSCAN_ACTIONS["transaction_count"],
        "tag": hex(block_number),
    }
    data = fetch_data_from_etherscan(params, api_key)
    
    try:
        return int(data["result"], 16) if data and "result" in data else None
    except ValueError:
        logging.error(f"Failed to parse transaction count for block {block_number}.")
        return None

def get_first_transaction_in_block(block_number: int, api_key: str) -> Optional[Dict[str, Any]]:
    """Retrieve the first transaction in a specific block."""
    params = {
        "module": ETHERSCAN_API_MODULE,
        "action": ETHERSCAN_ACTIONS["block_by_number"],
        "tag": hex(block_number),
        "boolean": "true",
    }
    data = fetch_data_from_etherscan(params, api_key)
    
    if data and "result" in data:
        transactions = data["result"].get("transactions", [])
        return transactions[0] if transactions else None
    
    logging.warning(f"No transactions found in block {block_number}.")
    return None

def main() -> None:
    """Main function to retrieve Ethereum block data."""
    try:
        api_key = get_api_key()
        latest_block_number = get_latest_block_number(api_key)
        
        if latest_block_number is None:
            logging.critical("Failed to retrieve the latest block number.")
            sys.exit(1)
        
        logging.info(f"Latest Block Number: {latest_block_number}")
        
        transaction_count = get_block_transaction_count(latest_block_number, api_key)
        
        if transaction_count is None:
            logging.critical(f"Failed to retrieve transaction count for block {latest_block_number}.")
            sys.exit(1)
        
        logging.info(f"Transaction Count in Block {latest_block_number}: {transaction_count}")
        
        if transaction_count > 0:
            first_transaction = get_first_transaction_in_block(latest_block_number, api_key)
            if first_transaction:
                logging.info("First Transaction in Block:")
                print(json.dumps(first_transaction, indent=4))
            else:
                logging.info("No transactions found in the latest block.")
        else:
            logging.info("No transactions in the latest block.")
    
    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
        sys.exit(0)
    except EnvironmentError as e:
        logging.critical(e)
        sys.exit(1)

if __name__ == "__main__":
    main()
