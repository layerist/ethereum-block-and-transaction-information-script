import requests
import json
import sys
import os
import logging
from typing import Optional, Dict, Any

# Constants
ETHERSCAN_API_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_api_key() -> str:
    """
    Retrieve the Etherscan API key from environment variables.

    Returns:
        str: The Etherscan API key.

    Raises:
        EnvironmentError: If the API key is not set.
    """
    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        logging.critical(
            "Etherscan API key not found. Set the 'ETHERSCAN_API_KEY' environment variable."
        )
        raise EnvironmentError("Etherscan API key not set.")
    return api_key

def fetch_data_from_etherscan(params: Dict[str, str], api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch data from the Etherscan API.

    Args:
        params (Dict[str, str]): Parameters for the API request.
        api_key (str): The Etherscan API key.

    Returns:
        Optional[Dict[str, Any]]: The JSON response from Etherscan, or None if an error occurred.
    """
    params["apikey"] = api_key
    try:
        response = requests.get(ETHERSCAN_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "0":
            logging.error(f"Etherscan API error: {data.get('message', 'Unknown error')}")
            return None

        return data
    except requests.Timeout:
        logging.error("Request to Etherscan API timed out.")
    except requests.RequestException as e:
        logging.error(f"Network error while accessing Etherscan API: {e}")
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON response from Etherscan API.")
    return None

def get_latest_block_number(api_key: str) -> Optional[int]:
    """
    Retrieve the latest Ethereum block number.

    Args:
        api_key (str): The Etherscan API key.

    Returns:
        Optional[int]: The latest block number, or None if an error occurred.
    """
    params = {"module": "proxy", "action": "eth_blockNumber"}
    data = fetch_data_from_etherscan(params, api_key)

    if data and "result" in data:
        try:
            return int(data["result"], 16)
        except ValueError:
            logging.error("Failed to parse block number from Etherscan API response.")
    return None

def get_block_transaction_count(block_number: int, api_key: str) -> Optional[int]:
    """
    Retrieve the number of transactions in a specific block.

    Args:
        block_number (int): The block number.
        api_key (str): The Etherscan API key.

    Returns:
        Optional[int]: The transaction count, or None if an error occurred.
    """
    params = {
        "module": "proxy",
        "action": "eth_getBlockTransactionCountByNumber",
        "tag": hex(block_number),
    }
    data = fetch_data_from_etherscan(params, api_key)

    if data and "result" in data:
        try:
            return int(data["result"], 16)
        except ValueError:
            logging.error(f"Failed to parse transaction count for block {block_number}.")
    return None

def get_first_transaction_in_block(block_number: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve the first transaction in a specific block.

    Args:
        block_number (int): The block number.
        api_key (str): The Etherscan API key.

    Returns:
        Optional[Dict[str, Any]]: Details of the first transaction, or None if an error occurred.
    """
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
    }
    data = fetch_data_from_etherscan(params, api_key)

    if data and "result" in data:
        transactions = data["result"].get("transactions", [])
        if transactions:
            return transactions[0]

    logging.warning(f"No transactions found in block {block_number}.")
    return None

def main():
    """
    Main function to orchestrate Ethereum block data retrieval and display.
    """
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
