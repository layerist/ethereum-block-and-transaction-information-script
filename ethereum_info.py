import requests
import json
import sys
import os
import logging
from typing import Optional, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ETHERSCAN_API_URL = 'https://api.etherscan.io/api'


def get_api_key() -> str:
    """
    Retrieve the Etherscan API key from environment variables.
    
    Returns:
        str: The Etherscan API key.
    """
    api_key = os.getenv('ETHERSCAN_API_KEY')
    if not api_key:
        logging.critical("Etherscan API key not found. Please set the ETHERSCAN_API_KEY environment variable.")
        raise EnvironmentError("Etherscan API key not set.")
    return api_key


def fetch_data_from_etherscan(params: Dict[str, str], api_key: str) -> Optional[Dict]:
    """
    Generic function to fetch data from the Etherscan API.

    Args:
        params (dict): Parameters for the API request.
        api_key (str): The Etherscan API key.

    Returns:
        dict or None: The JSON response from Etherscan if successful, None otherwise.
    """
    params['apikey'] = api_key
    try:
        response = requests.get(ETHERSCAN_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == '0':
            logging.error(f"Etherscan API error: {data.get('message', 'Unknown error')}")
            return None

        return data
    except requests.Timeout:
        logging.error("Request timed out.")
    except requests.RequestException as e:
        logging.error(f"Network error while fetching data: {e}")
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON response.")
    
    return None


def get_latest_block_number(api_key: str) -> Optional[int]:
    """
    Get the latest Ethereum block number.

    Args:
        api_key (str): The Etherscan API key.

    Returns:
        int or None: The latest block number, or None if there was an error.
    """
    params = {'module': 'proxy', 'action': 'eth_blockNumber'}
    data = fetch_data_from_etherscan(params, api_key)

    if data and 'result' in data:
        try:
            return int(data['result'], 16)
        except ValueError:
            logging.error("Failed to parse block number from response.")
    
    return None


def get_block_transaction_count(block_number: int, api_key: str) -> Optional[int]:
    """
    Get the number of transactions in a specific block.

    Args:
        block_number (int): The block number.
        api_key (str): The Etherscan API key.

    Returns:
        int or None: The transaction count, or None if there was an error.
    """
    params = {
        'module': 'proxy',
        'action': 'eth_getBlockTransactionCountByNumber',
        'tag': hex(block_number)
    }
    data = fetch_data_from_etherscan(params, api_key)

    if data and 'result' in data:
        try:
            return int(data['result'], 16)
        except ValueError:
            logging.error(f"Failed to parse transaction count for block {block_number}.")
    
    return None


def get_first_transaction_in_block(block_number: int, api_key: str) -> Optional[dict]:
    """
    Get the first transaction in a specific block.

    Args:
        block_number (int): The block number.
        api_key (str): The Etherscan API key.

    Returns:
        dict or None: The first transaction details, or None if there was an error.
    """
    params = {
        'module': 'proxy',
        'action': 'eth_getBlockByNumber',
        'tag': hex(block_number),
        'boolean': 'true'
    }
    data = fetch_data_from_etherscan(params, api_key)

    if data and 'result' in data:
        transactions = data['result'].get('transactions', [])
        if transactions:
            return transactions[0]
    
    logging.warning(f"No transactions found in block {block_number}.")
    return None


def main():
    """
    Main function to orchestrate the fetching and displaying of Ethereum block data.
    """
    try:
        api_key = get_api_key()
        latest_block_number = get_latest_block_number(api_key)

        if latest_block_number is None:
            logging.critical("Could not retrieve the latest block number.")
            sys.exit(1)

        logging.info(f'Latest Block Number: {latest_block_number}')

        transaction_count = get_block_transaction_count(latest_block_number, api_key)
        if transaction_count is None:
            logging.critical(f"Could not retrieve transaction count for block {latest_block_number}.")
            sys.exit(1)

        logging.info(f'Transaction Count in Block {latest_block_number}: {transaction_count}')

        if transaction_count > 0:
            first_transaction = get_first_transaction_in_block(latest_block_number, api_key)
            if first_transaction:
                logging.info('First Transaction in Block:')
                print(json.dumps(first_transaction, indent=4))
            else:
                logging.info('No transactions found in the latest block.')
        else:
            logging.info('No transactions in the latest block.')

    except KeyboardInterrupt:
        logging.info('Script stopped by user.')
        sys.exit(0)


if __name__ == "__main__":
    main()
