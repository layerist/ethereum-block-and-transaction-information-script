import requests
import json
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ETHERSCAN_API_URL = 'https://api.etherscan.io/api'

def get_api_key():
    """
    Retrieve the Etherscan API key from environment variables.
    
    Returns:
        str: The Etherscan API key.
    """
    api_key = os.getenv('ETHERSCAN_API_KEY')
    if not api_key:
        logging.critical("Etherscan API key not found. Please set the ETHERSCAN_API_KEY environment variable.")
        sys.exit(1)
    return api_key

def fetch_data_from_etherscan(params, api_key):
    """
    Generic function to fetch data from Etherscan API.
    
    Args:
        params (dict): Parameters for the API request.
        api_key (str): The Etherscan API key.
        
    Returns:
        dict or None: The JSON response from Etherscan if successful, None otherwise.
    """
    try:
        params['apikey'] = api_key
        response = requests.get(ETHERSCAN_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == '0':
            logging.error(f"Etherscan API returned an error: {data.get('message', 'Unknown error')}")
            return None
        
        return data
    
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred: {req_err}")
    
    return None

def get_latest_block_number(api_key):
    """
    Get the latest Ethereum block number.
    
    Args:
        api_key (str): The Etherscan API key.
        
    Returns:
        int or None: The latest block number or None if there was an error.
    """
    params = {'module': 'proxy', 'action': 'eth_blockNumber'}
    data = fetch_data_from_etherscan(params, api_key)
    
    if data and 'result' in data:
        try:
            return int(data['result'], 16)
        except ValueError:
            logging.error("Failed to parse block number from Etherscan response.")
    
    logging.error("Invalid data received for the latest block number.")
    return None

def get_block_transaction_count(block_number, api_key):
    """
    Get the number of transactions in a specific block.
    
    Args:
        block_number (int): The block number.
        api_key (str): The Etherscan API key.
        
    Returns:
        int or None: The transaction count or None if there was an error.
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
            logging.error(f"Failed to parse transaction count from Etherscan response for block {block_number}.")
    
    logging.error(f"Invalid data received for transaction count in block {block_number}.")
    return None

def get_first_transaction_in_block(block_number, api_key):
    """
    Get the first transaction in a specific block.
    
    Args:
        block_number (int): The block number.
        api_key (str): The Etherscan API key.
        
    Returns:
        dict or None: The first transaction details or None if there was an error.
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
            logging.info('No transactions in the latest block.')
    else:
        logging.info('No transactions in the latest block.')

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Script stopped by user.')
        sys.exit(0)
