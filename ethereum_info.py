import requests
import json
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ETHERSCAN_API_URL = 'https://api.etherscan.io/api'

def get_api_key():
    """Retrieve the Etherscan API key from environment variables."""
    api_key = os.getenv('ETHERSCAN_API_KEY')
    if not api_key:
        logging.error("Etherscan API key not found. Please set the ETHERSCAN_API_KEY environment variable.")
        sys.exit(1)
    return api_key

def fetch_data_from_etherscan(params, api_key):
    """Generic function to fetch data from Etherscan API."""
    try:
        params['apikey'] = api_key
        response = requests.get(ETHERSCAN_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Etherscan: {e}")
        return None

def get_latest_block_number(api_key):
    """Get the latest Ethereum block number."""
    params = {'module': 'proxy', 'action': 'eth_blockNumber'}
    data = fetch_data_from_etherscan(params, api_key)
    if data and 'result' in data:
        return int(data['result'], 16)
    logging.error("Invalid data received for latest block number.")
    return None

def get_block_transaction_count(block_number, api_key):
    """Get the number of transactions in a specific block."""
    params = {
        'module': 'proxy',
        'action': 'eth_getBlockTransactionCountByNumber',
        'tag': hex(block_number)
    }
    data = fetch_data_from_etherscan(params, api_key)
    if data and 'result' in data:
        return int(data['result'], 16)
    logging.error(f"Invalid data received for transaction count in block {block_number}.")
    return None

def get_first_transaction_in_block(block_number, api_key):
    """Get the first transaction in a specific block."""
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
    """Main function to orchestrate the fetching and displaying of Ethereum block data."""
    api_key = get_api_key()

    latest_block_number = get_latest_block_number(api_key)
    if latest_block_number is None:
        sys.exit(1)

    logging.info(f'Latest Block Number: {latest_block_number}')

    transaction_count = get_block_transaction_count(latest_block_number, api_key)
    if transaction_count is None:
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
