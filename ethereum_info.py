import requests
import json
import sys
import os

ETHERSCAN_API_URL = 'https://api.etherscan.io/api'

def get_api_key():
    return os.getenv('ETHERSCAN_API_KEY')

def get_latest_block_number(api_key):
    try:
        response = requests.get(f'{ETHERSCAN_API_URL}?module=proxy&action=eth_blockNumber&apikey={api_key}')
        response.raise_for_status()
        data = response.json()
        latest_block_number = int(data['result'], 16)
        return latest_block_number
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching latest block number: {e}")
        return None

def get_block_transaction_count(block_number, api_key):
    try:
        response = requests.get(f'{ETHERSCAN_API_URL}?module=proxy&action=eth_getBlockTransactionCountByNumber&tag={hex(block_number)}&apikey={api_key}')
        response.raise_for_status()
        data = response.json()
        transaction_count = int(data['result'], 16)
        return transaction_count
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching transaction count for block {block_number}: {e}")
        return None

def get_first_transaction_in_block(block_number, api_key):
    try:
        response = requests.get(f'{ETHERSCAN_API_URL}?module=proxy&action=eth_getBlockByNumber&tag={hex(block_number)}&boolean=true&apikey={api_key}')
        response.raise_for_status()
        data = response.json()
        transactions = data['result'].get('transactions', [])
        if transactions:
            return transactions[0]
        else:
            return None
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching first transaction in block {block_number}: {e}")
        return None

def main():
    api_key = get_api_key()
    if not api_key:
        print("Error: Etherscan API key not found. Please set the ETHERSCAN_API_KEY environment variable.")
        sys.exit(1)

    latest_block_number = get_latest_block_number(api_key)
    if latest_block_number is None:
        sys.exit(1)

    print(f'Latest Block Number: {latest_block_number}')

    transaction_count = get_block_transaction_count(latest_block_number, api_key)
    if transaction_count is None:
        sys.exit(1)

    print(f'Transaction Count in Block {latest_block_number}: {transaction_count}')

    if transaction_count > 0:
        first_transaction = get_first_transaction_in_block(latest_block_number, api_key)
        if first_transaction:
            print('First Transaction in Block:')
            print(json.dumps(first_transaction, indent=4))
        else:
            print('No transactions in the latest block.')
    else:
        print('No transactions in the latest block.')

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Script stopped by user.')
        sys.exit(0)
