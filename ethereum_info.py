import requests
import json
import sys

# Replace with your Etherscan API key
API_KEY = 'YOUR_API_KEY_HERE'
ETHERSCAN_API_URL = 'https://api.etherscan.io/api'

def get_latest_block_number():
    response = requests.get(f'{ETHERSCAN_API_URL}?module=proxy&action=eth_blockNumber&apikey={API_KEY}')
    data = response.json()
    latest_block_number = int(data['result'], 16)
    return latest_block_number

def get_block_transaction_count(block_number):
    response = requests.get(f'{ETHERSCAN_API_URL}?module=proxy&action=eth_getBlockTransactionCountByNumber&tag={hex(block_number)}&apikey={API_KEY}')
    data = response.json()
    transaction_count = int(data['result'], 16)
    return transaction_count

def get_first_transaction_in_block(block_number):
    response = requests.get(f'{ETHERSCAN_API_URL}?module=proxy&action=eth_getBlockByNumber&tag={hex(block_number)}&boolean=true&apikey={API_KEY}')
    data = response.json()
    if len(data['result']['transactions']) > 0:
        first_transaction = data['result']['transactions'][0]
        return first_transaction
    else:
        return None

def main():
    try:
        latest_block_number = get_latest_block_number()
        print(f'Latest Block Number: {latest_block_number}')

        transaction_count = get_block_transaction_count(latest_block_number)
        print(f'Transaction Count in Block {latest_block_number}: {transaction_count}')

        if transaction_count > 0:
            first_transaction = get_first_transaction_in_block(latest_block_number)
            print('First Transaction in Block:')
            print(json.dumps(first_transaction, indent=4))
        else:
            print('No transactions in the latest block.')

    except KeyboardInterrupt:
        print('Script stopped by user.')
        sys.exit(0)

if __name__ == "__main__":
    main()
