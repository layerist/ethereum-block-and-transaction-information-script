### Ethereum Block and Transaction Information Script

This script interacts with the Etherscan API to retrieve and display information about the latest block and its transactions on the Ethereum blockchain. Specifically, it performs the following tasks:

1. Fetches the latest block number.
2. Retrieves the number of transactions in the latest block.
3. Obtains and displays information about the first transaction in the latest block.

#### Prerequisites

- Python 3.x
- `requests` library

You can install the `requests` library using pip:

```bash
pip install requests
```

#### Setup

1. Obtain an API key from [Etherscan](https://etherscan.io/apis).
2. Replace the placeholder `'YOUR_API_KEY_HERE'` in the script with your actual Etherscan API key.

#### Usage

Run the script using Python:

```bash
python ethereum_info.py
```

The script will output the latest block number, the number of transactions in that block, and details of the first transaction. It can be stopped at any time with a keyboard interrupt (Ctrl+C).

#### Example Output

```
Latest Block Number: 12345678
Transaction Count in Block 12345678: 10
First Transaction in Block:
{
    "blockHash": "0x...",
    "blockNumber": "0x...",
    "from": "0x...",
    "gas": "0x...",
    "gasPrice": "0x...",
    "hash": "0x...",
    "input": "0x...",
    "nonce": "0x...",
    "to": "0x...",
    "transactionIndex": "0x...",
    "value": "0x...",
    "v": "0x...",
    "r": "0x...",
    "s": "0x..."
}
```

#### Error Handling

If there are no transactions in the latest block, the script will inform you accordingly.

#### Stopping the Script

To stop the script, use the keyboard interrupt (Ctrl+C). The script will handle the interruption gracefully and exit.
