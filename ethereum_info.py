#!/usr/bin/env python3
import argparse
import atexit
import json
import logging
import os
import sys
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
ETHERSCAN_API_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10  # seconds
ETHERSCAN_MODULE = "proxy"
ETHERSCAN_ACTIONS = {
    "latest_block": "eth_blockNumber",
    "tx_count": "eth_getBlockTransactionCountByNumber",
    "block_by_number": "eth_getBlockByNumber",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


class EtherscanAPIError(Exception):
    """Raised when Etherscan returns an error status."""


def get_api_key() -> str:
    key = os.getenv("ETHERSCAN_API_KEY")
    if not key:
        logging.critical("Environment variable ETHERSCAN_API_KEY is missing.")
        sys.exit(1)
    return key


def make_session(retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_etherscan(
    session: requests.Session, params: Dict[str, str], api_key: str
) -> Dict[str, Any]:
    params["apikey"] = api_key
    try:
        resp = session.get(ETHERSCAN_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logging.error(f"Network/API request error: {e}")
        raise

    if data.get("status") == "0":
        msg = data.get("message", "Unknown error")
        logging.error(f"Etherscan API error: {msg}")
        raise EtherscanAPIError(msg)

    return data


def get_latest_block(session: requests.Session, api_key: str) -> int:
    params = {"module": ETHERSCAN_MODULE, "action": ETHERSCAN_ACTIONS["latest_block"]}
    data = fetch_etherscan(session, params, api_key)
    hexnum = data["result"]
    return int(hexnum, 16)


def get_tx_count(session: requests.Session, block_number: int, api_key: str) -> int:
    params = {
        "module": ETHERSCAN_MODULE,
        "action": ETHERSCAN_ACTIONS["tx_count"],
        "tag": hex(block_number),
    }
    data = fetch_etherscan(session, params, api_key)
    return int(data["result"], 16)


def get_first_tx(session: requests.Session, block_number: int, api_key: str) -> Optional[Dict[str, Any]]:
    params = {
        "module": ETHERSCAN_MODULE,
        "action": ETHERSCAN_ACTIONS["block_by_number"],
        "tag": hex(block_number),
        "boolean": "true",
    }
    data = fetch_etherscan(session, params, api_key)
    txs = data["result"].get("transactions", [])
    return txs[0] if txs else None


def shutdown_handler() -> None:
    logging.info("Exiting cleanly.")


def main() -> None:
    atexit.register(shutdown_handler)

    parser = argparse.ArgumentParser(description="Ethereum block inspector via Etherscan API")
    parser.add_argument("--first-tx", action="store_true", help="Fetch the first transaction in the latest block")
    args = parser.parse_args()

    api_key = get_api_key()
    session = make_session()

    try:
        block_num = get_latest_block(session, api_key)
    except Exception:
        logging.critical("Could not retrieve latest block.")
        sys.exit(1)

    logging.info(f"Latest block: {block_num}")

    try:
        tx_count = get_tx_count(session, block_num, api_key)
    except Exception:
        logging.critical(f"Could not retrieve transaction count for block {block_num}.")
        sys.exit(1)

    logging.info(f"Transactions in block {block_num}: {tx_count}")

    if args.first_tx:
        if tx_count == 0:
            logging.info("No transactions in the latest block.")
            return
        try:
            tx = get_first_tx(session, block_num, api_key)
            if tx:
                print(json.dumps(tx, indent=4))
            else:
                logging.info("Block contains no transactions.")
        except Exception:
            logging.error(f"Failed to fetch first transaction from block {block_num}.")


if __name__ == "__main__":
    main()
