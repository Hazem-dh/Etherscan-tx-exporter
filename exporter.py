import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

# loading env variables
load_dotenv()

# Replace with your API key from Etherscan
api_key = os.getenv("ETHERSCAN_API_KEY")

# Wallet address to extract data from
wallet_address = '0xF5C9F957705bea56a7e806943f98F7777B995826'


# Function to fetch transactions in batches
def fetch_transactions(address, start_block, end_block):
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key} "
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == '1':
            return data['result']
    return None


# Function to extract data from wallet
def extract_wallet_data(address, start_block, end_block):
    all_transactions = []
    while True:
        transactions = fetch_transactions(address, start_block, end_block)
        if transactions is None or len(transactions) == 0:
            break
        all_transactions.extend(transactions)
        start_block = int(transactions[-1]['blockNumber']) + 1  # Set start block for next batch
        time.sleep(0.3)  # Add a small delay to avoid hitting rate limits
    return all_transactions


# Main function
def main():
    # random blocks
    start_block = 16308190
    end_block = 16408190
    # extract data
    wallet_data = extract_wallet_data(wallet_address, start_block, end_block)
    if wallet_data:
        df = pd.DataFrame(wallet_data)
        df.to_csv('Wallet etherscan.csv', index=False)
        print("Exported data successfully")
    else:
        print("Failed to fetch wallet data")


if __name__ == "__main__":
    main()
