# check_latest.py

import pandas as pd
import os
from fetch_config import FetchConfig

def check_latest_data(commodity: str, config: FetchConfig):
    """Show all contracts and their data for the most recent date"""
    commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
    
    try:
        # Load prices and volumes
        prices_df = pd.read_parquet(os.path.join(commodity_path, 'prices.parquet'))
        volumes_df = pd.read_parquet(os.path.join(commodity_path, 'volumes.parquet'))
        
        # Get last date
        last_date = prices_df.index.max()
        print(f"\nData for {commodity} as of {last_date.date()}")
        print("=" * 50)
        
        # Get data for last date
        latest_prices = prices_df.loc[last_date]
        latest_volumes = volumes_df.loc[last_date]
        
        # Combine into a DataFrame
        latest_data = pd.DataFrame({
            'Price': latest_prices,
            'Volume': latest_volumes
        })
        
        # Remove rows where both price and volume are NaN
        latest_data = latest_data.dropna(how='all').sort_index()
        
        # Add month and year columns for better understanding
        latest_data['Month'] = latest_data.index.str[2]
        latest_data['Year'] = latest_data.index.str[3:5]
        
        # Print all data
        print(f"\nFound {len(latest_data)} contracts:")
        pd.set_option('display.max_rows', None)
        print(latest_data)
        
        # Print some statistics
        print("\nSummary:")
        print(f"Number of contracts with prices: {latest_data['Price'].notna().sum()}")
        print(f"Number of contracts with volumes: {latest_data['Volume'].notna().sum()}")
        
        # Extract furthest contract date
        if not latest_data.empty:
            furthest_contract = latest_data.index[-1]
            print(f"Furthest contract: {furthest_contract}")
        
    except Exception as e:
        print(f"Error checking data: {e}")

def main():
    config = FetchConfig(
        BASE_PATH=os.getcwd()
    )
    
    commodity = 'HG'
    check_latest_data(commodity, config)

if __name__ == "__main__":
    main()