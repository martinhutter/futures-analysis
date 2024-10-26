import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import json
from typing import Tuple, Dict
from fetch_config import FetchConfig

def load_commodity_data(commodity: str, config: FetchConfig) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """Load saved data for a commodity"""
    commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
    
    try:
        # Load prices and volumes
        prices_df = pd.read_parquet(os.path.join(commodity_path, 'prices.parquet'))
        volumes_df = pd.read_parquet(os.path.join(commodity_path, 'volumes.parquet'))
        
        # Load metadata
        with open(os.path.join(commodity_path, 'metadata.json'), 'r') as f:
            metadata = json.load(f)
            
        return prices_df, volumes_df, metadata
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None, None

def analyze_data(prices_df: pd.DataFrame, volumes_df: pd.DataFrame, metadata: Dict):
    """Analyze the loaded data"""
    print("\nData Analysis:")
    print("=" * 50)
    
    # Basic info
    print("\nDate Range:")
    print(f"Start: {prices_df.index.min().date()}")
    print(f"End: {prices_df.index.max().date()}")
    print(f"Trading days: {len(prices_df):,}")
    
    # Contract analysis
    print("\nContracts:")
    print(f"Total contracts: {len(prices_df.columns)}")
    
    # Latest data
    last_date = prices_df.index.max()
    active_contracts = prices_df.loc[last_date].dropna()
    print(f"\nActive contracts on {last_date.date()}: {len(active_contracts)}")
    print("\nLatest prices:")
    print(active_contracts.sort_index())
    
    # Data quality
    print("\nData Quality:")
    completeness = (1 - prices_df.isna().mean()) * 100
    print("\nCompleteness by contract (top 10 most complete):")
    print(completeness.sort_values(ascending=False).head(10))
    
    print("\nCompleteness by contract (bottom 10 least complete):")
    print(completeness.sort_values(ascending=False).tail(10))
    
    # Volume analysis
    print("\nVolume Analysis:")
    latest_volumes = volumes_df.loc[last_date].dropna()
    print("\nLatest volumes:")
    print(latest_volumes.sort_values(ascending=False).head(10))

def plot_data(prices_df: pd.DataFrame, volumes_df: pd.DataFrame, commodity: str):
    """Create visualizations of the data"""
    # 1. Front month price history
    plt.figure(figsize=(15, 7))
    front_month_prices = pd.DataFrame(index=prices_df.index)
    
    for date in prices_df.index:
        # Get the first non-NaN price for each date
        valid_prices = prices_df.loc[date].dropna()
        if not valid_prices.empty:
            front_month_prices.loc[date, 'price'] = valid_prices.iloc[0]
    
    plt.plot(front_month_prices.index, front_month_prices['price'])
    plt.title(f'{commodity} Front Month Price History')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    
    # 2. Latest term structure
    plt.figure(figsize=(15, 7))
    last_date = prices_df.index.max()
    term_structure = prices_df.loc[last_date].dropna()
    
    plt.plot(range(len(term_structure)), term_structure.values, marker='o')
    plt.title(f'{commodity} Term Structure on {last_date.date()}')
    plt.xlabel('Contract Number')
    plt.ylabel('Price')
    plt.grid(True)
    plt.xticks(range(len(term_structure)), term_structure.index, rotation=45)
    plt.tight_layout()
    plt.show()
    
    # 3. Data availability heatmap
    plt.figure(figsize=(15, 7))
    availability = (~prices_df.isna()).astype(int)
    plt.imshow(availability.T, aspect='auto', cmap='YlOrRd')
    plt.colorbar(label='Data Available')
    plt.title(f'{commodity} Data Availability')
    plt.xlabel('Date Index')
    plt.ylabel('Contract')
    plt.tight_layout()
    plt.show()

def main():
    """Main function to check data"""
    config = FetchConfig(
        BASE_PATH=os.getcwd()
    )
    
    commodity = 'HG'  # Change this to check other commodities
    
    # Load the data
    prices_df, volumes_df, metadata = load_commodity_data(commodity, config)
    
    if prices_df is not None:
        # Run analysis
        analyze_data(prices_df, volumes_df, metadata)
        
        # Create plots
        plot_data(prices_df, volumes_df, commodity)
    else:
        print(f"No data found for {commodity}")

if __name__ == "__main__":
    main()