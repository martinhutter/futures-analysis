# fetch_commodities.py

import os
from datetime import datetime
from fetch_config import FetchConfig
from data_fetcher import fetch_commodity_data
import pandas as pd

def fetch_all_commodities():
    """Fetch data for all specified commodities"""
    # Configure with all commodities we want
    config = FetchConfig(
        BASE_PATH=os.getcwd(),
        START_YEAR=1985,
        MIN_FORWARD_YEARS=2,
        COMMODITIES=[
            'CL',  # Crude Oil
            'CO',  # Brent
            'HO',  # Heating Oil
            'XB',  # RBOB Gasoline
            'NG',  # Natural Gas
            'HG',  # Copper
            'GC',  # Gold
            'SI'   # Silver
            'HG',  # Copper
        ]
    )
    
    results = {}
    for commodity in config.COMMODITIES:
        print(f"\n{'='*50}")
        print(f"Processing {commodity}")
        print(f"{'='*50}")
        
        try:
            prices_df, volumes_df, metadata = fetch_commodity_data(commodity, config)
            
            if prices_df is not None and not prices_df.empty:
                # Get latest data summary
                last_date = prices_df.index.max()
                active_contracts = prices_df.loc[last_date].dropna()
                
                results[commodity] = {
                    'success': True,
                    'last_date': last_date,
                    'contracts_count': len(active_contracts),
                    'date_range': f"{prices_df.index.min().date()} to {prices_df.index.max().date()}",
                    'furthest_contract': sorted(active_contracts.index)[-1] if len(active_contracts) > 0 else None
                }
                
                # Show immediate feedback
                print(f"\nSuccessfully processed {commodity}:")
                print(f"Latest date: {last_date.date()}")
                print(f"Active contracts: {len(active_contracts)}")
                print(f"Furthest contract: {results[commodity]['furthest_contract']}")
                
            else:
                results[commodity] = {
                    'success': False,
                    'error': 'No data retrieved'
                }
                print(f"No data retrieved for {commodity}")
                
        except Exception as e:
            results[commodity] = {
                'success': False,
                'error': str(e)
            }
            print(f"Error processing {commodity}: {e}")
    
    # Print final summary
    print("\nFinal Processing Summary:")
    print("=" * 80)
    print(f"{'Commodity':<10} {'Status':<10} {'Last Date':<12} {'Active Contracts':<17} {'Furthest Contract'}")
    print("-" * 80)
    
    for commodity, result in results.items():
        if result['success']:
            print(f"{commodity:<10} {'✓':<10} {result['last_date'].date()!s:<12} {result['contracts_count']:<17} {result['furthest_contract']}")
        else:
            print(f"{commodity:<10} {'✗':<10} Error: {result['error']}")
    
    return results

def analyze_coverage():
    """Analyze contract coverage for all commodities"""
    config = FetchConfig(BASE_PATH=os.getcwd())
    
    print("\nContract Coverage Analysis:")
    print("=" * 80)
    
    for commodity in config.COMMODITIES:
        try:
            commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
            prices_path = os.path.join(commodity_path, 'prices.parquet')
            
            if os.path.exists(prices_path):
                prices_df = pd.read_parquet(prices_path)
                last_date = prices_df.index.max()
                active_contracts = prices_df.loc[last_date].dropna()
                
                print(f"\n{commodity} Contracts as of {last_date.date()}:")
                print("-" * 40)
                for contract in sorted(active_contracts.index):
                    price = active_contracts[contract]
                    print(f"{contract:<12} {price:>10.2f}")
                    
        except Exception as e:
            print(f"Error analyzing {commodity}: {e}")

def main():
    print("Starting multi-commodity data fetch...")
    results = fetch_all_commodities()
    
    # Analyze coverage if requested
    user_input = input("\nWould you like to see detailed contract coverage? (y/n): ")
    if user_input.lower() == 'y':
        analyze_coverage()

if __name__ == "__main__":
    main()