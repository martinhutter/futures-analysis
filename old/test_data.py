import os
from datetime import datetime
from fetch_config import FetchConfig
from data_fetcher import fetch_commodity_data

def test_hg_fetch():
    print("Starting HG (Copper) futures test...")
    
    # Setup config - note we now use MIN_FORWARD_YEARS instead of END_YEAR
    config = FetchConfig(
        BASE_PATH=os.getcwd(),
        START_YEAR=2000,
        MIN_FORWARD_YEARS=2,  # This will automatically set END_YEAR to current_year + 2
        COMMODITIES=['HG'],
        LOOKBACK_DAYS=5
    )
    
    try:
        # Fetch/update data
        print("\nUpdating HG futures data...")
        prices_df, volumes_df, metadata = fetch_commodity_data('HG', config)
        
        if prices_df is not None:
            print("\nData update successful!")
            print(f"Date range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
            
            # Show latest contracts
            last_date = prices_df.index.max()
            active_contracts = prices_df.loc[last_date].dropna()
            
            print(f"\nActive contracts on {last_date.date()}: {len(active_contracts)}")
            print("\nContracts available:")
            for contract in sorted(active_contracts.index):
                price = active_contracts[contract]
                volume = volumes_df.loc[last_date, contract] if contract in volumes_df.columns else 0
                print(f"{contract:<12} Price: {price:>8.2f}  Volume: {volume:>8.0f}")
            
            print(f"\nFurthest contract: {sorted(active_contracts.index)[-1]}")
            
        else:
            print("No data retrieved for HG")
            
    except Exception as e:
        print(f"Error during test: {str(e)}")

if __name__ == "__main__":
    test_hg_fetch()