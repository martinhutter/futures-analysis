import os
from datetime import datetime
from fetch_config import FetchConfig
from spreads_config import SpreadsConfig
from spreads_calculator import create_monthly_futures_data, save_spread_data
import pandas as pd
import json
from tqdm import tqdm  # For progress bars

def calculate_spreads_for_all():
    """Calculate spreads for all commodities with available data"""
    # Setup configs
    fetch_config = FetchConfig(BASE_PATH=os.getcwd())
    spreads_config = SpreadsConfig(BASE_PATH=os.getcwd())
    
    results = {}
    total_commodities = len(fetch_config.COMMODITIES)
    
    print(f"\nProcessing spreads for {total_commodities} commodities...")
    
    for i, commodity in enumerate(fetch_config.COMMODITIES, 1):
        print(f"\n[{i}/{total_commodities}] Processing {commodity}")
        print("=" * 50)
        
        try:
            # Load raw data
            commodity_path = os.path.join(fetch_config.RAW_DATA_PATH, commodity)
            prices_path = os.path.join(commodity_path, 'prices.parquet')
            metadata_path = os.path.join(commodity_path, 'metadata.json')
            
            if not os.path.exists(prices_path):
                print(f"❌ No price data found for {commodity}")
                results[commodity] = {'success': False, 'error': 'No price data found'}
                continue
                
            # Load data
            print(f"Loading data for {commodity}...")
            prices_df = pd.read_parquet(prices_path)
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                
            print(f"✓ Loaded data: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
            print(f"✓ Number of contracts: {len(prices_df.columns)}")
            
            # Calculate spreads
            print(f"Calculating spreads for {commodity}...")
            spread_data = create_monthly_futures_data(
                prices_df=prices_df,
                metadata=metadata,
                config=spreads_config
            )
            
            # Save results
            print(f"Saving spread calculations for {commodity}...")
            save_spread_data(commodity, spread_data, spreads_config)
            
            # Unpack and analyze results
            monthly_futures, spreads_dollar, spreads_percent, spreads_annual, _ = spread_data
            last_date = spreads_dollar.index.max()
            
            results[commodity] = {
                'success': True,
                'last_date': last_date,
                'spreads_calculated': {
                    'dollar': len(spreads_dollar.columns),
                    'percent': len(spreads_percent.columns),
                    'annual': len(spreads_annual.columns)
                }
            }
            
            # Show spread summary
            print(f"\nResults for {commodity}:")
            print(f"✓ Dollar spreads: {len(spreads_dollar.columns)}")
            print(f"✓ Percentage spreads: {len(spreads_percent.columns)}")
            print(f"✓ Annualized spreads: {len(spreads_annual.columns)}")
            
            # Show latest spreads
            print(f"\nLatest spreads for {commodity} ({last_date.date()}):")
            latest_spreads = pd.DataFrame({
                'Dollar': spreads_dollar.loc[last_date],
                'Percent': spreads_percent.loc[last_date],
                'Annual': spreads_annual.loc[last_date]
            })
            print(latest_spreads.round(4))
            
            print(f"\n✓ Successfully processed {commodity}")
            
        except Exception as e:
            print(f"❌ Error processing {commodity}: {e}")
            results[commodity] = {'success': False, 'error': str(e)}
            
        print(f"Progress: {i}/{total_commodities} commodities processed")
    
    # Print final summary
    print("\nFinal Processing Summary")
    print("=" * 80)
    print(f"{'Commodity':<10} {'Status':<10} {'Last Date':<12} {'Spreads Calculated':<20}")
    print("-" * 80)
    
    successful = 0
    for commodity, result in results.items():
        if result['success']:
            successful += 1
            spreads_count = sum(result['spreads_calculated'].values())
            print(f"{commodity:<10} {'✓':<10} {result['last_date'].date()!s:<12} {spreads_count:<20}")
        else:
            print(f"{commodity:<10} {'❌':<10} Error: {result['error']}")
    
    print("\nProcessing Complete!")
    print(f"Successfully processed: {successful}/{total_commodities} commodities")
    print(f"Failed: {total_commodities - successful}/{total_commodities} commodities")

def main():
    start_time = datetime.now()
    print(f"Starting spread calculations at {start_time.strftime('%H:%M:%S')}")
    calculate_spreads_for_all()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\nTotal processing time: {duration}")

if __name__ == "__main__":
    main()