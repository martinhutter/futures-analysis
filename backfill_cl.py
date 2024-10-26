# backfill_cl.py

import pandas as pd
import numpy as np
import blpapi
import os
import math
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from fetch_config import FetchConfig
from data_fetcher import (start_bloomberg_session, generate_futures_tickers, 
                         fetch_metadata_batch, fetch_price_volume_batch,
                         process_price_volume_data, save_commodity_data)

def fetch_full_history(commodity: str, config: FetchConfig):
    """Force fetch complete historical data ignoring existing data"""
    print(f"\nForcing full historical fetch for {commodity}...")
    print(f"Target date range: {config.START_YEAR} to {datetime.now().year}")
    
    session = start_bloomberg_session(config)
    try:
        # Generate all possible tickers
        tickers = generate_futures_tickers(commodity, config)
        print(f"Generated {len(tickers)} tickers")
        
        # Fetch metadata
        print("\nFetching metadata...")
        metadata = {}
        batches = np.array_split(tickers, math.ceil(len(tickers)/config.BATCH_SIZE))
        
        for i, batch in enumerate(batches, 1):
            print(f"Processing metadata batch {i}/{len(batches)}")
            batch_metadata = fetch_metadata_batch(session, batch)
            metadata.update(batch_metadata)
        
        print(f"Fetched metadata for {len(metadata)} contracts")
        
        # Fetch full price history
        print("\nFetching complete price history...")
        start_date = datetime(config.START_YEAR, 1, 1)
        end_date = datetime.now()
        
        raw_data = pd.DataFrame()
        for i, batch in enumerate(batches, 1):
            print(f"Processing price/volume batch {i}/{len(batches)}")
            batch_data = fetch_price_volume_batch(
                session=session,
                securities_batch=batch,
                fields=config.DEFAULT_FIELDS,
                start_date=start_date,
                end_date=end_date
            )
            
            if not batch_data.empty:
                if raw_data.empty:
                    raw_data = batch_data
                else:
                    new_cols = [col for col in batch_data.columns if col not in raw_data.columns]
                    if new_cols:
                        raw_data = pd.concat([raw_data, batch_data[new_cols]], axis=1)
            
            print(f"Batch {i} complete. Current data shape: {raw_data.shape}")
        
        if not raw_data.empty:
            # Process into prices and volumes
            print("\nProcessing raw data into prices and volumes...")
            prices_df, volumes_df = process_price_volume_data(raw_data)
            
            print(f"\nFetched data summary:")
            print(f"Date range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
            print(f"Number of contracts: {len(prices_df.columns)}")
            print(f"Number of trading days: {len(prices_df)}")
            
            # Save the data
            save_commodity_data(commodity, prices_df, volumes_df, metadata, config)
            print(f"\nSuccessfully saved complete historical data")
            
            return prices_df, volumes_df, metadata
        else:
            print("No data retrieved")
            return None, None, None
            
    finally:
        session.stop()

def main():
    print("Starting forced CL historical backfill...")
    
    # Setup config
    config = FetchConfig(
        BASE_PATH=os.getcwd(),
        START_YEAR=1985,
        MIN_FORWARD_YEARS=2,
        BATCH_SIZE=50,
        COMMODITIES=['CL']
    )
    
    # Create backup path
    backup_path = None
    cl_path = os.path.join(config.RAW_DATA_PATH, 'CL')
    if os.path.exists(cl_path):
        backup_path = f"{cl_path}_backup_{datetime.now():%Y%m%d_%H%M%S}"
        os.rename(cl_path, backup_path)
        print(f"Backed up existing data to {backup_path}")
    
    # Fetch complete history
    try:
        prices_df, volumes_df, metadata = fetch_full_history('CL', config)
        
        if prices_df is not None:
            print("\nBackfill successful!")
            print(f"Total date range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
            print(f"Total trading days: {len(prices_df):,}")
            print(f"Total contracts: {len(prices_df.columns)}")
            
            # If everything worked, we can optionally delete the backup
            user_input = input("\nBackfill successful. Delete backup? (y/n): ")
            if user_input.lower() == 'y' and backup_path and os.path.exists(backup_path):
                os.rmdir(backup_path)
                print("Backup deleted")
            else:
                print(f"Backup retained at: {backup_path}")
                
    except Exception as e:
        print(f"Error during backfill: {e}")
        
        # Restore backup if something went wrong
        if backup_path and os.path.exists(backup_path):
            if os.path.exists(cl_path):
                os.remove(cl_path)
            os.rename(backup_path, cl_path)
            print("Restored backup due to error")

if __name__ == "__main__":
    main()