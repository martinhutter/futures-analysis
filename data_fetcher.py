import blpapi
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from typing import List, Dict, Optional, Tuple
import math
from fetch_config import FetchConfig

def verify_data_integrity(prices_df: pd.DataFrame, volumes_df: pd.DataFrame) -> bool:
    """Verify data integrity after fetch/update"""
    try:
        # Check basic integrity
        if prices_df.empty or volumes_df.empty:
            print("Empty dataframes found")
            return False
            
        # Check index alignment
        if not (prices_df.index == volumes_df.index).all():
            print("Index mismatch between prices and volumes")
            return False
            
        # Check for complete missingness
        if prices_df.isna().all().all() or volumes_df.isna().all().all():
            print("Complete data missingness found")
            return False
            
        # Check for suspicious patterns
        zero_price_pct = (prices_df == 0).sum().sum() / prices_df.size
        if zero_price_pct > 0.01:  # More than 1% zeros
            print(f"Warning: High percentage of zero prices found ({zero_price_pct:.2%})")
            
        return True
        
    except Exception as e:
        print(f"Error during data integrity check: {e}")
        return False

def start_bloomberg_session(config: FetchConfig) -> blpapi.Session:
    """Initialize Bloomberg API session"""
    session_options = blpapi.SessionOptions()
    session_options.setServerHost(config.BLOOMBERG_HOST)
    session_options.setServerPort(config.BLOOMBERG_PORT)
    session = blpapi.Session(session_options)
    
    if not session.start():
        raise Exception("Failed to start session.")
    if not session.openService("//blp/refdata"):
        raise Exception("Failed to open service")
    return session

def check_existing_data(commodity: str, config: FetchConfig) -> Tuple[Optional[datetime], Optional[Dict]]:
    """Check if we have existing data and return the last date and metadata"""
    commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
    prices_path = os.path.join(commodity_path, 'prices.parquet')
    
    if os.path.exists(prices_path):
        try:
            prices_df = pd.read_parquet(prices_path)
            last_date = prices_df.index.max()
            
            metadata_path = os.path.join(commodity_path, 'metadata.json')
            existing_metadata = {}
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    existing_metadata = json.load(f)
            
            print(f"Found existing data for {commodity} up to {last_date.date()}")
            print(f"Number of contracts: {len(prices_df.columns)}")
            return last_date, existing_metadata
            
        except Exception as e:
            print(f"Error reading existing data: {e}")
    
    print(f"No existing data found for {commodity}")
    return None, None

def generate_futures_tickers(commodity: str, config: FetchConfig) -> List[str]:
    """Generate futures tickers for specified date range"""
    tickers = []
    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)[-2:]
        for month in config.MONTHS:
            tickers.append(f"{commodity}{month}{year_str} Comdty")
    print(f"Generated {len(tickers)} tickers for {commodity}")
    return tickers

def fetch_metadata(session: blpapi.Session, tickers: List[str], 
                  config: FetchConfig) -> Dict:
    """Fetch metadata for all tickers in batches"""
    metadata = {}
    batches = np.array_split(tickers, math.ceil(len(tickers)/config.BATCH_SIZE))
    
    for i, batch in enumerate(batches, 1):
        print(f"Processing metadata batch {i}/{len(batches)}")
        batch_metadata = fetch_metadata_batch(session, batch)
        metadata.update(batch_metadata)
    
    return metadata

def fetch_metadata_batch(session: blpapi.Session, securities_batch: List[str]) -> Dict:
    """Fetch metadata for a batch of securities"""
    refDataService = session.getService("//blp/refdata")
    metadata = {}
    
    try:
        request = refDataService.createRequest("ReferenceDataRequest")
        for security in securities_batch:
            request.append("securities", security)
        request.append("fields", "name")
        request.append("fields", "QUOTE_UNITS")
        request.append("fields", "LAST_TRADEABLE_DT")
        
        session.sendRequest(request)
        end_reached = False
        
        while not end_reached:
            event = session.nextEvent(500)
            if event.eventType() in [blpapi.Event.PARTIAL_RESPONSE, 
                                   blpapi.Event.RESPONSE]:
                for msg in event:
                    securityDataArray = msg.getElement("securityData")
                    for i in range(securityDataArray.numValues()):
                        securityData = securityDataArray.getValueAsElement(i)
                        ticker = securityData.getElementAsString("security")
                        fieldData = securityData.getElement("fieldData")
                        
                        metadata[ticker] = {
                            'name': fieldData.getElementAsString("name") if fieldData.hasElement("name") else ticker,
                            'units': fieldData.getElementAsString("QUOTE_UNITS") if fieldData.hasElement("QUOTE_UNITS") else '',
                            'last_trade_date': fieldData.getElementAsString("LAST_TRADEABLE_DT") if fieldData.hasElement("LAST_TRADEABLE_DT") else ''
                        }
            
            if event.eventType() == blpapi.Event.RESPONSE:
                end_reached = True
                
    except Exception as e:
        print(f"Error fetching metadata batch: {e}")
        
    return metadata

def fetch_price_volume_data(session: blpapi.Session, tickers: List[str],
                          start_date: datetime, end_date: datetime,
                          config: FetchConfig) -> pd.DataFrame:
    """Fetch price and volume data for all tickers in batches"""
    all_data = pd.DataFrame()
    batches = np.array_split(tickers, math.ceil(len(tickers)/config.BATCH_SIZE))
    
    for i, batch in enumerate(batches, 1):
        print(f"Processing price/volume batch {i}/{len(batches)}")
        batch_data = fetch_price_volume_batch(
            session, batch, config.DEFAULT_FIELDS, start_date, end_date)
        
        if not batch_data.empty:
            if all_data.empty:
                all_data = batch_data
            else:
                new_cols = [col for col in batch_data.columns if col not in all_data.columns]
                if new_cols:
                    all_data = pd.concat([all_data, batch_data[new_cols]], axis=1)
    
    return all_data

def fetch_price_volume_batch(session: blpapi.Session, securities_batch: List[str],
                           fields: List[str], start_date: datetime, 
                           end_date: datetime) -> pd.DataFrame:
    """Fetch historical data for a batch of securities"""
    refDataService = session.getService("//blp/refdata")
    
    request = refDataService.createRequest("HistoricalDataRequest")
    for security in securities_batch:
        request.append("securities", security)
    for field in fields:
        request.append("fields", field)
    
    request.set("startDate", start_date.strftime("%Y%m%d"))
    request.set("endDate", end_date.strftime("%Y%m%d"))
    request.set("periodicityAdjustment", "ACTUAL")
    request.set("periodicitySelection", "DAILY")
    
    data_dict = {}
    
    try:
        session.sendRequest(request)
        end_reached = False
        
        while not end_reached:
            event = session.nextEvent(500)
            if event.eventType() in [blpapi.Event.PARTIAL_RESPONSE, 
                                   blpapi.Event.RESPONSE]:
                for msg in event:
                    securityData = msg.getElement("securityData")
                    security_name = securityData.getElementAsString("security")
                    fieldDataArray = securityData.getElement("fieldData")
                    
                    for i in range(fieldDataArray.numValues()):
                        fieldData = fieldDataArray.getValueAsElement(i)
                        date = fieldData.getElementAsDatetime("date")
                        
                        if date not in data_dict:
                            data_dict[date] = {}
                        
                        for field in fields:
                            if fieldData.hasElement(field):
                                data_dict[date][f"{security_name}_{field}"] = fieldData.getElementAsFloat(field)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                end_reached = True
                
    except Exception as e:
        print(f"Error fetching batch data: {e}")
        return pd.DataFrame()
    
    if data_dict:
        df = pd.DataFrame.from_dict(data_dict, orient='index')
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        return df
    
    return pd.DataFrame()

def process_price_volume_data(raw_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split raw data into separate price and volume dataframes"""
    price_cols = [col for col in raw_data.columns if 'PX_LAST' in col]
    volume_cols = [col for col in raw_data.columns if 'PX_VOLUME' in col]
    
    print(f"Found {len(price_cols)} price series and {len(volume_cols)} volume series")
    
    # Create separate dataframes
    prices_df = raw_data[price_cols].copy()
    volumes_df = raw_data[volume_cols].copy()
    
    # Clean up column names
    prices_df.columns = [col.replace('_PX_LAST', '') for col in prices_df.columns]
    volumes_df.columns = [col.replace('_PX_VOLUME', '') for col in volumes_df.columns]
    
    # Sort columns
    prices_df = prices_df.reindex(sorted(prices_df.columns), axis=1)
    volumes_df = volumes_df.reindex(sorted(volumes_df.columns), axis=1)
    
    return prices_df, volumes_df

def merge_with_existing(commodity: str, new_prices: pd.DataFrame, new_volumes: pd.DataFrame, 
                       last_date: datetime, config: FetchConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Merge new data with existing data"""
    print(f"Merging new data with existing data...")
    commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
    
    # Load existing data
    existing_prices = pd.read_parquet(os.path.join(commodity_path, 'prices.parquet'))
    existing_volumes = pd.read_parquet(os.path.join(commodity_path, 'volumes.parquet'))
    
    # For the overlap period, prefer new data
    cutoff_date = last_date - timedelta(days=config.LOOKBACK_DAYS)
    
    # Combine old and new data
    merged_prices = pd.concat([
        existing_prices[existing_prices.index <= cutoff_date],
        new_prices[new_prices.index > cutoff_date]
    ]).sort_index()
    
    merged_volumes = pd.concat([
        existing_volumes[existing_volumes.index <= cutoff_date],
        new_volumes[new_volumes.index > cutoff_date]
    ]).sort_index()
    
    # Check for new contracts
    new_contracts = set(new_prices.columns) - set(existing_prices.columns)
    if new_contracts:
        print(f"Found {len(new_contracts)} new contracts")
    
    return merged_prices, merged_volumes

def save_commodity_data(commodity: str, prices_df: pd.DataFrame, 
                       volumes_df: pd.DataFrame, metadata: Dict, 
                       config: FetchConfig):
    """Save commodity data to disk"""
    commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
    os.makedirs(commodity_path, exist_ok=True)
    
    # Save data files
    prices_df.to_parquet(os.path.join(commodity_path, 'prices.parquet'))
    volumes_df.to_parquet(os.path.join(commodity_path, 'volumes.parquet'))
    
    # Save metadata
    with open(os.path.join(commodity_path, 'metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Save config/info
    with open(os.path.join(commodity_path, 'config.json'), 'w') as f:
        json.dump({
            'last_update': datetime.now().isoformat(),
            'start_date': prices_df.index.min().isoformat(),
            'end_date': prices_df.index.max().isoformat(),
            'number_of_contracts': len(prices_df.columns),
            'data_fields': config.DEFAULT_FIELDS,
            'data_statistics': {
                'trading_days': len(prices_df),
                'completeness': float(1 - prices_df.isna().mean().mean()),
                'contracts_count': len(prices_df.columns)
            }
        }, f, indent=2)
    
    print(f"Data saved to {commodity_path}")

def fetch_commodity_data(commodity: str, config: FetchConfig) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Main function to fetch/update commodity data.
    Only fetches new data if existing data is found.
    """
    print(f"\nProcessing {commodity}...")
    
    # Check existing data
    last_date, existing_metadata = check_existing_data(commodity, config)
    
    # Determine date range
    end_date = datetime.now()
    if last_date is not None:
        start_date = last_date - timedelta(days=config.LOOKBACK_DAYS)
        print(f"Will update data from {start_date.date()} to {end_date.date()}")
    else:
        start_date = datetime(config.START_YEAR, 1, 1)
        print(f"Will fetch full history from {start_date.date()} to {end_date.date()}")
    
    session = start_bloomberg_session(config)
    try:
        # Generate tickers
        tickers = generate_futures_tickers(commodity, config)
        
        # Fetch metadata only for new contracts
        if existing_metadata:
            new_tickers = [t for t in tickers if t not in existing_metadata]
            if new_tickers:
                print(f"Fetching metadata for {len(new_tickers)} new contracts...")
                new_metadata = fetch_metadata(session, new_tickers, config)
                existing_metadata.update(new_metadata)
            metadata = existing_metadata
        else:
            print("Fetching metadata for all contracts...")
            metadata = fetch_metadata(session, tickers, config)
        
        # Fetch price and volume data
        print("Fetching price and volume data...")
        raw_data = fetch_price_volume_data(session, tickers, start_date, end_date, config)
        
        if raw_data.empty:
            print("No new data retrieved")
            return pd.DataFrame(), pd.DataFrame(), metadata
        
        # Process into prices and volumes
        prices_df, volumes_df = process_price_volume_data(raw_data)
        
        # If we have existing data, merge with new data
        if last_date is not None:
            prices_df, volumes_df = merge_with_existing(
                commodity, prices_df, volumes_df, last_date, config)
        
        # Verify data integrity
        if verify_data_integrity(prices_df, volumes_df):
            print("\nData integrity check passed")
        else:
            print("\nWarning: Data integrity check failed")
        
        # Save data
        save_commodity_data(commodity, prices_df, volumes_df, metadata, config)
        
        # Print summary
        print(f"\nProcessing complete for {commodity}")
        print(f"Date range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
        print(f"Number of contracts: {len(prices_df.columns)}")
        print(f"Number of trading days: {len(prices_df)}")
        print(f"Data completeness: {(1 - prices_df.isna().mean().mean()) * 100:.2f}%")
        
        return prices_df, volumes_df, metadata
        
    finally:
        session.stop()

def main():
    """Example usage"""
    config = FetchConfig(
        BASE_PATH=os.getcwd(),
        START_YEAR=2000,  # Adjust as needed
        COMMODITIES=['HG']  # Test with copper
    )
    
    try:
        prices_df, volumes_df, metadata = fetch_commodity_data('HG', config)
        
        if not prices_df.empty:
            print("\nFetch/update successful!")
    except Exception as e:
        print(f"Error processing: {str(e)}")

if __name__ == "__main__":
    main()