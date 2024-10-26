import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
from typing import Dict, Tuple, List, Optional
from spreads_config import SpreadsConfig

def load_raw_data(commodity: str, config: SpreadsConfig) -> Tuple[Optional[pd.DataFrame], 
                                                                Optional[Dict]]:
    """Load raw price data and metadata for a commodity"""
    commodity_path = os.path.join(config.RAW_DATA_PATH, commodity)
    
    try:
        # Load prices
        prices_path = os.path.join(commodity_path, 'prices.parquet')
        if not os.path.exists(prices_path):
            print(f"No price data found for {commodity}")
            return None, None
            
        prices_df = pd.read_parquet(prices_path)
        
        # Load metadata
        metadata_path = os.path.join(commodity_path, 'metadata.json')
        metadata = None
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                
        return prices_df, metadata
        
    except Exception as e:
        print(f"Error loading raw data for {commodity}: {e}")
        return None, None

def get_last_trade_dates(metadata: Dict) -> Dict[str, datetime]:
    """Extract last trade dates from metadata"""
    last_trade_dates = {}
    for contract, info in metadata.items():
        try:
            last_trade_dates[contract] = pd.to_datetime(info['last_trade_date'])
        except:
            continue
    return last_trade_dates

def calculate_days_to_expiry(date: datetime, last_trade_dates: Dict[str, datetime]) -> Dict[str, int]:
    """Calculate days to expiry for each contract from a given date"""
    days_to_expiry = {}
    for contract, last_trade_date in last_trade_dates.items():
        if pd.notna(last_trade_date) and last_trade_date >= date:
            days = (last_trade_date - date).days
            if days >= 0:  # Only include non-expired contracts
                days_to_expiry[contract] = days
    return days_to_expiry

def create_monthly_futures_data(prices_df: pd.DataFrame, 
                              metadata: Dict,
                              config: SpreadsConfig) -> Tuple[pd.DataFrame, ...]:
    """Create monthly futures data and calculate spreads"""
    print(f"\nProcessing spreads...")
    print(f"Data range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
    
    # Get last trade dates
    last_trade_dates = get_last_trade_dates(metadata)
    
    # Initialize result dataframes
    monthly_futures = pd.DataFrame(index=prices_df.index)
    spreads_dollar = pd.DataFrame(index=prices_df.index)
    spreads_percent = pd.DataFrame(index=prices_df.index)
    spreads_percent_annual = pd.DataFrame(index=prices_df.index)
    days_to_expiry_df = pd.DataFrame(index=prices_df.index)
    
    total_dates = len(prices_df.index)
    print(f"Processing {total_dates} dates...")
    
    for date in prices_df.index:
        # Get valid contracts for this date
        valid_contracts = [c for c in prices_df.columns 
                         if pd.notna(prices_df.loc[date, c])]
        
        if not valid_contracts:
            continue
        
        # Calculate days to expiry and filter contracts
        days_to_expiry = calculate_days_to_expiry(date, last_trade_dates)
        valid_contracts = [c for c in valid_contracts if c in days_to_expiry]
        
        if not valid_contracts:
            continue
        
        # Order contracts by expiry
        ordered_contracts = sorted(valid_contracts, 
                                 key=lambda x: days_to_expiry.get(x, float('inf')))
        
        # Store price and days to expiry for each month
        for i, contract in enumerate(ordered_contracts, 1):
            if i > config.MAX_MONTHS_FORWARD:
                break
            monthly_futures.loc[date, f"month_{i}_future"] = contract
            monthly_futures.loc[date, f"month_{i}_price"] = prices_df.loc[date, contract]
            days_to_expiry_df.loc[date, f"month_{i}_days"] = days_to_expiry[contract]
        
        # Calculate spreads if we have at least two months
        if len(ordered_contracts) >= 2:
            m1_contract = ordered_contracts[0]
            m1_price = prices_df.loc[date, m1_contract]
            m1_days = days_to_expiry[m1_contract]
            
            # Calculate spreads for subsequent months
            for i in range(1, min(len(ordered_contracts), config.MAX_MONTHS_FORWARD)):
                far_contract = ordered_contracts[i]
                far_price = prices_df.loc[date, far_contract]
                far_days = days_to_expiry[far_contract]
                
                if m1_price != 0 and far_days and m1_days:
                    # Dollar spreads
                    if config.CALCULATE_DOLLAR_SPREADS:
                        spreads_dollar.loc[date, f"spread_1_{i+1}m"] = far_price - m1_price
                    
                    # Percentage spreads
                    if config.CALCULATE_PERCENT_SPREADS:
                        pct_spread = (far_price - m1_price) / m1_price
                        spreads_percent.loc[date, f"spread_1_{i+1}m_pct"] = pct_spread
                    
                    # Annualized percentage spreads
                    if config.CALCULATE_ANNUAL_SPREADS:
                        days_difference = far_days - m1_days
                        if days_difference > 0:
                            annual_factor = config.TRADING_DAYS_PER_YEAR / days_difference
                            annual_spread = pct_spread * annual_factor
                            spreads_percent_annual.loc[date, f"spread_1_{i+1}m_pct_annual"] = annual_spread
    
    print("Spread calculations complete")
    return (monthly_futures, spreads_dollar, spreads_percent, 
            spreads_percent_annual, days_to_expiry_df)

def save_spread_data(commodity: str, spread_data: Tuple[pd.DataFrame, ...], 
                    config: SpreadsConfig):
    """Save calculated spread data"""
    spread_path = os.path.join(config.PROCESSED_DATA_PATH, commodity)
    os.makedirs(spread_path, exist_ok=True)
    
    # Unpack spread data
    (monthly_futures, spreads_dollar, spreads_percent, 
     spreads_percent_annual, days_to_expiry) = spread_data
    
    # Save dataframes
    monthly_futures.to_parquet(os.path.join(spread_path, 'monthly_futures.parquet'))
    spreads_dollar.to_parquet(os.path.join(spread_path, 'spreads_dollar.parquet'))
    spreads_percent.to_parquet(os.path.join(spread_path, 'spreads_percent.parquet'))
    spreads_percent_annual.to_parquet(os.path.join(spread_path, 'spreads_annual.parquet'))
    days_to_expiry.to_parquet(os.path.join(spread_path, 'days_to_expiry.parquet'))
    
    # Save configuration and summary
    with open(os.path.join(spread_path, 'spread_info.json'), 'w') as f:
        json.dump({
            'last_calculation': datetime.now().isoformat(),
            'date_range': {
                'start': monthly_futures.index.min().isoformat(),
                'end': monthly_futures.index.max().isoformat()
            },
            'spread_counts': {
                'dollar': len(spreads_dollar.columns),
                'percent': len(spreads_percent.columns),
                'annual': len(spreads_percent_annual.columns)
            },
            'max_months_forward': config.MAX_MONTHS_FORWARD,
            'trading_days_per_year': config.TRADING_DAYS_PER_YEAR
        }, f, indent=2)

def main():
    """Example usage"""
    config = SpreadsConfig(
        BASE_PATH=os.getcwd(),
        MAX_MONTHS_FORWARD=13,
        TRADING_DAYS_PER_YEAR=251
    )
    
    commodity = 'CL'
    
    try:
        # Load raw data
        prices_df, metadata = load_raw_data(commodity, config)
        
        if prices_df is not None:
            # Calculate spreads
            spread_data = create_monthly_futures_data(
                prices_df=prices_df,
                metadata=metadata,
                config=config
            )
            
            # Save results
            save_spread_data(commodity, spread_data, config)
            
            print("\nResults saved successfully")
            print(f"Date range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
            print("Available spread types:")
            print(f"Dollar spreads: {spread_data[1].columns.tolist()}")
            print(f"Percentage spreads: {spread_data[2].columns.tolist()}")
            print(f"Annualized spreads: {spread_data[3].columns.tolist()}")
            
        else:
            print(f"No data found for {commodity}")
            
    except Exception as e:
        print(f"Error calculating spreads: {str(e)}")

if __name__ == "__main__":
    main()