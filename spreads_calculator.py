import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
from typing import Dict, Tuple, List
from tqdm import tqdm
from spreads_config import SpreadsConfig

def get_last_trade_dates(metadata: Dict) -> Dict[str, datetime]:
    """Extract last trade dates from metadata"""
    last_trade_dates = {}
    for contract, info in metadata.items():
        try:
            last_trade_dates[contract] = pd.to_datetime(info['last_trade_date'])
        except:
            continue
    return last_trade_dates

def calculate_days_to_expiry(date: datetime, 
                           last_trade_dates: Dict[str, datetime],
                           min_days: int = 0) -> Dict[str, int]:
    """Calculate days to expiry for each contract from a given date"""
    days_to_expiry = {}
    for contract, last_trade_date in last_trade_dates.items():
        if pd.notna(last_trade_date) and last_trade_date >= date:
            days = (last_trade_date - date).days
            if days >= min_days:
                days_to_expiry[contract] = days
    return days_to_expiry

def create_monthly_futures_data(prices_df: pd.DataFrame, 
                              metadata: Dict,
                              config: SpreadsConfig) -> Tuple[pd.DataFrame, ...]:
    """Create monthly futures data and calculate spreads"""
    print("\nProcessing spreads...")
    print(f"Data range: {prices_df.index.min().date()} to {prices_df.index.max().date()}")
    
    # Get last trade dates for all contracts
    last_trade_dates = get_last_trade_dates(metadata)
    
    # Initialize result dataframes
    monthly_futures = pd.DataFrame(index=prices_df.index)
    spreads_dollar = pd.DataFrame(index=prices_df.index)
    spreads_percent = pd.DataFrame(index=prices_df.index)
    spreads_percent_annual = pd.DataFrame(index=prices_df.index)
    days_to_expiry_df = pd.DataFrame(index=prices_df.index)
    
    # Pre-calculate valid contracts and their expiry dates for each date
    print("Pre-processing contract data...")
    date_contracts_map = {}
    
    for date in tqdm(prices_df.index, desc="Analyzing dates"):
        # Get valid contracts for this date (non-NaN prices)
        valid_contracts = [c for c in prices_df.columns 
                         if pd.notna(prices_df.at[date, c])]
        
        if not valid_contracts:
            continue
            
        # Calculate days to expiry and filter contracts
        days_to_expiry = calculate_days_to_expiry(date, last_trade_dates)
        valid_contracts = [(c, days_to_expiry[c]) for c in valid_contracts 
                          if c in days_to_expiry]
        
        if valid_contracts:
            # Sort by days to expiry
            date_contracts_map[date] = sorted(valid_contracts, 
                                            key=lambda x: x[1])
    
    print(f"Processing {len(prices_df.index)} dates...")
    for date in tqdm(prices_df.index, desc="Calculating spreads"):
        if date not in date_contracts_map:
            continue
        
        # Get pre-calculated contracts for this date
        ordered_contracts = date_contracts_map[date]
        
        # Store prices and days to expiry for each month
        for i, (contract, days) in enumerate(ordered_contracts, 1):
            if i > config.MAX_MONTHS_FORWARD:
                break
            monthly_futures.at[date, f"month_{i}_future"] = contract
            monthly_futures.at[date, f"month_{i}_price"] = prices_df.at[date, contract]
            days_to_expiry_df.at[date, f"month_{i}_days"] = days
        
        # Calculate spreads if we have at least two months
        if len(ordered_contracts) >= 2:
            m1_contract, m1_days = ordered_contracts[0]
            m1_price = prices_df.at[date, m1_contract]
            
            # Calculate spreads for subsequent months
            for i in range(1, min(len(ordered_contracts), config.MAX_MONTHS_FORWARD)):
                far_contract, far_days = ordered_contracts[i]
                far_price = prices_df.at[date, far_contract]
                
                if m1_price != 0 and far_days and m1_days:
                    # Dollar spreads
                    dollar_spread = far_price - m1_price
                    spreads_dollar.at[date, f"spread_1_{i+1}m"] = dollar_spread
                    
                    # Percentage spreads
                    pct_spread = dollar_spread / m1_price
                    spreads_percent.at[date, f"spread_1_{i+1}m_pct"] = pct_spread
                    
                    # Annualized percentage spreads
                    days_difference = far_days - m1_days
                    if days_difference > 0:
                        annual_factor = config.TRADING_DAYS_PER_YEAR / days_difference
                        annual_spread = pct_spread * annual_factor
                        spreads_percent_annual.at[date, f"spread_1_{i+1}m_pct_annual"] = annual_spread
    
    print("Spread calculations complete")
    return (monthly_futures, spreads_dollar, spreads_percent, 
            spreads_percent_annual, days_to_expiry_df)

def save_spread_data(commodity: str, spread_data: Tuple[pd.DataFrame, ...], 
                    config: SpreadsConfig):
    """Save calculated spread data"""
    # Create directory if it doesn't exist
    spread_path = os.path.join(config.PROCESSED_DATA_PATH, commodity)
    os.makedirs(spread_path, exist_ok=True)
    
    # Unpack spread data
    (monthly_futures, spreads_dollar, spreads_percent, 
     spreads_percent_annual, days_to_expiry) = spread_data
    
    # Save all dataframes to parquet format
    monthly_futures.to_parquet(os.path.join(spread_path, 'monthly_futures.parquet'))
    spreads_dollar.to_parquet(os.path.join(spread_path, 'spreads_dollar.parquet'))
    spreads_percent.to_parquet(os.path.join(spread_path, 'spreads_percent.parquet'))
    spreads_percent_annual.to_parquet(os.path.join(spread_path, 'spreads_annual.parquet'))
    days_to_expiry.to_parquet(os.path.join(spread_path, 'days_to_expiry.parquet'))
    
    # Save calculation info
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
    
    # Load raw data (example for one commodity)
    commodity_path = os.path.join(config.RAW_DATA_PATH, 'CL')
    prices_path = os.path.join(commodity_path, 'prices.parquet')
    metadata_path = os.path.join(commodity_path, 'metadata.json')
    
    if os.path.exists(prices_path) and os.path.exists(metadata_path):
        prices_df = pd.read_parquet(prices_path)
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
            
        # Calculate spreads
        spread_data = create_monthly_futures_data(
            prices_df=prices_df,
            metadata=metadata,
            config=config
        )
        
        # Save results
        save_spread_data('CL', spread_data, config)
        print("Calculations complete and saved")

if __name__ == "__main__":
    main()