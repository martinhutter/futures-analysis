# spreads_visualizer.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
from typing import List, Dict, Optional
from fetch_config import FetchConfig
from spreads_config import SpreadsConfig
from datetime import datetime

def load_spread_data(commodity: str, config: SpreadsConfig) -> Dict[str, pd.DataFrame]:
    """Load all spread data for a commodity"""
    spread_path = os.path.join(config.PROCESSED_DATA_PATH, commodity)
    
    data = {}
    try:
        # Load all spread types
        data['monthly_futures'] = pd.read_parquet(os.path.join(spread_path, 'monthly_futures.parquet'))
        data['spreads_dollar'] = pd.read_parquet(os.path.join(spread_path, 'spreads_dollar.parquet'))
        data['spreads_percent'] = pd.read_parquet(os.path.join(spread_path, 'spreads_percent.parquet'))
        data['spreads_annual'] = pd.read_parquet(os.path.join(spread_path, 'spreads_annual.parquet'))
        
        print(f"Loaded spread data for {commodity}")
        print(f"Date range: {data['spreads_dollar'].index.min().date()} to {data['spreads_dollar'].index.max().date()}")
        return data
    except Exception as e:
        print(f"Error loading spread data for {commodity}: {e}")
        return None

def identify_roll_dates(monthly_futures_df: pd.DataFrame) -> List[datetime]:
    """Identify dates when the front month future changes"""
    roll_dates = []
    previous_front = None
    front_month_col = 'month_1_future'
    
    if front_month_col not in monthly_futures_df.columns:
        return roll_dates
        
    for date in monthly_futures_df.index:
        current_front = monthly_futures_df.loc[date, front_month_col]
        if pd.notna(current_front) and current_front != previous_front:
            roll_dates.append(date)
            previous_front = current_front
    
    print(f"Identified {len(roll_dates)} roll dates")
    return roll_dates

def create_spread_visualizations(spreads_data: Dict[str, pd.DataFrame], 
                               commodity: str, 
                               config: SpreadsConfig):
    """Create visualizations for all types of spreads in a single PDF"""
    print(f"\nCreating visualizations for {commodity}...")
    
    # Identify roll dates
    roll_dates = identify_roll_dates(spreads_data['monthly_futures'])
    print(f"Plotting with {len(roll_dates)} roll dates")
    
    # Setup the PDF
    viz_path = os.path.join(config.BASE_PATH, 'visualizations')
    os.makedirs(viz_path, exist_ok=True)
    pdf_path = os.path.join(viz_path, f'{commodity}_spreads.pdf')
    
    with PdfPages(pdf_path) as pdf:
        # Dollar spreads
        plt.figure(figsize=(15, 8))
        for column in spreads_data['spreads_dollar'].columns:
            plt.plot(spreads_data['spreads_dollar'].index, 
                    spreads_data['spreads_dollar'][column], 
                    label=column, linewidth=1.5)
        
        # Add roll date lines
        for roll_date in roll_dates:
            plt.axvline(x=roll_date, color='gray', linewidth=0.5, alpha=0.7)
        
        plt.title(f'{commodity} Dollar Spreads')
        plt.xlabel('Date')
        plt.ylabel('Spread Value')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, axis='y')
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # Percentage spreads
        plt.figure(figsize=(15, 8))
        for column in spreads_data['spreads_percent'].columns:
            plt.plot(spreads_data['spreads_percent'].index, 
                    spreads_data['spreads_percent'][column], 
                    label=column, linewidth=1.5)
        
        for roll_date in roll_dates:
            plt.axvline(x=roll_date, color='gray', linewidth=0.5, alpha=0.7)
        
        plt.title(f'{commodity} Percentage Spreads')
        plt.xlabel('Date')
        plt.ylabel('Spread Percentage')
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.1%}'.format(y)))
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, axis='y')
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # Annualized percentage spreads
        plt.figure(figsize=(15, 8))
        for column in spreads_data['spreads_annual'].columns:
            plt.plot(spreads_data['spreads_annual'].index, 
                    spreads_data['spreads_annual'][column], 
                    label=column, linewidth=1.5)
        
        for roll_date in roll_dates:
            plt.axvline(x=roll_date, color='gray', linewidth=0.5, alpha=0.7)
        
        plt.title(f'{commodity} Annualized Percentage Spreads')
        plt.xlabel('Date')
        plt.ylabel('Annualized Spread Percentage')
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.1%}'.format(y)))
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, axis='y')
        plt.tight_layout()
        pdf.savefig()
        plt.close()
    
    print(f"Visualizations saved to {pdf_path}")

def main():
    """Create visualizations for all commodities"""
    # Setup configs
    base_path = os.getcwd()
    config = SpreadsConfig(BASE_PATH=base_path)
    fetch_config = FetchConfig(BASE_PATH=base_path)
    
    for commodity in fetch_config.COMMODITIES:
        print(f"\nProcessing visualizations for {commodity}")
        
        # Load spread data
        spread_data = load_spread_data(commodity, config)
        
        if spread_data:
            # Create visualizations
            create_spread_visualizations(spread_data, commodity, config)
        else:
            print(f"Skipping {commodity} - no data available")

if __name__ == "__main__":
    main()