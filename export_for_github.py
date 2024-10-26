# export_for_github.py

import pandas as pd
import os
import json
from datetime import datetime
from fetch_config import FetchConfig
from spreads_config import SpreadsConfig

def export_spreads_to_json(commodity: str, configs: tuple):
    """Export spread data to JSON format for GitHub"""
    fetch_config, spreads_config = configs
    print(f"\nExporting {commodity} data...")
    
    # Setup paths
    processed_path = os.path.join(spreads_config.PROCESSED_DATA_PATH, commodity)
    github_path = os.path.join(fetch_config.BASE_PATH, 'data', commodity)
    os.makedirs(github_path, exist_ok=True)
    
    # Map of spread types to files
    spread_files = {
        'dollar': 'spreads_dollar.parquet',
        'percent': 'spreads_percent.parquet',
        'annual': 'spreads_annual.parquet'
    }
    
    exported_files = {}
    
    try:
        for spread_type, filename in spread_files.items():
            # Load parquet file
            parquet_path = os.path.join(processed_path, filename)
            if not os.path.exists(parquet_path):
                print(f"Warning: {filename} not found for {commodity}")
                continue
                
            df = pd.read_parquet(parquet_path)
            
            # Convert to the format needed for visualization
            df_export = df.reset_index()
            df_export['date'] = df_export['index'].dt.strftime('%Y-%m-%d')
            df_export = df_export.drop('index', axis=1)
            
            # Save as JSON
            json_filename = f'{commodity}_{spread_type}_spreads.json'
            json_path = os.path.join(github_path, json_filename)
            df_export.to_json(json_path, orient='records', date_format='iso')
            
            exported_files[spread_type] = json_filename
            print(f"Exported {json_filename}")
            
        # Create metadata file
        metadata = {
            'commodity': commodity,
            'date_range': {
                'start': df.index.min().strftime('%Y-%m-%d'),
                'end': df.index.max().strftime('%Y-%m-%d')
            },
            'available_spreads': list(exported_files.keys()),
            'files': exported_files,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(os.path.join(github_path, f'{commodity}_metadata.json'), 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print(f"Successfully exported {commodity} data")
        return True
        
    except Exception as e:
        print(f"Error exporting {commodity}: {e}")
        return False

def main():
    """Export all commodities' data for GitHub"""
    # Setup configs
    fetch_config = FetchConfig(BASE_PATH=os.getcwd())
    spreads_config = SpreadsConfig(BASE_PATH=os.getcwd())
    configs = (fetch_config, spreads_config)
    
    # Create main data directory
    data_path = os.path.join(fetch_config.BASE_PATH, 'data')
    os.makedirs(data_path, exist_ok=True)
    
    # Export each commodity
    results = {}
    for commodity in fetch_config.COMMODITIES:
        success = export_spreads_to_json(commodity, configs)
        results[commodity] = success
    
    # Create index file
    index = {
        'commodities': fetch_config.COMMODITIES,
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'spread_types': ['dollar', 'percent', 'annual'],
        'status': results
    }
    
    with open(os.path.join(data_path, 'index.json'), 'w') as f:
        json.dump(index, f, indent=2)
    
    print("\nExport Summary:")
    print("=" * 50)
    for commodity, success in results.items():
        status = "✓" if success else "✗"
        print(f"{commodity}: {status}")

if __name__ == "__main__":
    main()