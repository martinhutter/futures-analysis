# Futures Analysis

Code for fetching and analyzing futures data from Bloomberg.

## Structure
- `fetch_config.py`: Configuration for data fetching
- `data_fetcher.py`: Core data fetching functionality
- `spreads_config.py`: Configuration for spread calculations
- `spreads_calculator.py`: Spread calculation functionality
- `spreads_visualizer.py`: Visualization tools
- `backfill_cl.py`: Utility for backfilling historical data

## Setup
1. Create required directories:
```bash
mkdir raw_data
mkdir processed_data
mkdir visualizations
mkdir logs
```

2. Make sure you have required packages:
```bash
pip install pandas numpy blpapi matplotlib tqdm
```

3. Make sure you have Bloomberg terminal running and are logged in

## Usage
1. Fetch data:
```python
python data_fetcher.py
```

2. Calculate spreads:
```python
python spreads_calculator.py
```

3. Create visualizations:
```python
python spreads_visualizer.py
```

## Data Structure
- Raw data stored in parquet format in `raw_data/`
- Processed spreads stored in `processed_data/`
- Visualizations saved as PDFs in `visualizations/`

## Notes
- Requires Bloomberg terminal and Python API
- Data updates are incremental by default
- Historical data can be backfilled using utility scripts