#!/usr/bin/env python3
"""
Simple usage example for the MOEX data fetcher.

This script demonstrates how to use the MOEX API client to fetch
equity index data and save it to a CSV file.
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add the src directory to the path so we can import the MOEX API client
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
project_dir = script_dir.parent
sys.path.append(str(project_dir))

# Import the MOEX data source
from src.moex_api_client import MOEXDataSource

def main():
    """Simple example to fetch IMOEX (Moscow Exchange Index) data."""
    # Initialize MOEX data source
    moex = MOEXDataSource()
    
    print("Fetching IMOEX data from MOEX ISS API...")
    
    # Fetch IMOEX (Moscow Exchange Index) data for the last year
    df = moex.get_historical_data(
        engine='stock',
        market='index',
        board='SNDX',
        security='IMOEX',
        from_date=datetime.now().replace(year=datetime.now().year - 1).strftime('%Y-%m-%d'),
        till_date=datetime.now().strftime('%Y-%m-%d')
    )
    
    if df.empty:
        print("No data found for IMOEX")
        return
    
    # Print basic statistics
    print(f"Retrieved {len(df)} records for IMOEX")
    print("\nFirst 5 records:")
    print(df.head())
    
    print("\nBasic statistics:")
    print(df[['CLOSE', 'VOLUME']].describe())
    
    # Save to CSV
    output_dir = project_dir / "data" / "equity_indices"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"IMOEX_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\nData saved to {output_file}")

if __name__ == "__main__":
    main()