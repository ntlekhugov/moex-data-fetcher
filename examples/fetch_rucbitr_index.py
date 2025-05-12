#!/usr/bin/env python3
"""
Fetch RUCBITR (Russian Corporate Bond Total Return Index) data from MOEX ISS API.

This script fetches the RUCBITR index historical data and saves it
to the appropriate directory for further analysis.

Usage:
    python examples/fetch_rucbitr_index.py --from-date 2020-01-01
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Set up project paths
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
project_dir = script_dir.parent
sys.path.append(str(project_dir))

# Import the MOEX data source
from src.moex_api_client import MOEXDataSource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fetch_rucbitr_index')

def fetch_corporate_bond_index(from_date, to_date=None):
    """
    Fetch RUCBITR index data and save to CSV file.
    
    Args:
        from_date: Start date for historical data
        to_date: End date for historical data (default: today)
        
    Returns:
        Path to the saved CSV file
    """
    # Initialize MOEX data source
    moex = MOEXDataSource()
    
    # Create data directory if it doesn't exist
    data_dir = project_dir / 'data' / 'bond_indices'
    os.makedirs(data_dir, exist_ok=True)
    
    # Ticker for Russian Corporate Bond Total Return Index
    ticker = 'RUCBITR'
    
    logger.info(f"Fetching {ticker} data from {from_date} to {to_date or 'today'}...")
    
    # Fetch historical data
    df = moex.get_historical_data(
        engine='stock',
        market='index',
        board='SNDX',
        security=ticker,
        from_date=from_date,
        till_date=to_date
    )
    
    if df.empty:
        logger.error(f"No data found for {ticker}")
        return None
    
    # Add some basic statistics
    logger.info(f"Retrieved {len(df)} records for {ticker}")
    
    # Calculate daily returns
    if 'CLOSE' in df.columns and len(df) > 1:
        df['DAILY_RETURN'] = df['CLOSE'].pct_change() * 100
        
        # Calculate some statistics
        avg_return = df['DAILY_RETURN'].mean()
        volatility = df['DAILY_RETURN'].std()
        
        logger.info(f"Average daily return: {avg_return:.4f}%")
        logger.info(f"Daily volatility: {volatility:.4f}%")
    
    # Save to CSV
    date_str = datetime.now().strftime("%Y%m%d")
    output_file = data_dir / f"{ticker}_{date_str}.csv"
    df.to_csv(output_file, index=False)
    
    logger.info(f"Data saved to {output_file}")
    
    return output_file

def main():
    """Main function to fetch RUCBITR index data."""
    parser = argparse.ArgumentParser(description='Fetch RUCBITR index data from MOEX ISS API')
    
    parser.add_argument('--from-date', type=str, default='2020-01-01', 
                        help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--to-date', type=str, 
                        help='End date for historical data (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Parse dates
    from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
    to_date = datetime.strptime(args.to_date, '%Y-%m-%d') if args.to_date else datetime.now()
    
    # Fetch data
    output_file = fetch_corporate_bond_index(from_date, to_date)
    
    if output_file:
        print(f"\nSuccessfully fetched RUCBITR data and saved to {output_file}")
    else:
        print("\nFailed to fetch RUCBITR data")

if __name__ == "__main__":
    main()