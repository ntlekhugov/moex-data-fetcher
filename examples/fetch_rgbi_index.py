#!/usr/bin/env python3
"""
Fetch RGBI (Russian Government Bond Index) data from MOEX ISS API.

This script specifically fetches the RGBI index historical data and saves it
to the appropriate directory for further analysis.

Usage:
    python examples/fetch_rgbi_index.py --from-date 2020-01-01
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
from moex_data_fetcher.api.client import MOEXDataSource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fetch_rgbi_index')

def fetch_and_save_bond_index(
    moex_source,
    ticker,
    from_date,
    to_date=None
):
    """
    Fetch bond index data and save to CSV file.
    
    Args:
        moex_source: MOEXDataSource instance
        ticker: Bond index ticker symbol
        from_date: Start date for historical data
        to_date: End date for historical data
        
    Returns:
        Dictionary with metadata about the fetched data
    """
    logger.info(f"Fetching {ticker} bond index data...")
    
    # Create directories if they don't exist
    data_dir = project_dir / 'data' / 'bond_indices'
    os.makedirs(data_dir, exist_ok=True)
    
    # Current date for filenames
    date_str = datetime.now().strftime("%Y%m%d")
    
    try:
        # Determine the appropriate board based on index name
        if ticker.startswith('RTS'):
            board = 'RTSI'
        else:
            board = 'SNDX'
        
        # Fetch historical data
        df = moex_source.get_historical_data(
            engine='stock',
            market='index',
            board=board,
            security=ticker,
            from_date=from_date,
            till_date=to_date
        )
        
        if df.empty:
            logger.warning(f"No data found for {ticker}")
            return {"status": "error", "message": f"No data found for {ticker}"}
            
        # Save to CSV
        filename = f"{ticker}_{date_str}.csv"
        file_path = data_dir / filename
        df.to_csv(file_path, index=False)
            
        logger.info(f"Saved {len(df)} records to {file_path}")
        return {
            "status": "success", 
            "file_path": str(file_path),
            "record_count": len(df)
        }
        
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return {"status": "error", "message": str(e)}

def main():
    """Main function to fetch RGBI and related bond indices."""
    parser = argparse.ArgumentParser(description='Fetch RGBI and related bond indices from MOEX ISS API')
    
    parser.add_argument('--from-date', type=str, default='2020-01-01', 
                        help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--to-date', type=str, 
                        help='End date for historical data (YYYY-MM-DD)')
    parser.add_argument('--ticker', type=str, default='RGBITR',
                        help='Specific bond index ticker to fetch (default: RGBITR)')
    
    args = parser.parse_args()
    
    # Parse dates
    from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
    to_date = datetime.strptime(args.to_date, '%Y-%m-%d') if args.to_date else datetime.now()
    
    # Initialize MOEX data source
    moex_source = MOEXDataSource()
    
    # Fetch data for the ticker
    result = fetch_and_save_bond_index(
        moex_source=moex_source,
        ticker=args.ticker,
        from_date=from_date,
        to_date=to_date
    )
    
    # Print summary
    print("\nFetch Summary:")
    print("=" * 50)
    if result["status"] == "success":
        print(f"{args.ticker}: Successfully fetched {result['record_count']} records")
        print(f"Data saved to: {result['file_path']}")
    else:
        print(f"{args.ticker}: Error - {result.get('message', 'Unknown error')}")

if __name__ == "__main__":
    main()