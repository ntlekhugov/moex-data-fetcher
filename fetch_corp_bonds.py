#!/usr/bin/env python3
"""
Script to fetch data for 100 different corporate bonds from MOEX.
Uses the MOEXDataSource class from the project.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
from tqdm import tqdm

# Set up project paths
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(str(script_dir))

# Import the MOEX data source
from moex_data_fetcher.api.client import MOEXDataSource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fetch_corp_bonds')

def fetch_corporate_bonds(limit=100, output_file=None):
    """
    Fetch data for corporate bonds from MOEX.
    
    Args:
        limit: Maximum number of bonds to fetch (default: 100)
        output_file: Path to save the data (default: None, will use a timestamp)
    
    Returns:
        DataFrame with bond data
    """
    # Initialize MOEX data source
    moex = MOEXDataSource()
    
    # Get corporate bonds from TQCB board (main corporate bonds board)
    logger.info("Fetching list of corporate bonds...")
    bonds = moex.get_securities('stock', 'bonds', 'TQCB')
    
    # Ensure we have bonds data
    if bonds.empty:
        logger.error("No corporate bonds found")
        return pd.DataFrame()
    
    logger.info(f"Found {len(bonds)} corporate bonds. Will fetch data for {min(limit, len(bonds))} bonds.")
    
    # Limit the number of bonds to fetch
    bonds = bonds.head(limit)
    
    # Prepare to store all data
    all_data = []
    
    # Set date range for the last 1 year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Fetch data for each bond
    for _, bond in tqdm(bonds.iterrows(), total=len(bonds), desc="Fetching bond data"):
        try:
            secid = bond['SECID']
            logger.info(f"Fetching data for {secid}")
            
            # Fetch historical data
            df = moex.get_historical_data(
                engine='stock',
                market='bonds',
                board='TQCB',
                security=secid,
                from_date=start_date,
                till_date=end_date
            )
            
            if df.empty:
                logger.warning(f"No data found for {secid}")
                continue
                
            # Add bond identifier
            df['SECID'] = secid
            
            # Add bond name
            df['SHORTNAME'] = bond.get('SHORTNAME', '')
            
            # Add to our collection
            all_data.append(df)
            
            # Be nice to the API
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error fetching data for {bond.get('SECID', 'unknown')}: {str(e)}")
    
    # Combine all data
    if not all_data:
        logger.error("No data was collected")
        return pd.DataFrame()
        
    combined_data = pd.concat(all_data, ignore_index=True)
    logger.info(f"Collected {len(combined_data)} records for {len(all_data)} bonds")
    
    # Save to file if requested
    if output_file is None:
        output_file = f"data/corp_bonds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save to CSV
    combined_data.to_csv(output_file, index=False)
    logger.info(f"Data saved to {output_file}")
    
    return combined_data

if __name__ == "__main__":
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Output file path
    output_file = "data/corp_bonds_100.csv"
    
    # Fetch data for 100 corporate bonds
    data = fetch_corporate_bonds(limit=100, output_file=output_file)
    
    print(f"Fetched data for {len(data['SECID'].unique())} bonds")
    print(f"Total records: {len(data)}")
    print(f"Data saved to {output_file}")