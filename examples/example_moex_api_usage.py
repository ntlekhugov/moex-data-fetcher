#!/usr/bin/env python3
"""
Example script demonstrating how to use the MOEX ISS API client module.

This script shows how to fetch bond indices data from MOEX ISS API
and perform basic analysis.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

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
logger = logging.getLogger('example_moex_api_usage')

def get_bond_indices_info():
    """Get information about available bond indices."""
    # Initialize MOEX data source
    moex = MOEXDataSource()
    
    # Get all available indices
    indices = moex.get_securities('stock', 'index')
    
    # Filter for bond indices
    bond_pattern = '|'.join(['RGBI', 'RUCBI', 'RUEU', 'RUCNY', 'RUGROW', 'DOMMBS', 'RUABI', 'RUPCI', 'RUPMI', 'RUPAI'])
    bond_indices = indices[indices['SECID'].str.contains(bond_pattern, na=False, regex=True, case=False)]
    
    print(f"\nFound {len(bond_indices)} bond indices")
    print("\nSample of available bond indices:")
    print(bond_indices[['SECID', 'SHORTNAME', 'BOARDID']].head(10))
    
    return bond_indices

def fetch_and_compare_indices(indices_to_compare):
    """Fetch and compare multiple bond indices."""
    # Initialize MOEX data source
    moex = MOEXDataSource()
    
    # Set date range for the last 3 years
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 3)
    
    # Fetch data for each index
    all_data = {}
    
    for index_code in indices_to_compare:
        logger.info(f"Fetching data for {index_code}")
        
        # Get securities to find the board
        securities = moex.get_securities('stock', 'index')
        search_results = securities[securities['SECID'] == index_code]
        
        if search_results.empty:
            logger.warning(f"Index {index_code} not found")
            continue
            
        board = search_results.iloc[0]['BOARDID']
        
        # Fetch historical data
        df = moex.get_historical_data(
            engine='stock',
            market='index',
            board=board,
            security=index_code,
            from_date=start_date,
            till_date=end_date
        )
        
        if df.empty:
            logger.warning(f"No data found for {index_code}")
            continue
            
        # Convert TRADEDATE to datetime
        df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
        
        # Store in dictionary
        all_data[index_code] = df
        
        logger.info(f"Fetched {len(df)} records for {index_code}")
    
    return all_data

def plot_indices_comparison(indices_data):
    """Plot a comparison of multiple bond indices."""
    plt.figure(figsize=(12, 8))
    
    for index_code, df in indices_data.items():
        # Normalize to 100 at the start
        first_value = df.iloc[0]['CLOSE']
        normalized_values = df['CLOSE'] / first_value * 100
        
        plt.plot(df['TRADEDATE'], normalized_values, label=f"{index_code}")
    
    plt.title('Bond Indices Comparison (Normalized to 100)')
    plt.xlabel('Date')
    plt.ylabel('Index Value (Normalized)')
    plt.legend()
    plt.grid(True)
    
    # Save the plot
    output_dir = project_dir / "data" / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_dir / "bond_indices_comparison.png")
    
    logger.info(f"Plot saved to {output_dir / 'bond_indices_comparison.png'}")
    
    # Show the plot
    plt.tight_layout()
    plt.show()

def main():
    """Main function to demonstrate MOEX ISS API usage."""
    # Get information about available bond indices
    bond_indices = get_bond_indices_info()
    
    # Select a few indices to compare
    indices_to_compare = [
        'RGBITR',      # Government bonds (total return)
        'RUCBITR',     # Corporate bonds (total return)
        'RUEU10',      # Eurobonds
        'RUCNYTR',     # CNY bonds
        'RUGROWTR'     # Growth sector bonds
    ]
    
    # Fetch data for selected indices
    indices_data = fetch_and_compare_indices(indices_to_compare)
    
    # Plot comparison
    if indices_data:
        plot_indices_comparison(indices_data)
    else:
        logger.warning("No data to plot")

if __name__ == "__main__":
    main()