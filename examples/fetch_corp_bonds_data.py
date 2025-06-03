#!/usr/bin/env python
"""
Example script demonstrating how to fetch detailed bond data from MOEX.

This script fetches:
1. Instrument parameters
2. Daily trading results
3. Coupon payment schedules

For Russian domestic bonds within the period from September 19, 2022 to April 4, 2025.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from moex_data_fetcher.bonds.data import MOEXBondData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create data directory if it doesn't exist
DATA_DIR = Path('./data')
DATA_DIR.mkdir(exist_ok=True)

def fetch_and_save_bond_data(sample_size=5):
    """
    Fetch and save detailed bond data for Russian domestic bonds
    within the specified date range.
    """
    # Initialize the MOEX Bond Data client
    bond_client = MOEXBondData()
    
    # Define the date range
    from_date = "2022-09-19"
    till_date = "2025-04-04"
    
    logger.info(f"Fetching Russian domestic bonds data for period: {from_date} to {till_date}")
    
    # Find Russian domestic bonds active in the specified period
    russian_bonds = bond_client.find_russian_domestic_bonds(from_date, till_date)
    
    if russian_bonds.empty:
        logger.warning("No Russian domestic bonds found in the specified period")
        return
    
    logger.info(f"Found {len(russian_bonds)} Russian domestic bonds")
    
    # Save the list of bonds to CSV
    bonds_file = DATA_DIR / "russian_domestic_bonds.csv"
    russian_bonds.to_csv(bonds_file, index=False)
    logger.info(f"Saved list of bonds to {bonds_file}")
    
    # For demonstration, select a few bonds to analyze in detail
    # In a real scenario, you might want to process all bonds or filter by specific criteria
    sample_bonds = russian_bonds.head(sample_size)
    
    # Make sure we have ISIN column
    if 'ISIN' not in sample_bonds.columns:
        logger.error("ISIN column not found in bond data. Available columns: %s", sample_bonds.columns.tolist())
        # Try to find an alternative column that might contain ISIN
        for col in sample_bonds.columns:
            if 'ISIN' in col.upper():
                logger.info(f"Using {col} column instead of ISIN")
                sample_bonds['ISIN'] = sample_bonds[col]
                break
        else:
            logger.error("Could not find ISIN column, cannot proceed")
            return
    
    # Process each bond in the sample
    for idx, bond in sample_bonds.iterrows():
        try:
            isin = bond.get('ISIN')
            secid = bond.get('SECID')
            name = bond.get('SHORTNAME', 'Unknown')
            
            if not isin or pd.isna(isin):
                logger.warning(f"Bond at index {idx} has no ISIN, skipping")
                continue
                
            if not secid or pd.isna(secid):
                logger.warning(f"Bond at index {idx} has no SECID, skipping")
                continue
        
            logger.info(f"Processing bond: {name} (ISIN: {isin})")
            
            # 1. Fetch instrument parameters
            try:
                parameters = bond_client.get_bond_parameters(isin)
                if not parameters.empty:
                    params_file = DATA_DIR / f"{secid}_parameters.csv"
                    parameters.to_csv(params_file, index=False)
                    logger.info(f"Saved parameters to {params_file}")
                else:
                    logger.warning(f"No parameters found for bond {isin}")
            except Exception as e:
                logger.error(f"Error fetching parameters for bond {isin}: {e}")
        
            # 2. Fetch daily trading results
            try:
                trading_data = bond_client.get_bond_daily_trading(isin, from_date, till_date)
                if not trading_data.empty:
                    trading_file = DATA_DIR / f"{secid}_trading.csv"
                    trading_data.to_csv(trading_file, index=False)
                    logger.info(f"Saved trading data to {trading_file}")
                    
                    # Create a simple price chart
                    if 'CLOSE' in trading_data.columns and 'TRADEDATE' in trading_data.columns:
                        try:
                            plt.figure(figsize=(12, 6))
                            plt.plot(pd.to_datetime(trading_data['TRADEDATE']), trading_data['CLOSE'])
                            plt.title(f"{name} - Closing Price")
                            plt.xlabel("Date")
                            plt.ylabel("Price")
                            plt.grid(True)
                            chart_file = DATA_DIR / f"{secid}_price_chart.png"
                            plt.savefig(chart_file)
                            plt.close()
                            logger.info(f"Saved price chart to {chart_file}")
                        except Exception as e:
                            logger.error(f"Error creating price chart for bond {isin}: {e}")
                else:
                    logger.warning(f"No trading data found for bond {isin}")
            except Exception as e:
                logger.error(f"Error fetching trading data for bond {isin}: {e}")
        
            # 3. Fetch coupon payment schedule
            try:
                coupons = bond_client.get_bond_coupons(isin)
                if not coupons.empty:
                    # Filter coupons within our date range
                    if 'coupondate' in coupons.columns:
                        try:
                            coupons['coupondate'] = pd.to_datetime(coupons['coupondate'])
                            filtered_coupons = coupons[
                                (coupons['coupondate'] >= pd.to_datetime(from_date)) & 
                                (coupons['coupondate'] <= pd.to_datetime(till_date))
                            ]
                            
                            if not filtered_coupons.empty:
                                coupons_file = DATA_DIR / f"{secid}_coupons.csv"
                                filtered_coupons.to_csv(coupons_file, index=False)
                                logger.info(f"Saved coupon schedule to {coupons_file}")
                                
                                # Create a coupon payment chart
                                try:
                                    plt.figure(figsize=(12, 6))
                                    plt.bar(filtered_coupons['coupondate'], filtered_coupons['value'])
                                    plt.title(f"{name} - Coupon Payments")
                                    plt.xlabel("Date")
                                    plt.ylabel("Coupon Value")
                                    plt.grid(True)
                                    chart_file = DATA_DIR / f"{secid}_coupon_chart.png"
                                    plt.savefig(chart_file)
                                    plt.close()
                                    logger.info(f"Saved coupon chart to {chart_file}")
                                except Exception as e:
                                    logger.error(f"Error creating coupon chart for bond {isin}: {e}")
                            else:
                                logger.warning(f"No coupons found in the specified date range for bond {isin}")
                        except Exception as e:
                            logger.error(f"Error processing coupon dates for bond {isin}: {e}")
                    else:
                        logger.warning(f"No 'coupondate' column found in coupon data for bond {isin}")
                else:
                    logger.warning(f"No coupon data found for bond {isin}")
            except Exception as e:
                logger.error(f"Error fetching coupon data for bond {isin}: {e}")
        
            # 4. Fetch amortization schedule
            try:
                amortizations = bond_client.get_bond_amortizations(isin)
                if not amortizations.empty:
                    # Filter amortizations within our date range
                    if 'amortdate' in amortizations.columns:
                        try:
                            amortizations['amortdate'] = pd.to_datetime(amortizations['amortdate'])
                            filtered_amortizations = amortizations[
                                (amortizations['amortdate'] >= pd.to_datetime(from_date)) & 
                                (amortizations['amortdate'] <= pd.to_datetime(till_date))
                            ]
                            
                            if not filtered_amortizations.empty:
                                amort_file = DATA_DIR / f"{secid}_amortizations.csv"
                                filtered_amortizations.to_csv(amort_file, index=False)
                                logger.info(f"Saved amortization schedule to {amort_file}")
                            else:
                                logger.warning(f"No amortizations found in the specified date range for bond {isin}")
                        except Exception as e:
                            logger.error(f"Error processing amortization dates for bond {isin}: {e}")
                    else:
                        logger.warning(f"No 'amortdate' column found in amortization data for bond {isin}")
                else:
                    logger.warning(f"No amortization data found for bond {isin}")
            except Exception as e:
                logger.error(f"Error fetching amortization data for bond {isin}: {e}")
            
            logger.info(f"Completed processing for bond: {name}")
            logger.info("-" * 50)
        except Exception as e:
            logger.error(f"Error processing bond at index {idx}: {e}")

def analyze_bond_market():
    """
    Perform basic analysis on the bond market data.
    """
    # Initialize the MOEX Bond Data client
    bond_client = MOEXBondData()
    
    # Define the date range
    from_date = "2022-09-19"
    till_date = "2025-04-04"
    
    # Find Russian domestic bonds active in the specified period
    russian_bonds = bond_client.find_russian_domestic_bonds(from_date, till_date)
    
    if russian_bonds.empty:
        logger.warning("No Russian domestic bonds found for analysis")
        return
    
    # Basic statistics
    logger.info(f"Total number of Russian domestic bonds: {len(russian_bonds)}")
    
    # Count bonds by issuer type (if available)
    if 'TYPENAME' in russian_bonds.columns:
        issuer_counts = russian_bonds['TYPENAME'].value_counts()
        logger.info("Bonds by issuer type:")
        for issuer_type, count in issuer_counts.items():
            logger.info(f"  {issuer_type}: {count}")
    
    # Count bonds by maturity year (if available)
    if 'MATDATE' in russian_bonds.columns:
        # Handle invalid date values
        try:
            # Replace invalid dates with NaT (Not a Time)
            russian_bonds['MATDATE_CLEAN'] = russian_bonds['MATDATE'].replace('0000-00-00', pd.NaT)
            
            # Convert to datetime, handling errors
            russian_bonds['MATURITY_YEAR'] = pd.to_datetime(russian_bonds['MATDATE_CLEAN'], errors='coerce').dt.year
            
            # Drop NaN values for the chart
            valid_maturities = russian_bonds.dropna(subset=['MATURITY_YEAR'])
            
            if not valid_maturities.empty:
                maturity_counts = valid_maturities['MATURITY_YEAR'].value_counts().sort_index()
                logger.info("Bonds by maturity year:")
                for year, count in maturity_counts.items():
                    logger.info(f"  {year}: {count}")
                
                # Create a chart of bonds by maturity year
                plt.figure(figsize=(12, 6))
                maturity_counts.plot(kind='bar')
                plt.title("Russian Domestic Bonds by Maturity Year")
                plt.xlabel("Maturity Year")
                plt.ylabel("Number of Bonds")
                plt.grid(True)
                chart_file = DATA_DIR / "bonds_by_maturity_year.png"
                plt.savefig(chart_file)
                plt.close()
                logger.info(f"Saved maturity year chart to {chart_file}")
            else:
                logger.warning("No valid maturity dates found for charting")
        except Exception as e:
            logger.error(f"Error processing maturity dates: {e}")
            logger.info("Skipping maturity year analysis due to errors")
    else:
        logger.info("MATDATE column not found, skipping maturity year analysis")

def main():
    """Main function to run the example."""
    logger.info("Starting MOEX bond data fetcher example")
    
    try:
        # Fetch and save detailed bond data
        # Use a small sample size (3) for testing to speed up execution
        fetch_and_save_bond_data(sample_size=3)
        
        # Perform basic market analysis
        analyze_bond_market()
        
        logger.info("MOEX bond data fetcher example completed")
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        logger.error("MOEX bond data fetcher example failed")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()