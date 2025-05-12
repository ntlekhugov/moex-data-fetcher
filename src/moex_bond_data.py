"""
MOEX Bond Data Fetcher

This module provides specialized functions for fetching detailed bond data from the
Moscow Exchange (MOEX) Information & Statistical Server (ISS) API, including:
- Bond instrument parameters
- Daily trading results
- Coupon payment schedules
- Amortization schedules
- Offer (put/call) information

Designed specifically for Russian domestic bonds.
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Union, List, Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MOEXBondData:
    """
    Class for fetching detailed bond data from MOEX ISS API.
    Focuses on Russian domestic bonds and their lifecycle events.
    """
    
    BASE_URL = "https://iss.moex.com/iss"
    
    def __init__(self):
        """Initialize the MOEX Bond Data fetcher."""
        self.session = requests.Session()
    
    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a request to the MOEX ISS API.
        
        Args:
            url: API endpoint URL
            params: Optional query parameters
            
        Returns:
            Dictionary containing the API response
        """
        # Ensure URL ends with .json to get JSON response format
        if not url.endswith('.json'):
            url = f"{url}.json"
            
        default_params = {
            'iss.meta': 'off',
            'lang': 'ru'  # Can be changed to 'en' for English
        }
        
        if params:
            default_params.update(params)
            
        try:
            response = self.session.get(url, params=default_params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {e}")
            raise
    
    def get_bond_parameters(self, isin: str) -> pd.DataFrame:
        """
        Get detailed parameters for a specific bond by its ISIN.
        
        Args:
            isin: International Securities Identification Number
            
        Returns:
            DataFrame containing bond parameters
        """
        # First get the security ID from the ISIN
        security_info = self._get_security_info_by_isin(isin)
        
        if not security_info:
            logger.error(f"Could not find security info for ISIN {isin}")
            return pd.DataFrame()
        
        secid = security_info.get('secid')
        
        # Now fetch the security details using the security ID
        url = f"{self.BASE_URL}/securities/{secid}"
        response = self._make_request(url)
        
        # The securities endpoint returns multiple data blocks
        # We'll combine them into a single DataFrame for easier use
        result_dfs = []
        
        # Process each data block in the response
        for block_name, block_data in response.items():
            if 'data' in block_data and block_data['data']:
                df = pd.DataFrame(
                    data=block_data['data'],
                    columns=block_data['columns']
                )
                # Add a column to identify which data block this came from
                df['data_block'] = block_name
                result_dfs.append(df)
        
        if result_dfs:
            # Combine all data blocks into a single DataFrame
            return pd.concat(result_dfs, ignore_index=True)
        else:
            logger.warning(f"No parameter data found for bond {isin}")
            return pd.DataFrame()
    
    def get_bond_daily_trading(self, 
                              isin: str, 
                              from_date: Optional[Union[str, datetime]] = None,
                              till_date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
        """
        Get daily trading results for a specific bond.
        
        Args:
            isin: International Securities Identification Number
            from_date: Start date for data retrieval (inclusive)
            till_date: End date for data retrieval (inclusive)
            
        Returns:
            DataFrame containing daily trading results
        """
        # First, get the security ID and board ID from the ISIN
        security_info = self._get_security_info_by_isin(isin)
        
        if not security_info:
            logger.error(f"Could not find security info for ISIN {isin}")
            return pd.DataFrame()
        
        secid = security_info.get('secid')
        board = security_info.get('primary_boardid', 'TQCB')  # Default to TQCB (corporate bonds)
        
        # Format dates
        from_date_str = None
        till_date_str = None
        
        if from_date:
            if isinstance(from_date, str):
                from_date_str = from_date
            else:
                from_date_str = from_date.strftime('%Y-%m-%d')
                
        if till_date:
            if isinstance(till_date, str):
                till_date_str = till_date
            else:
                till_date_str = till_date.strftime('%Y-%m-%d')
        
        # Construct URL for historical data
        url = f"{self.BASE_URL}/history/engines/stock/markets/bonds/boards/{board}/securities/{secid}"
        
        params = {}
        if from_date_str:
            params['from'] = from_date_str
        if till_date_str:
            params['till'] = till_date_str
            
        # Make the request
        response = self._make_request(url, params)
        
        # Process the response
        if 'history' in response and 'data' in response['history']:
            df = pd.DataFrame(
                data=response['history']['data'],
                columns=response['history']['columns']
            )
            return df
        else:
            logger.warning(f"No historical data found for bond {isin}")
            return pd.DataFrame()
    
    def get_bond_coupons(self, isin: str) -> pd.DataFrame:
        """
        Get coupon payment schedule for a specific bond.
        
        Args:
            isin: International Securities Identification Number
            
        Returns:
            DataFrame containing coupon payment schedule
        """
        # Get security ID first
        security_info = self._get_security_info_by_isin(isin)
        
        if not security_info:
            logger.error(f"Could not find security info for ISIN {isin}")
            return pd.DataFrame()
        
        secid = security_info.get('secid')
        
        # Use the bondization endpoint
        url = f"{self.BASE_URL}/statistics/engines/stock/markets/bonds/bondization/{secid}"
        response = self._make_request(url)
        
        # Extract coupon data
        if 'coupons' in response and 'data' in response['coupons']:
            df = pd.DataFrame(
                data=response['coupons']['data'],
                columns=response['coupons']['columns']
            )
            return df
        else:
            logger.warning(f"No coupon data found for bond {isin}")
            return pd.DataFrame()
    
    def get_bond_amortizations(self, isin: str) -> pd.DataFrame:
        """
        Get amortization (principal repayment) schedule for a specific bond.
        
        Args:
            isin: International Securities Identification Number
            
        Returns:
            DataFrame containing amortization schedule
        """
        # Get security ID first
        security_info = self._get_security_info_by_isin(isin)
        
        if not security_info:
            logger.error(f"Could not find security info for ISIN {isin}")
            return pd.DataFrame()
        
        secid = security_info.get('secid')
        
        # Use the bondization endpoint
        url = f"{self.BASE_URL}/statistics/engines/stock/markets/bonds/bondization/{secid}"
        response = self._make_request(url)
        
        # Extract amortization data
        if 'amortizations' in response and 'data' in response['amortizations']:
            df = pd.DataFrame(
                data=response['amortizations']['data'],
                columns=response['amortizations']['columns']
            )
            return df
        else:
            logger.warning(f"No amortization data found for bond {isin}")
            return pd.DataFrame()
    
    def get_bond_offers(self, isin: str) -> pd.DataFrame:
        """
        Get offer (put/call) information for a specific bond.
        
        Args:
            isin: International Securities Identification Number
            
        Returns:
            DataFrame containing offer information
        """
        # Get security ID first
        security_info = self._get_security_info_by_isin(isin)
        
        if not security_info:
            logger.error(f"Could not find security info for ISIN {isin}")
            return pd.DataFrame()
        
        secid = security_info.get('secid')
        
        # Use the bondization endpoint
        url = f"{self.BASE_URL}/statistics/engines/stock/markets/bonds/bondization/{secid}"
        response = self._make_request(url)
        
        # Extract offer data
        if 'offers' in response and 'data' in response['offers']:
            df = pd.DataFrame(
                data=response['offers']['data'],
                columns=response['offers']['columns']
            )
            return df
        else:
            logger.warning(f"No offer data found for bond {isin}")
            return pd.DataFrame()
    
    def find_russian_domestic_bonds(self, 
                                   from_date: Optional[Union[str, datetime]] = None,
                                   till_date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
        """
        Find Russian domestic bonds that are active within the specified date range.
        
        Args:
            from_date: Start date for filtering (inclusive)
            till_date: End date for filtering (inclusive)
            
        Returns:
            DataFrame containing bond information
        """
        # Default dates if not provided
        if not from_date:
            from_date = datetime.now() - timedelta(days=30)
        if not till_date:
            till_date = datetime.now() + timedelta(days=30)
            
        # Format dates
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, '%Y-%m-%d')
        if isinstance(till_date, str):
            till_date = datetime.strptime(till_date, '%Y-%m-%d')
            
        # Get list of all bonds
        url = f"{self.BASE_URL}/engines/stock/markets/bonds/securities"
        response = self._make_request(url)
        
        if 'securities' in response and 'data' in response['securities']:
            all_bonds = pd.DataFrame(
                data=response['securities']['data'],
                columns=response['securities']['columns']
            )
            
            # Filter for Russian domestic bonds
            # Typically, Russian domestic bonds have ISIN starting with "RU"
            russian_bonds = all_bonds[all_bonds['ISIN'].str.startswith('RU', na=False)]
            
            # Further filtering could be done here based on specific requirements
            # For example, filtering by issue date, maturity date, etc.
            
            return russian_bonds
        else:
            logger.warning("No bond data found")
            return pd.DataFrame()
    
    def get_complete_bond_data(self, isin: str) -> Dict[str, pd.DataFrame]:
        """
        Get complete data for a specific bond, including parameters, trading history,
        coupons, amortizations, and offers.
        
        Args:
            isin: International Securities Identification Number
            
        Returns:
            Dictionary containing DataFrames for each data type
        """
        result = {
            'parameters': self.get_bond_parameters(isin),
            'trading': self.get_bond_daily_trading(isin),
            'coupons': self.get_bond_coupons(isin),
            'amortizations': self.get_bond_amortizations(isin),
            'offers': self.get_bond_offers(isin)
        }
        return result
    
    def _get_security_info_by_isin(self, isin: str) -> Dict[str, str]:
        """
        Get security ID and board ID for a given ISIN.
        
        Args:
            isin: International Securities Identification Number
            
        Returns:
            Dictionary with security information
        """
        url = f"{self.BASE_URL}/securities"
        params = {'q': isin}
        
        response = self._make_request(url, params)
        
        if 'securities' in response and 'data' in response['securities']:
            securities = response['securities']['data']
            columns = response['securities']['columns']
            
            if securities:
                # Convert to DataFrame for easier handling
                df = pd.DataFrame(data=securities, columns=columns)
                
                # Find the row with matching ISIN
                isin_col_idx = columns.index('isin')
                secid_col_idx = columns.index('secid')
                primary_board_col_idx = columns.index('primary_boardid') if 'primary_boardid' in columns else None
                
                for row in securities:
                    if row[isin_col_idx] == isin:
                        result = {'secid': row[secid_col_idx]}
                        if primary_board_col_idx is not None:
                            result['primary_boardid'] = row[primary_board_col_idx]
                        return result
        
        logger.warning(f"Could not find security info for ISIN {isin}")
        return {}