#!/usr/bin/env python3
"""
MOEX ISS API Client

This module provides functionality to fetch Russian market data from MOEX ISS API.
Based on the ISS API documentation: https://fs.moex.com/files/8888
"""

import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
import os
import json
from typing import Dict, List, Optional, Union, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants for MOEX ISS API
BASE_URL = "http://iss.moex.com/iss"
HISTORY_URL = "http://iss.moex.com/iss/history"

# Engines
ENGINE_STOCK = "stock"
ENGINE_CURRENCY = "currency"
ENGINE_FUTURES = "futures"
ENGINE_COMMODITY = "commodity"

# Markets
MARKET_INDEX = "index"
MARKET_SHARES = "shares"
MARKET_BONDS = "bonds"
MARKET_REPO = "repo"

# Boards
BOARD_SNDX = "SNDX"  # Main indices board
BOARD_RTSI = "RTSI"  # RTS indices board
BOARD_TQBR = "TQBR"  # Main shares board
BOARD_TQCB = "TQCB"  # Main corporate bonds board
BOARD_TQOB = "TQOB"  # Main government bonds board

class MOEXDataSource:
    """
    Data source handler for MOEX ISS API.
    Provides access to Russian market data from Moscow Exchange.
    """
    
    # Base URLs
    BASE_URL = BASE_URL
    HISTORY_URL = HISTORY_URL
    
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize the MOEX data source.
        
        Args:
            username: Optional username for authenticated access
            password: Optional password for authenticated access
        """
        self.username = username
        self.password = password
        self.session = requests.Session()
        
        # Set up authentication if provided
        if username and password:
            self.session.auth = (username, password)
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """
        Make a request to the MOEX ISS API.
        
        Args:
            url: The URL to request
            params: Optional parameters to include in the request
            
        Returns:
            Response object
        """
        # Set default parameters
        if params is None:
            params = {}
        
        # Always request English language
        if 'lang' not in params:
            params['lang'] = 'en'
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            raise
    
    def _get_json_data(self, url: str, params: Optional[Dict] = None) -> Dict:
        """
        Get JSON data from the MOEX ISS API.
        
        Args:
            url: The URL to request
            params: Optional parameters to include in the request
            
        Returns:
            Dictionary with the JSON response
        """
        response = self._make_request(url + '.json', params)
        return response.json()
    
    def get_engines(self) -> pd.DataFrame:
        """
        Get the list of available engines.
        
        Returns:
            DataFrame with engine information
        """
        url = f"{self.BASE_URL}/engines"
        data = self._get_json_data(url)
        return pd.DataFrame(data['engines']['data'], columns=data['engines']['columns'])
    
    def get_markets(self, engine: str) -> pd.DataFrame:
        """
        Get the list of markets for a specific engine.
        
        Args:
            engine: Engine name (e.g., 'stock')
            
        Returns:
            DataFrame with market information
        """
        url = f"{self.BASE_URL}/engines/{engine}/markets"
        data = self._get_json_data(url)
        return pd.DataFrame(data['markets']['data'], columns=data['markets']['columns'])
    
    def get_boards(self, engine: str, market: str) -> pd.DataFrame:
        """
        Get the list of boards for a specific engine and market.
        
        Args:
            engine: Engine name (e.g., 'stock')
            market: Market name (e.g., 'index')
            
        Returns:
            DataFrame with board information
        """
        url = f"{self.BASE_URL}/engines/{engine}/markets/{market}/boards"
        data = self._get_json_data(url)
        return pd.DataFrame(data['boards']['data'], columns=data['boards']['columns'])
    
    def get_securities(self, engine: str, market: str, board: Optional[str] = None) -> pd.DataFrame:
        """
        Get the list of securities for a specific engine and market.
        
        Args:
            engine: Engine name (e.g., 'stock')
            market: Market name (e.g., 'index')
            board: Optional board name (e.g., 'TQBR')
            
        Returns:
            DataFrame with securities information
        """
        if board:
            url = f"{self.BASE_URL}/engines/{engine}/markets/{market}/boards/{board}/securities"
        else:
            url = f"{self.BASE_URL}/engines/{engine}/markets/{market}/securities"
        
        data = self._get_json_data(url)
        return pd.DataFrame(data['securities']['data'], columns=data['securities']['columns'])
    
    def get_historical_data(
        self, 
        engine: str, 
        market: str, 
        board: str, 
        security: str, 
        from_date: Optional[Union[str, datetime]] = None,
        till_date: Optional[Union[str, datetime]] = None,
        interval: int = 24  # Daily data
    ) -> pd.DataFrame:
        """
        Get historical data for a specific security.
        
        Args:
            engine: Engine name (e.g., 'stock')
            market: Market name (e.g., 'index')
            board: Board name (e.g., 'SNDX')
            security: Security ticker (e.g., 'IMOEX')
            from_date: Optional start date (default: 30 days ago)
            till_date: Optional end date (default: today)
            interval: Optional interval in hours (default: 24 for daily data)
            
        Returns:
            DataFrame with historical data
        """
        # Set default dates if not provided
        if from_date is None:
            from_date = datetime.now() - timedelta(days=30)
        if till_date is None:
            till_date = datetime.now()
        
        # Convert dates to strings if they are datetime objects
        if isinstance(from_date, datetime):
            from_date = from_date.strftime('%Y-%m-%d')
        if isinstance(till_date, datetime):
            till_date = till_date.strftime('%Y-%m-%d')
        
        # Prepare URL and parameters
        url = f"{self.HISTORY_URL}/engines/{engine}/markets/{market}/boards/{board}/securities/{security}"
        params = {
            'from': from_date,
            'till': till_date,
            'interval': interval
        }
        
        # Get data with pagination
        all_data = []
        start = 0
        
        while True:
            # Add start parameter for pagination
            params['start'] = start
            
            # Get data
            data = self._get_json_data(url, params)
            
            # Extract history data
            if 'history' not in data or not data['history']['data']:
                break
                
            # Convert to DataFrame
            df = pd.DataFrame(data['history']['data'], columns=data['history']['columns'])
            
            # Add to our collection
            all_data.append(df)
            
            # Check if we need to get more data
            if len(df) < 100:  # MOEX API returns max 100 records per request
                break
                
            # Move to next page
            start += 100
        
        # Combine all data
        if not all_data:
            return pd.DataFrame()
            
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # Convert date column to datetime
        if 'TRADEDATE' in combined_data.columns:
            combined_data['TRADEDATE'] = pd.to_datetime(combined_data['TRADEDATE'])
        
        return combined_data
    
    def get_index_data(self, index_name: str, from_date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
        """
        Get historical data for a specific index.
        
        Args:
            index_name: Index ticker (e.g., 'IMOEX', 'RGBITR')
            from_date: Optional start date (default: 30 days ago)
            
        Returns:
            DataFrame with historical index data
        """
        # Determine the appropriate board based on index name
        if index_name.startswith('RTS'):
            board = 'RTSI'
        else:
            board = 'SNDX'
        
        return self.get_historical_data(
            engine='stock',
            market='index',
            board=board,
            security=index_name,
            from_date=from_date
        )
    
    def get_bond_index_data(self, index_name: str, from_date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
        """
        Get historical data for a bond index.
        
        Args:
            index_name: Bond index ticker (e.g., 'RGBITR', 'RUCBITR')
            from_date: Optional start date (default: 30 days ago)
            
        Returns:
            DataFrame with historical bond index data
        """
        return self.get_index_data(index_name, from_date)