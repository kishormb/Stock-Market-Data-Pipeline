
# scripts/database_manager.py
"""
Stock Data Fetcher Module

This module handles fetching stock market data from Alpha Vantage API.
Includes error handling, rate limiting, and data validation.
"""

import os
import time
import requests
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import json
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils import setup_logging

logger = setup_logging(__name__)


class StockDataFetcher:
    """
    Handles fetching stock market data from Alpha Vantage API
    """
    
    def __init__(self):
        """Initialize the stock data fetcher"""
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.base_url = 'https://www.alphavantage.co/query'
        self.session = requests.Session()
        
        # Set session headers
        self.session.headers.update({
            'User-Agent': 'Stock-Market-Pipeline/1.0',
            'Accept': 'application/json',
        })
        
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")
        
        # Rate limiting settings (Alpha Vantage free tier: 5 calls per minute, 500 per day)
        self.calls_per_minute = 5
        self.last_call_times = []
        
        logger.info("StockDataFetcher initialized successfully")

    def _enforce_rate_limit(self) -> None:
        """
        Enforce API rate limiting to avoid exceeding API limits
        """
        current_time = time.time()
        
        # Remove calls older than 1 minute
        self.last_call_times = [
            call_time for call_time in self.last_call_times 
            if current_time - call_time < 60
        ]
        
        # If we've made too many calls in the last minute, wait
        if len(self.last_call_times) >= self.calls_per_minute:
            sleep_time = 60 - (current_time - self.last_call_times[0]) + 1
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        # Record this call
        self.last_call_times.append(current_time)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout))
    )
    def _make_api_request(self, params: Dict[str, str]) -> Dict[str, Any]:
        """
        Make API request with retry logic and error handling
        
        Args:
            params: API request parameters
            
        Returns:
            JSON response from API
            
        Raises:
            requests.RequestException: If API request fails after retries
            ValueError: If API returns error message
        """
        self._enforce_rate_limit()
        
        try:
            # Add API key to parameters
            params['apikey'] = self.api_key
            
            logger.debug(f"Making API request with params: {params}")
            
            response = self.session.get(
                self.base_url, 
                params=params, 
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            
            if 'Note' in data:
                if 'API call frequency' in data['Note']:
                    logger.warning("API rate limit reached, implementing backoff")
                    time.sleep(60)  # Wait 1 minute
                    raise requests.exceptions.RequestException("Rate limit reached")
                else:
                    logger.warning(f"API Note: {data['Note']}")
            
            logger.debug("API request completed successfully")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response: {e}")
            raise ValueError(f"Invalid JSON response: {e}")

    def fetch_daily_data(self, symbol: str, outputsize: str = 'compact') -> Optional[Dict[str, Any]]:
        """
        Fetch daily stock data for a given symbol
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            outputsize: 'compact' for last 100 days or 'full' for all data
            
        Returns:
            Dict containing stock data or None if failed
        """
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol.upper(),
                'outputsize': outputsize
            }
            
            logger.info(f"Fetching daily data for symbol: {symbol}")
            
            data = self._make_api_request(params)
            
            # Validate response structure
            if 'Time Series (Daily)' not in data:
                logger.warning(f"No time series data found for {symbol}")
                if 'Meta Data' in data:
                    logger.debug(f"Meta data: {data['Meta Data']}")
                return None
            
            # Log successful fetch
            time_series = data['Time Series (Daily)']
            logger.info(f"Successfully fetched {len(time_series)} daily records for {symbol}")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch daily data for {symbol}: {e}")
            return None

    def fetch_intraday_data(self, symbol: str, interval: str = '5min') -> Optional[Dict[str, Any]]:
        """
        Fetch intraday stock data for a given symbol
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            interval: Time interval ('1min', '5min', '15min', '30min', '60min')
            
        Returns:
            Dict containing intraday stock data or None if failed
        """
        try:
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol.upper(),
                'interval': interval,
                'outputsize': 'compact'
            }
            
            logger.info(f"Fetching intraday data for symbol: {symbol} with interval: {interval}")
            
            data = self._make_api_request(params)
            
            # Validate response structure
            time_series_key = f'Time Series ({interval})'
            if time_series_key not in data:
                logger.warning(f"No intraday time series data found for {symbol}")
                return None
            
            # Log successful fetch
            time_series = data[time_series_key]
            logger.info(f"Successfully fetched {len(time_series)} intraday records for {symbol}")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch intraday data for {symbol}: {e}")
            return None

    def fetch_quote_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch current quote data for a given symbol
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            
        Returns:
            Dict containing current quote data or None if failed
        """
        try:
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol.upper()
            }
            
            logger.info(f"Fetching quote data for symbol: {symbol}")
            
            data = self._make_api_request(params)
            
            # Validate response structure
            if 'Global Quote' not in data:
                logger.warning(f"No quote data found for {symbol}")
                return None
            
            logger.info(f"Successfully fetched quote data for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch quote data for {symbol}: {e}")
            return None

    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate if a stock symbol exists by making a quote request
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if symbol is valid, False otherwise
        """
        try:
            data = self.fetch_quote_data(symbol)
            if data and 'Global Quote' in data:
                quote = data['Global Quote']
                # Check if we have actual data (not empty values)
                if quote.get('05. price', '0.0000') != '0.0000':
                    logger.info(f"Symbol {symbol} is valid")
                    return True
            
            logger.warning(f"Symbol {symbol} appears to be invalid")
            return False
            
        except Exception as e:
            logger.error(f"Failed to validate symbol {symbol}: {e}")
            return False

    def get_api_status(self) -> Dict[str, Any]:
        """
        Check API status and remaining quota
        
        Returns:
            Dict with API status information
        """
        try:
            # Make a simple quote request to check API status
            data = self.fetch_quote_data('AAPL')
            
            if data:
                return {
                    'status': 'operational',
                    'last_check': datetime.now().isoformat(),
                    'remaining_calls_estimated': max(0, self.calls_per_minute - len(self.last_call_times))
                }
            else:
                return {
                    'status': 'error',
                    'last_check': datetime.now().isoformat(),
                    'remaining_calls_estimated': 0
                }
                
        except Exception as e:
            logger.error(f"Failed to check API status: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat(),
                'remaining_calls_estimated': 0
            }

    def __del__(self):
        """Cleanup when object is destroyed"""
        if hasattr(self, 'session'):
            self.session.close()
        logger.info("StockDataFetcher instance destroyed")
        self.session = None

# Example usage:
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    symbol = 'AAPL'
    
    daily_data = fetcher.fetch_daily_data(symbol)
    if daily_data:
        print(f"Daily data for {symbol}: {daily_data['Time Series (Daily)']}")
    
    intraday_data = fetcher.fetch_intraday_data(symbol, '5min')
    if intraday_data:
        print(f"Intraday data for {symbol}: {intraday_data[f'Time Series (5min)']}")
    
    quote_data = fetcher.fetch_quote_data(symbol)
    if quote_data:
        print(f"Quote data for {symbol}: {quote_data['Global Quote']}")
    
    is_valid = fetcher.validate_symbol(symbol)
    print(f"Is symbol {symbol} valid? {is_valid}")
    
    api_status = fetcher.get_api_status()
    print(f"API Status: {api_status}")