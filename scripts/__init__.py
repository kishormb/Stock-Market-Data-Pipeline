# """
# Scripts package for Stock Market Data Pipeline.

# This package contains:
# - stock_fetcher: Fetches stock data from APIs (Alpha Vantage, etc.)
# - database_manager: Handles PostgreSQL operations with connection pooling
# - utils: Common utilities like logging and environment validation
# """

# from .stock_fetcher import StockDataFetcher
# from .database_manager import DatabaseManager
# from .utils import setup_logging, validate_environment

# __all__ = [
#     "StockDataFetcher",
#     "DatabaseManager",
#     "setup_logging",
#     "validate_environment",
# ]
# # scripts/__init__.py
# # This file initializes the scripts package for the Stock Market Data Pipeline.     






#_# scripts/__init__.py

"""
Stock Market Data Pipeline Scripts

This package contains all the utility modules for the stock market data pipeline.
"""

from .stock_fetcher import StockDataFetcher
from .database_manager import DatabaseManager
from .utils import setup_logging, validate_environment, ConfigManager

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

__all__ = [
    "StockDataFetcher",
    "DatabaseManager", 
    "setup_logging",
    "validate_environment",
    "ConfigManager"
]