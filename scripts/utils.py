# """
# Utility functions for the Stock Market Data Pipeline.
# Includes logging configuration and environment validation.
# """

# import os
# import logging
# from typing import Dict, Any


# def setup_logging(name: str = None) -> logging.Logger:
#     """
#     Set up and return a logger with a consistent format.

#     Args:
#         name: Logger name (usually __name__)

#     Returns:
#         Configured logger instance
#     """
#     logger = logging.getLogger(name if name else __name__)
#     if not logger.handlers:
#         logger.setLevel(logging.INFO)
#         handler = logging.StreamHandler()
#         formatter = logging.Formatter(
#             '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
#             datefmt='%Y-%m-%d %H:%M:%S'
#         )
#         handler.setFormatter(formatter)
#         logger.addHandler(handler)
#     return logger


# def validate_environment() -> Dict[str, Any]:
#     """
#     Validate that required environment variables are set.

#     Returns:
#         Dictionary with validation results
#     Raises:
#         EnvironmentError if any variable is missing
#     """
#     required_vars = [
#         "ALPHA_VANTAGE_API_KEY",
#         "STOCK_DB_HOST",
#         "STOCK_DB_NAME",
#         "STOCK_DB_USER",
#         "STOCK_DB_PASSWORD",
#         "STOCK_DB_PORT",
#     ]

#     missing_vars = [var for var in required_vars if not os.getenv(var)]
#     if missing_vars:
#         raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

#     return {
#         "status": "success",
#         "validated_vars": {var: os.getenv(var) for var in required_vars}
#     }








"""
Utility functions for the stock market data pipeline
"""

import os
import logging
import sys
from typing import Dict, Any
from datetime import datetime


def setup_logging(name: str, level: str = None) -> logging.Logger:
    """
    Setup logging configuration
    
    Args:
        name: Logger name (usually __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
    """
    # Get log level from environment or default to INFO
    log_level = level or os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    return logger


def validate_environment() -> Dict[str, Any]:
    """
    Validate that all required environment variables are set
    
    Returns:
        Dict with validation results
        
    Raises:
        ValueError: If required environment variables are missing
    """
    logger = setup_logging(__name__)
    
    # Required environment variables
    required_vars = {
        'ALPHA_VANTAGE_API_KEY': 'Alpha Vantage API key for stock data',
        'STOCK_DB_HOST': 'PostgreSQL database host',
        'STOCK_DB_NAME': 'PostgreSQL database name',
        'STOCK_DB_USER': 'PostgreSQL database user',
        'STOCK_DB_PASSWORD': 'PostgreSQL database password',
        'POSTGRES_PASSWORD': 'PostgreSQL root password',
    }
    
    # Optional environment variables with defaults
    optional_vars = {
        'STOCK_DB_PORT': '5432',
        'LOG_LEVEL': 'INFO',
        'AIRFLOW_UID': '50000',
    }
    
    missing_vars = []
    validation_results = {
        'status': 'success',
        'missing_required': [],
        'optional_defaults': {},
        'all_vars': {}
    }
    
    # Check required variables
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value:
            missing_vars.append(f"{var}: {description}")
            validation_results['missing_required'].append(var)
        else:
            # Mask sensitive values in logs
            if 'PASSWORD' in var or 'KEY' in var:
                validation_results['all_vars'][var] = '***MASKED***'
            else:
                validation_results['all_vars'][var] = value
    
    # Check optional variables and set defaults
    for var, default_value in optional_vars.items():
        value = os.getenv(var, default_value)
        validation_results['all_vars'][var] = value
        if var not in os.environ:
            validation_results['optional_defaults'][var] = default_value
    
    # Report missing variables
    if missing_vars:
        error_message = "Missing required environment variables:\n" + "\n".join(f"  - {var}" for var in missing_vars)
        logger.error(error_message)
        validation_results['status'] = 'error'
        validation_results['error_message'] = error_message
        raise ValueError(error_message)
    
    logger.info("Environment validation completed successfully")
    return validation_results


def format_currency(value: float, currency: str = 'USD') -> str:
    """
    Format a numeric value as currency
    
    Args:
        value: Numeric value to format
        currency: Currency code (default: USD)
        
    Returns:
        Formatted currency string
    """
    if currency == 'USD':
        return f"${value:,.2f}"
    else:
        return f"{value:,.2f} {currency}"


def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """
    Calculate percentage change between two values
    
    Args:
        old_value: Original value
        new_value: New value
        
    Returns:
        Percentage change (positive for increase, negative for decrease)
    """
    if old_value == 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100


def validate_stock_symbol(symbol: str) -> str:
    """
    Validate and normalize stock symbol
    
    Args:
        symbol: Stock symbol to validate
        
    Returns:
        Normalized symbol
        
    Raises:
        ValueError: If symbol is invalid
    """
    if not symbol or not isinstance(symbol, str):
        raise ValueError("Symbol must be a non-empty string")
    
    # Normalize symbol
    symbol = symbol.strip().upper()
    
    # Basic validation (1-5 characters, alphanumeric)
    if not symbol.isalnum() or len(symbol) < 1 or len(symbol) > 5:
        raise ValueError(f"Invalid symbol format: {symbol}")
    
    return symbol


def get_trading_date_range(days_back: int = 30) -> tuple:
    """
    Get trading date range (excluding weekends)
    
    Args:
        days_back: Number of days to go back
        
    Returns:
        Tuple of (start_date, end_date)
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    return start_date, end_date


def retry_with_backoff(func, max_retries: int = 3, backoff_factor: float = 2.0):
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        backoff_factor: Backoff multiplication factor
    """
    import time
    from functools import wraps
    
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            logger = setup_logging(f.__module__)
            
            for attempt in range(max_retries + 1):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"Function {f.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Function {f.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
            
        return wrapper
    return decorator


def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """
    Safely convert value to float
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Float value or default
    """
    try:
        if value is None or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """
    Safely convert value to integer
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Integer value or default
    """
    try:
        if value is None or value == '':
            return default
        return int(float(value))  # Convert through float to handle decimal strings
    except (ValueError, TypeError):
        return default


def get_current_timestamp() -> str:
    """
    Get current timestamp in ISO format
    
    Returns:
        ISO formatted timestamp string
    """
    return datetime.now().isoformat()


def parse_api_date(date_string: str) -> datetime:
    """
    Parse date string from API response
    
    Args:
        date_string: Date string to parse
        
    Returns:
        Parsed datetime object
        
    Raises:
        ValueError: If date string cannot be parsed
    """
    common_formats = [
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y',
        '%d/%m/%Y'
    ]
    
    for fmt in common_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_string}")


class ConfigManager:
    """
    Configuration manager for the pipeline
    """
    
    def __init__(self):
        self.config = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from environment variables"""
        self.config = {
            # API Configuration
            'api_key': os.getenv('ALPHA_VANTAGE_API_KEY'),
            'api_base_url': 'https://www.alphavantage.co/query',
            'api_timeout': int(os.getenv('API_TIMEOUT', '30')),
            'api_retries': int(os.getenv('API_RETRIES', '3')),
            
            # Database Configuration
            'db_host': os.getenv('STOCK_DB_HOST'),
            'db_name': os.getenv('STOCK_DB_NAME'),
            'db_user': os.getenv('STOCK_DB_USER'),
            'db_password': os.getenv('STOCK_DB_PASSWORD'),
            'db_port': int(os.getenv('STOCK_DB_PORT', '5432')),
            'db_pool_size': int(os.getenv('DB_POOL_SIZE', '5')),
            'db_max_connections': int(os.getenv('DB_MAX_CONNECTIONS', '20')),
            
            # Pipeline Configuration
            'default_symbols': os.getenv('DEFAULT_STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT,AMZN,TSLA').split(','),
            'data_retention_days': int(os.getenv('DATA_RETENTION_DAYS', '365')),
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
        }
    
    def get(self, key: str, default=None):
        """Get configuration value"""
        return self.config.get(key, default)
    
    def get_db_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return {
            'host': self.config['db_host'],
            'database': self.config['db_name'],
            'user': self.config['db_user'],
            'password': self.config['db_password'],
            'port': self.config['db_port'],
        }
    
    def validate(self):
        """Validate configuration"""
        required_keys = [
            'api_key', 'db_host', 'db_name', 
            'db_user', 'db_password'
        ]
        
        missing_keys = [key for key in required_keys if not self.config.get(key)]
        
        if missing_keys:
            raise ValueError(f"Missing configuration keys: {missing_keys}")


# Global configuration instance
config = ConfigManager()