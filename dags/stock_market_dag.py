# # """
# # Stock Market Data Pipeline DAG

# # This DAG orchestrates the fetching, processing, and storage of stock market data
# # using Alpha Vantage API and PostgreSQL database.
# # """

# # from datetime import datetime, timedelta
# # from typing import Dict, Any
# # import os

# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from airflow.operators.bash import BashOperator
# # from airflow.utils.dates import days_ago
# # from airflow.models import Variable
# # from airflow.utils.task_group import TaskGroup

# # # Import custom modules
# # from scripts.stock_fetcher import StockDataFetcher
# # from scripts.database_manager import DatabaseManager
# # from scripts.utils import setup_logging, validate_environment

# # # Setup logging
# # logger = setup_logging(__name__)

# # # Default arguments for the DAG
# # default_args = {
# #     'owner': 'data-engineering-team',
# #     'depends_on_past': False,
# #     'start_date': days_ago(1),
# #     'email_on_failure': True,
# #     'email_on_retry': False,
# #     'retries': 3,
# #     'retry_delay': timedelta(minutes=5),
# #     'max_active_runs': 1,
# # }

# # # DAG definition
# # dag = DAG(
# #     'stock_market_data_pipeline',
# #     default_args=default_args,
# #     description='Fetches and processes stock market data from Alpha Vantage API',
# #     schedule_interval=timedelta(hours=1),  # Run every hour
# #     catchup=False,
# #     max_active_runs=1,
# #     tags=['stock-market', 'data-pipeline', 'etl'],
# # )

# # # Stock symbols to track (can be configured via Airflow Variables)
# # def get_stock_symbols() -> list:
# #     """Get stock symbols from Airflow Variables or use default"""
# #     try:
# #         symbols = Variable.get("stock_symbols", default_var="AAPL,GOOGL,MSFT,AMZN,TSLA")
# #         return [symbol.strip() for symbol in symbols.split(',')]
# #     except Exception as e:
# #         logger.warning(f"Failed to get symbols from Variables: {e}")
# #         return ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']


# # def validate_environment_task(**context) -> Dict[str, Any]:
# #     """
# #     Validate that all required environment variables are set
# #     """
# #     try:
# #         validation_result = validate_environment()
# #         logger.info("Environment validation completed successfully")
# #         return validation_result
# #     except Exception as e:
# #         logger.error(f"Environment validation failed: {e}")
# #         raise


# # def fetch_stock_data_task(symbol: str, **context) -> Dict[str, Any]:
# #     """
# #     Fetch stock data for a specific symbol
    
# #     Args:
# #         symbol: Stock symbol to fetch data for
# #         **context: Airflow context
        
# #     Returns:
# #         Dict containing fetched data and metadata
# #     """
# #     try:
# #         # Initialize stock data fetcher
# #         fetcher = StockDataFetcher()
        
# #         # Get run information
# #         run_id = context['run_id']
# #         task_id = context['task_instance'].task_id
        
# #         logger.info(f"Fetching data for symbol: {symbol}")
        
# #         # Fetch data
# #         data = fetcher.fetch_daily_data(symbol)
        
# #         if not data or 'Time Series (Daily)' not in data:
# #             logger.warning(f"No data found for symbol: {symbol}")
# #             return {
# #                 'symbol': symbol,
# #                 'status': 'no_data',
# #                 'records_count': 0,
# #                 'data': None
# #             }
        
# #         time_series = data['Time Series (Daily)']
# #         records_count = len(time_series)
        
# #         logger.info(f"Successfully fetched {records_count} records for {symbol}")
        
# #         return {
# #             'symbol': symbol,
# #             'status': 'success',
# #             'records_count': records_count,
# #             'data': time_series,
# #             'metadata': data.get('Meta Data', {})
# #         }
        
# #     except Exception as e:
# #         logger.error(f"Failed to fetch data for {symbol}: {e}")
# #         return {
# #             'symbol': symbol,
# #             'status': 'error',
# #             'error': str(e),
# #             'records_count': 0,
# #             'data': None
# #         }


# # def process_and_store_data_task(symbol: str, **context) -> Dict[str, Any]:
# #     """
# #     Process and store the fetched stock data
    
# #     Args:
# #         symbol: Stock symbol
# #         **context: Airflow context
        
# #     Returns:
# #         Dict containing processing results
# #     """
# #     try:
# #         # Get the data from the previous task
# #         ti = context['task_instance']
# #         fetch_result = ti.xcom_pull(task_ids=f'fetch_data_group.fetch_{symbol.lower()}_data')
        
# #         if not fetch_result or fetch_result['status'] != 'success':
# #             logger.warning(f"No valid data to process for {symbol}")
# #             return {
# #                 'symbol': symbol,
# #                 'status': 'skipped',
# #                 'records_processed': 0
# #             }
        
# #         # Initialize database manager
# #         db_manager = DatabaseManager()
        
# #         # Process and store data
# #         result = db_manager.store_daily_data(
# #             symbol=symbol,
# #             data=fetch_result['data'],
# #             metadata=fetch_result.get('metadata', {})
# #         )
        
# #         logger.info(f"Processed {result['records_processed']} records for {symbol}")
        
# #         return result
        
# #     except Exception as e:
# #         logger.error(f"Failed to process data for {symbol}: {e}")
# #         raise


# # def data_quality_check_task(**context) -> Dict[str, Any]:
# #     """
# #     Perform data quality checks on the stored data
# #     """
# #     try:
# #         db_manager = DatabaseManager()
        
# #         # Get processing results from all symbols
# #         ti = context['task_instance']
# #         symbols = get_stock_symbols()
        
# #         total_records = 0
# #         processed_symbols = []
        
# #         for symbol in symbols:
# #             try:
# #                 store_result = ti.xcom_pull(task_ids=f'process_data_group.store_{symbol.lower()}_data')
# #                 if store_result and store_result.get('status') == 'success':
# #                     total_records += store_result.get('records_processed', 0)
# #                     processed_symbols.append(symbol)
# #             except Exception as e:
# #                 logger.warning(f"Could not get result for {symbol}: {e}")
        
# #         # Perform quality checks
# #         quality_results = db_manager.perform_data_quality_checks(processed_symbols)
        
# #         logger.info(f"Data quality check completed. Total records: {total_records}")
        
# #         return {
# #             'status': 'success',
# #             'total_records_processed': total_records,
# #             'symbols_processed': processed_symbols,
# #             'quality_checks': quality_results
# #         }
        
# #     except Exception as e:
# #         logger.error(f"Data quality check failed: {e}")
# #         raise


# # def cleanup_task(**context) -> None:
# #     """
# #     Cleanup old data and logs
# #     """
# #     try:
# #         db_manager = DatabaseManager()
        
# #         # Cleanup old pipeline runs (keep last 30 days)
# #         cleanup_date = datetime.now() - timedelta(days=30)
# #         rows_deleted = db_manager.cleanup_old_pipeline_runs(cleanup_date)
        
# #         logger.info(f"Cleanup completed. Deleted {rows_deleted} old pipeline run records")
        
# #     except Exception as e:
# #         logger.error(f"Cleanup task failed: {e}")
# #         # Don't raise here as cleanup failure shouldn't fail the entire pipeline


# # # Create tasks
# # validate_env_task = PythonOperator(
# #     task_id='validate_environment',
# #     python_callable=validate_environment_task,
# #     dag=dag,
# # )

# # # Task group for data fetching
# # with TaskGroup('fetch_data_group', dag=dag) as fetch_data_group:
# #     symbols = get_stock_symbols()
# #     fetch_tasks = []
    
# #     for symbol in symbols:
# #         fetch_task = PythonOperator(
# #             task_id=f'fetch_{symbol.lower()}_data',
# #             python_callable=fetch_stock_data_task,
# #             op_kwargs={'symbol': symbol},
# #             provide_context=True,
# #             dag=dag,
# #         )
# #         fetch_tasks.append(fetch_task)
# #     fetch_data_group.set_downstream(fetch_tasks)
# # # Task group for data processing and storage
# # with TaskGroup('process_data_group', dag=dag) as process_data_group:
# #     symbols = get_stock_symbols()
# #     process_tasks = []
    
# #     for symbol in symbols:
# #         process_task = PythonOperator(
# #             task_id=f'store_{symbol.lower()}_data',
# #             python_callable=process_and_store_data_task,
# #             op_kwargs={'symbol': symbol},
# #             provide_context=True,
# #             dag=dag,
# #         )
# #         process_tasks.append(process_task)
# #     process_data_group.set_downstream(process_tasks)        
# # # Data quality check task
# # data_quality_check = PythonOperator(
# #     task_id='data_quality_check',
# #     python_callable=data_quality_check_task,
# #     provide_context=True,
# #     dag=dag,
# # )
# # # Cleanup task
# # cleanup = PythonOperator(
# #     task_id='cleanup_old_data',
# #     python_callable=cleanup_task,
# #     provide_context=True,
# #     dag=dag,
# # )
# # # Define task dependencies
# # validate_env_task >> fetch_data_group >> process_data_group >> data_quality_check >> cleanup    






# #stock_market_dag.py
# """
# Stock Market Data Pipeline DAG

# This DAG orchestrates the fetching, processing, and storage of stock market data
# using Alpha Vantage API and PostgreSQL database.
# """

# from datetime import datetime, timedelta
# from typing import Dict, Any
# import os

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.utils.dates import days_ago
# from airflow.models import Variable
# from airflow.utils.task_group import TaskGroup

# # Import custom modules
# from scripts.stock_fetcher import StockDataFetcher
# from scripts.database_manager import DatabaseManager
# from scripts.utils import setup_logging, validate_environment

# # Setup logging
# logger = setup_logging(__name__)

# # Default arguments for the DAG
# default_args = {
#     'owner': 'data-engineering-team',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
#     'max_active_runs': 1,
# }

# # DAG definition
# dag = DAG(
#     'stock_market_data_pipeline',
#     default_args=default_args,
#     description='Fetches and processes stock market data from Alpha Vantage API',
#     schedule_interval=timedelta(hours=1),  # Run every hour
#     catchup=False,
#     max_active_runs=1,
#     tags=['stock-market', 'data-pipeline', 'etl'],
# )

# # Stock symbols to track (can be configured via Airflow Variables)
# def get_stock_symbols() -> list:
#     """Get stock symbols from Airflow Variables or use default"""
#     try:
#         symbols = Variable.get("stock_symbols", default_var="AAPL,GOOGL,MSFT,AMZN,TSLA")
#         return [symbol.strip() for symbol in symbols.split(',')]
#     except Exception as e:
#         logger.warning(f"Failed to get symbols from Variables: {e}")
#         return ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']


# def validate_environment_task(**context) -> Dict[str, Any]:
#     """
#     Validate that all required environment variables are set
#     """
#     try:
#         validation_result = validate_environment()
#         logger.info("Environment validation completed successfully")
#         return validation_result
#     except Exception as e:
#         logger.error(f"Environment validation failed: {e}")
#         raise


# def fetch_stock_data_task(symbol: str, **context) -> Dict[str, Any]:
#     """
#     Fetch stock data for a specific symbol
    
#     Args:
#         symbol: Stock symbol to fetch data for
#         **context: Airflow context
        
#     Returns:
#         Dict containing fetched data and metadata
#     """
#     try:
#         # Initialize stock data fetcher
#         fetcher = StockDataFetcher()
        
#         # Get run information
#         run_id = context['run_id']
#         task_id = context['task_instance'].task_id
        
#         logger.info(f"Fetching data for symbol: {symbol}")
        
#         # Fetch data
#         data = fetcher.fetch_daily_data(symbol)
        
#         if not data or 'Time Series (Daily)' not in data:
#             logger.warning(f"No data found for symbol: {symbol}")
#             return {
#                 'symbol': symbol,
#                 'status': 'no_data',
#                 'records_count': 0,
#                 'data': None
#             }
        
#         time_series = data['Time Series (Daily)']
#         records_count = len(time_series)
        
#         logger.info(f"Successfully fetched {records_count} records for {symbol}")
        
#         return {
#             'symbol': symbol,
#             'status': 'success',
#             'records_count': records_count,
#             'data': time_series,
#             'metadata': data.get('Meta Data', {})
#         }
        
#     except Exception as e:
#         logger.error(f"Failed to fetch data for {symbol}: {e}")
#         return {
#             'symbol': symbol,
#             'status': 'error',
#             'error': str(e),
#             'records_count': 0,
#             'data': None
#         }


# def process_and_store_data_task(symbol: str, **context) -> Dict[str, Any]:
#     """
#     Process and store the fetched stock data
    
#     Args:
#         symbol: Stock symbol
#         **context: Airflow context
        
#     Returns:
#         Dict containing processing results
#     """
#     try:
#         # Get the data from the previous task
#         ti = context['task_instance']
#         fetch_result = ti.xcom_pull(task_ids=f'fetch_data_group.fetch_{symbol.lower()}_data')
        
#         if not fetch_result or fetch_result['status'] != 'success':
#             logger.warning(f"No valid data to process for {symbol}")
#             return {
#                 'symbol': symbol,
#                 'status': 'skipped',
#                 'records_processed': 0
#             }
        
#         # Initialize database manager
#         db_manager = DatabaseManager()
        
#         # Process and store data
#         result = db_manager.store_daily_data(
#             symbol=symbol,
#             data=fetch_result['data'],
#             metadata=fetch_result.get('metadata', {})
#         )
        
#         logger.info(f"Processed {result['records_processed']} records for {symbol}")
        
#         return result
        
#     except Exception as e:
#         logger.error(f"Failed to process data for {symbol}: {e}")
#         raise


# def data_quality_check_task(**context) -> Dict[str, Any]:
#     """
#     Perform data quality checks on the stored data
#     """
#     try:
#         db_manager = DatabaseManager()
        
#         # Get processing results from all symbols
#         ti = context['task_instance']
#         symbols = get_stock_symbols()
        
#         total_records = 0
#         processed_symbols = []
        
#         for symbol in symbols:
#             try:
#                 store_result = ti.xcom_pull(task_ids=f'process_data_group.store_{symbol.lower()}_data')
#                 if store_result and store_result.get('status') == 'success':
#                     total_records += store_result.get('records_processed', 0)
#                     processed_symbols.append(symbol)
#             except Exception as e:
#                 logger.warning(f"Could not get result for {symbol}: {e}")
        
#         # Perform quality checks
#         quality_results = db_manager.perform_data_quality_checks(processed_symbols)
        
#         logger.info(f"Data quality check completed. Total records: {total_records}")
        
#         return {
#             'status': 'success',
#             'total_records_processed': total_records,
#             'symbols_processed': processed_symbols,
#             'quality_checks': quality_results
#         }
        
#     except Exception as e:
#         logger.error(f"Data quality check failed: {e}")
#         raise


# def cleanup_task(**context) -> None:
#     """
#     Cleanup old data and logs
#     """
#     try:
#         db_manager = DatabaseManager()
        
#         # Cleanup old pipeline runs (keep last 30 days)
#         cleanup_date = datetime.now() - timedelta(days=30)
#         rows_deleted = db_manager.cleanup_old_pipeline_runs(cleanup_date)
        
#         logger.info(f"Cleanup completed. Deleted {rows_deleted} old pipeline run records")
        
#     except Exception as e:
#         logger.error(f"Cleanup task failed: {e}")
#         # Don't raise here as cleanup failure shouldn't fail the entire pipeline


# # Create tasks
# validate_env_task = PythonOperator(
#     task_id='validate_environment',
#     python_callable=validate_environment_task,
#     dag=dag,
# )

# # Task group for data fetching
# with TaskGroup('fetch_data_group', dag=dag) as fetch_data_group:
#     symbols = get_stock_symbols()
#     fetch_tasks = []
    
#     for symbol in symbols:
#         fetch_task = PythonOperator(
#             task_id=f'fetch_{symbol.lower()}_data',
#             python_callable=fetch_stock_data_task,
#             op_kwargs={'symbol': symbol},
#             dag=dag,
#         )
#         fetch_tasks.append(fetch_task)

# # Task group for data processing and storage
# with TaskGroup('process_data_group', dag=dag) as process_data_group:
#     symbols = get_stock_symbols()
#     process_tasks = []
    
#     for symbol in symbols:
#         process_task = PythonOperator(
#             task_id=f'store_{symbol.lower()}_data',
#             python_callable=process_and_store_data_task,
#             op_kwargs={'symbol': symbol},
#             dag=dag,
#         )
#         process_tasks.append(process_task)

# # Data quality check task
# data_quality_task = PythonOperator(
#     task_id='data_quality_check',
#     python_callable=data_quality_check_task,
#     dag=dag,
# )

# # Cleanup task
# cleanup_task_op = PythonOperator(
#     task_id='cleanup_old_data',
#     python_callable=cleanup_task,
#     dag=dag,
#     trigger_rule='all_done',  # Run even if other tasks fail
# )

# # Health check task
# health_check_task = BashOperator(
#     task_id='health_check',
#     bash_command="""
#     echo "Pipeline run completed at $(date)"
#     echo "Checking database connectivity..."
#     python -c "
# from scripts.database_manager import DatabaseManager
# try:
#     db = DatabaseManager()
#     db.test_connection()
#     print('✓ Database connection successful')
# except Exception as e:
#     print(f'✗ Database connection failed: {e}')
#     exit(1)
# "
#     """,
#     dag=dag,
# )

# # Define task dependencies
# validate_env_task >> fetch_data_group >> process_data_group >> data_quality_task >> [cleanup_task_op, health_check_task]















#stock_market_dag.py

"""
Database Manager Module

This module handles all database operations for the stock market data pipeline.
Includes connection pooling, data validation, and CRUD operations.
"""

import os
import psycopg2
from psycopg2 import pool, extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal
import pandas as pd
from contextlib import contextmanager
from utils import setup_logging

logger = setup_logging(__name__)


class DatabaseManager:
    """
    Manages database connections and operations for stock market data
    """
    
    def __init__(self, pool_size: int = 5, max_connections: int = 20):
        """
        Initialize database manager with connection pooling
        
        Args:
            pool_size: Minimum number of connections in pool
            max_connections: Maximum number of connections in pool
        """
        # Database configuration
        self.config = {
            'host': os.getenv('STOCK_DB_HOST', 'localhost'),
            'database': os.getenv('STOCK_DB_NAME', 'stock_data'),
            'user': os.getenv('STOCK_DB_USER', 'postgres'),
            'password': os.getenv('STOCK_DB_PASSWORD', 'password'),
            'port': int(os.getenv('STOCK_DB_PORT', '5432')),
        }
        
        # Validate configuration
        for key, value in self.config.items():
            if not value:
                raise ValueError(f"Database configuration missing: {key}")
        
        # Initialize connection pool
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                pool_size, 
                max_connections,
                **self.config,
                cursor_factory=extras.RealDictCursor
            )
            logger.info("Database connection pool initialized successfully")
            
            # Test connection
            self.test_connection()
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        """
        connection = None
        try:
            connection = self.pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if connection:
                self.pool.putconn(connection)

    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info("Database connection test successful")
                    return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def store_daily_data(self, symbol: str, data: Dict[str, Dict], metadata: Dict = None) -> Dict[str, Any]:
        """
        Store daily stock data in the database
        
        Args:
            symbol: Stock symbol
            data: Time series data from API
            metadata: Optional metadata from API response
            
        Returns:
            Dict with operation results
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    records_processed = 0
                    records_updated = 0
                    records_inserted = 0
                    errors = []
                    
                    for date_str, price_data in data.items():
                        try:
                            # Parse and validate data
                            trade_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                            
                            # Extract price data with validation
                            open_price = self._validate_decimal(price_data.get('1. open'))
                            high_price = self._validate_decimal(price_data.get('2. high'))
                            low_price = self._validate_decimal(price_data.get('3. low'))
                            close_price = self._validate_decimal(price_data.get('4. close'))
                            volume = self._validate_integer(price_data.get('5. volume'))
                            
                            # Validate price relationships
                            if not self._validate_price_relationships(open_price, high_price, low_price, close_price):
                                logger.warning(f"Invalid price relationships for {symbol} on {date_str}")
                                continue
                            
                            # Insert or update record
                            insert_query = """
                            INSERT INTO stock_market.daily_prices 
                            (symbol, date, open_price, high_price, low_price, close_price, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, date) 
                            DO UPDATE SET 
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close_price = EXCLUDED.close_price,
                                volume = EXCLUDED.volume,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING (xmax = 0) AS inserted
                            """
                            
                            cursor.execute(insert_query, (
                                symbol.upper(),
                                trade_date,
                                open_price,
                                high_price,
                                low_price,
                                close_price,
                                volume
                            ))
                            
                            result = cursor.fetchone()
                            if result['inserted']:
                                records_inserted += 1
                            else:
                                records_updated += 1
                            
                            records_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Failed to process record for {date_str}: {e}"
                            logger.error(error_msg)
                            errors.append(error_msg)
                            continue
                    
                    # Commit transaction
                    conn.commit()
                    
                    logger.info(f"Successfully processed {records_processed} records for {symbol} "
                              f"(inserted: {records_inserted}, updated: {records_updated})")
                    
                    return {
                        'status': 'success',
                        'symbol': symbol,
                        'records_processed': records_processed,
                        'records_inserted': records_inserted,
                        'records_updated': records_updated,
                        'errors': errors
                    }
                    
        except Exception as e:
            logger.error(f"Failed to store daily data for {symbol}: {e}")
            return {
                'status': 'error',
                'symbol': symbol,
                'error': str(e),
                'records_processed': 0
            }

    def store_intraday_data(self, symbol: str, data: Dict[str, Dict], interval: str) -> Dict[str, Any]:
        """
        Store intraday stock data in the database
        
        Args:
            symbol: Stock symbol
            data: Time series data from API
            interval: Time interval (e.g., '5min')
            
        Returns:
            Dict with operation results
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    records_processed = 0
                    records_inserted = 0
                    records_updated = 0
                    errors = []
                    
                    for timestamp_str, price_data in data.items():
                        try:
                            # Parse timestamp
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            
                            # Extract and validate price data
                            open_price = self._validate_decimal(price_data.get('1. open'))
                            high_price = self._validate_decimal(price_data.get('2. high'))
                            low_price = self._validate_decimal(price_data.get('3. low'))
                            close_price = self._validate_decimal(price_data.get('4. close'))
                            volume = self._validate_integer(price_data.get('5. volume'))
                            
                            # Validate price relationships
                            if not self._validate_price_relationships(open_price, high_price, low_price, close_price):
                                logger.warning(f"Invalid price relationships for {symbol} at {timestamp_str}")
                                continue
                            
                            # Insert or update record
                            insert_query = """
                            INSERT INTO stock_market.intraday_prices 
                            (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, timestamp) 
                            DO UPDATE SET 
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close_price = EXCLUDED.close_price,
                                volume = EXCLUDED.volume
                            RETURNING (xmax = 0) AS inserted
                            """
                            
                            cursor.execute(insert_query, (
                                symbol.upper(),
                                timestamp,
                                open_price,
                                high_price,
                                low_price,
                                close_price,
                                volume
                            ))
                            
                            result = cursor.fetchone()
                            if result['inserted']:
                                records_inserted += 1
                            else:
                                records_updated += 1
                            
                            records_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Failed to process record for {timestamp_str}: {e}"
                            logger.error(error_msg)
                            errors.append(error_msg)
                            continue
                    
                    conn.commit()
                    
                    logger.info(f"Successfully processed {records_processed} intraday records for {symbol}")
                    
                    return {
                        'status': 'success',
                        'symbol': symbol,
                        'records_processed': records_processed,
                        'records_inserted': records_inserted,
                        'records_updated': records_updated,
                        'errors': errors
                    }
                    
        except Exception as e:
            logger.error(f"Failed to store intraday data for {symbol}: {e}")
            return {
                'status': 'error',
                'symbol': symbol,
                'error': str(e),
                'records_processed': 0
            }

    def get_latest_data(self, symbol: str, limit: int = 10) -> List[Dict]:
        """
        Get latest stock data for a symbol
        
        Args:
            symbol: Stock symbol
            limit: Number of records to fetch
            
        Returns:
            List of dictionaries containing stock data
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                    SELECT symbol, date, open_price, high_price, low_price, 
                           close_price, volume, created_at, updated_at
                    FROM stock_market.daily_prices
                    WHERE symbol = %s
                    ORDER BY date DESC
                    LIMIT %s
                    """
                    
                    cursor.execute(query, (symbol.upper(), limit))
                    results = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    return [dict(row) for row in results]
                    
        except Exception as e:
            logger.error(f"Failed to get latest data for {symbol}: {e}")
            return []

    def perform_data_quality_checks(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Perform data quality checks on stored data
        
        Args:
            symbols: List of symbols to check
            
        Returns:
            Dict with quality check results
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    quality_results = {
                        'total_records': 0,
                        'symbols_checked': len(symbols),
                        'date_range': {},
                        'volume_statistics': {},
                        'price_anomalies': [],
                        'missing_data': []
                    }
                    
                    for symbol in symbols:
                        # Count records
                        cursor.execute(
                            "SELECT COUNT(*) as count FROM stock_market.daily_prices WHERE symbol = %s",
                            (symbol.upper(),)
                        )
                        count_result = cursor.fetchone()
                        symbol_count = count_result['count'] if count_result else 0
                        quality_results['total_records'] += symbol_count
                        
                        # Date range
                        cursor.execute("""
                            SELECT MIN(date) as min_date, MAX(date) as max_date 
                            FROM stock_market.daily_prices 
                            WHERE symbol = %s
                        """, (symbol.upper(),))
                        
                        date_result = cursor.fetchone()
                        if date_result and date_result['min_date']:
                            quality_results['date_range'][symbol] = {
                                'min_date': date_result['min_date'].isoformat(),
                                'max_date': date_result['max_date'].isoformat(),
                                'record_count': symbol_count
                            }
                        
                        # Volume statistics
                        cursor.execute("""
                            SELECT AVG(volume) as avg_volume, 
                                   MIN(volume) as min_volume,
                                   MAX(volume) as max_volume
                            FROM stock_market.daily_prices 
                            WHERE symbol = %s AND volume > 0
                        """, (symbol.upper(),))
                        
                        volume_result = cursor.fetchone()
                        if volume_result and volume_result['avg_volume']:
                            quality_results['volume_statistics'][symbol] = {
                                'avg_volume': int(volume_result['avg_volume']),
                                'min_volume': volume_result['min_volume'],
                                'max_volume': volume_result['max_volume']
                            }
                        
                        # Check for price anomalies (prices <= 0 or high/low relationship issues)
                        cursor.execute("""
                            SELECT date, open_price, high_price, low_price, close_price
                            FROM stock_market.daily_prices
                            WHERE symbol = %s 
                            AND (open_price <= 0 OR high_price <= 0 OR low_price <= 0 OR close_price <= 0
                                 OR high_price < low_price 
                                 OR high_price < open_price 
                                 OR high_price < close_price
                                 OR low_price > open_price 
                                 OR low_price > close_price)
                            ORDER BY date DESC
                            LIMIT 10
                        """, (symbol.upper(),))
                        
                        anomalies = cursor.fetchall()
                        if anomalies:
                            quality_results['price_anomalies'].extend([
                                {
                                    'symbol': symbol,
                                    'date': anomaly['date'].isoformat(),
                                    'prices': dict(anomaly)
                                }
                                for anomaly in anomalies
                            ])
                    
                    logger.info(f"Data quality check completed for {len(symbols)} symbols")
                    return quality_results
                    
        except Exception as e:
            logger.error(f"Data quality check failed: {e}")
            return {'error': str(e)}

    def cleanup_old_pipeline_runs(self, cutoff_date: datetime) -> int:
        """
        Clean up old pipeline run records
        
        Args:
            cutoff_date: Delete records older than this date
            
        Returns:
            Number of records deleted
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    delete_query = """
                    DELETE FROM stock_market.pipeline_runs
                    WHERE created_at < %s
                    """
                    
                    cursor.execute(delete_query, (cutoff_date,))
                    deleted_count = cursor.rowcount
                    conn.commit()
                    
                    logger.info(f"Deleted {deleted_count} old pipeline run records")
                    return deleted_count
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old pipeline runs: {e}")
            return 0

    def _validate_decimal(self, value: str) -> Decimal:
        """Validate and convert string to Decimal"""
        if not value:
            raise ValueError("Price value cannot be empty")
        try:
            decimal_value = Decimal(str(value))
            if decimal_value <= 0:
                raise ValueError(f"Price must be positive: {value}")
            return decimal_value
        except Exception as e:
            raise ValueError(f"Invalid decimal value: {value} - {e}")

    def _validate_integer(self, value: str) -> int:
        """Validate and convert string to integer"""
        if not value:
            raise ValueError("Volume value cannot be empty")
        try:
            int_value = int(value)
            if int_value < 0:
                raise ValueError(f"Volume cannot be negative: {value}")
            return int_value
        except Exception as e:
            raise ValueError(f"Invalid integer value: {value} - {e}")

    def _validate_price_relationships(self, open_p: Decimal, high_p: Decimal, 
                                    low_p: Decimal, close_p: Decimal) -> bool:
        """
        Validate that price relationships make sense
        
        Args:
            open_p: Opening price
            high_p: High price
            low_p: Low price  
            close_p: Closing price
            
        Returns:
            True if relationships are valid
        """
        try:
            # High should be >= all other prices
            if high_p < max(open_p, low_p, close_p):
                return False
            
            # Low should be <= all other prices
            if low_p > min(open_p, high_p, close_p):
                return False
            
            return True
            
        except Exception:
            return False

    def __del__(self):
        """Cleanup connection pool when object is destroyed"""
        if hasattr(self, 'pool') and self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")


# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta

# # Create an instance of your DatabaseManager
# db_manager = DatabaseManager()

# def test_db_connection():
#     """Function to test DB connection using your DatabaseManager"""
#     result = db_manager.test_connection()
#     if not result:
#         raise Exception("Database connection failed")

# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }

# with DAG(
#     dag_id='stock_market_db_test',
#     default_args=default_args,
#     description='Test DB connection with DatabaseManager',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval='@daily',
#     catchup=False,
#     tags=['stock', 'db'],
# ) as dag:
    
#     test_connection_task = PythonOperator(
#         task_id='test_db_connection',
#         python_callable=test_db_connection
#     )


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def test_db_connection():
    db_manager = DatabaseManager()  # create instance inside the task
    result = db_manager.test_connection()
    if not result:
        raise Exception("Database connection failed")

with DAG(
    dag_id='stock_market_db_test',
    default_args=default_args,
    description='Test DB connection with DatabaseManager',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock', 'db'],
) as dag:
    
    test_connection_task = PythonOperator(
        task_id='test_db_connection',
        python_callable=test_db_connection
    )
