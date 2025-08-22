#!/usr/bin/env python3
"""
Data Validation Script

This script can be run independently to validate the pipeline data
and perform health checks on the system.
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Add scripts directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_manager import DatabaseManager
from stock_fetcher import StockDataFetcher
from utils import setup_logging, validate_environment

logger = setup_logging(__name__)


def validate_api_connectivity() -> Dict[str, Any]:
    """Validate API connectivity and quotas"""
    try:
        fetcher = StockDataFetcher()
        status = fetcher.get_api_status()
        
        # Test with a sample symbol
        test_data = fetcher.fetch_quote_data('AAPL')
        
        return {
            'status': 'success' if test_data else 'warning',
            'api_status': status,
            'test_fetch': 'success' if test_data else 'failed',
            'message': 'API connectivity validated' if test_data else 'API test fetch failed'
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'message': 'Failed to validate API connectivity'
        }


def validate_database_connectivity() -> Dict[str, Any]:
    """Validate database connectivity and schema"""
    try:
        db = DatabaseManager()
        
        # Test basic connectivity
        if not db.test_connection():
            return {
                'status': 'error',
                'message': 'Database connection failed'
            }
        
        # Check if tables exist
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check main tables
                tables_to_check = [
                    'stock_market.daily_prices',
                    'stock_market.pipeline_runs',
                    'stock_market.intraday_prices'
                ]
                
                existing_tables = []
                for table in tables_to_check:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        );
                    """, tuple(table.split('.')))
                    
                    if cursor.fetchone()[0]:
                        existing_tables.append(table)
                
                # Get record counts
                record_counts = {}
                for table in existing_tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    record_counts[table] = cursor.fetchone()[0]
        
        return {
            'status': 'success',
            'existing_tables': existing_tables,
            'record_counts': record_counts,
            'message': 'Database validation successful'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'message': 'Database validation failed'
        }


def validate_data_quality() -> Dict[str, Any]:
    """Validate data quality in the database"""
    try:
        db = DatabaseManager()
        
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check for recent data
                cursor.execute("""
                    SELECT 
                        symbol,
                        COUNT(*) as record_count,
                        MAX(date) as latest_date,
                        MIN(date) as earliest_date
                    FROM stock_market.daily_prices
                    GROUP BY symbol
                    ORDER BY latest_date DESC
                """)
                
                symbol_stats = cursor.fetchall()
                
                # Check for data anomalies
                cursor.execute("""
                    SELECT COUNT(*) as anomaly_count
                    FROM stock_market.daily_prices
                    WHERE open_price <= 0 OR high_price <= 0 
                       OR low_price <= 0 OR close_price <= 0
                       OR high_price < low_price
                       OR volume < 0
                """)
                
                anomaly_count = cursor.fetchone()['anomaly_count']
                
                # Check data freshness (data from last 7 days)
                cursor.execute("""
                    SELECT COUNT(*) as recent_count
                    FROM stock_market.daily_prices
                    WHERE date >= %s
                """, (datetime.now().date() - timedelta(days=7),))
                
                recent_data_count = cursor.fetchone()['recent_count']
                
                return {
                    'status': 'success',
                    'symbol_statistics': [dict(row) for row in symbol_stats],
                    'anomaly_count': anomaly_count,
                    'recent_data_count': recent_data_count,
                    'message': f'Data quality check completed. {len(symbol_stats)} symbols, {anomaly_count} anomalies found'
                }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'message': 'Data quality validation failed'
        }


def validate_pipeline_runs() -> Dict[str, Any]:
    """Validate recent pipeline runs"""
    try:
        db = DatabaseManager()
        
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get recent pipeline runs
                cursor.execute("""
                    SELECT 
                        status,
                        COUNT(*) as count
                    FROM stock_market.pipeline_runs
                    WHERE created_at >= %s
                    GROUP BY status
                    ORDER BY count DESC
                """, (datetime.now() - timedelta(days=7),))
                
                status_counts = dict(cursor.fetchall())
                
                # Get latest runs
                cursor.execute("""
                    SELECT 
                        dag_id,
                        task_id,
                        symbol,
                        status,
                        records_processed,
                        created_at
                    FROM stock_market.pipeline_runs
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                
                recent_runs = [dict(row) for row in cursor.fetchall()]
                
                return {
                    'status': 'success',
                    'status_counts': status_counts,
                    'recent_runs': recent_runs,
                    'message': f'Pipeline validation completed. Recent runs: {sum(status_counts.values())}'
                }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'message': 'Pipeline runs validation failed'
        }


def generate_data_report() -> Dict[str, Any]:
    """Generate a comprehensive data report"""
    try:
        db = DatabaseManager()
        
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Stock performance summary
                cursor.execute("""
                    WITH latest_prices AS (
                        SELECT 
                            symbol,
                            close_price,
                            date,
                            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
                        FROM stock_market.daily_prices
                    ),
                    previous_prices AS (
                        SELECT 
                            symbol,
                            close_price as prev_price,
                            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
                        FROM stock_market.daily_prices
                    )
                    SELECT 
                        l.symbol,
                        l.close_price as current_price,
                        l.date as latest_date,
                        p.prev_price,
                        ROUND(((l.close_price - p.prev_price) / p.prev_price * 100)::numeric, 2) as daily_change_pct
                    FROM latest_prices l
                    LEFT JOIN previous_prices p ON l.symbol = p.symbol AND p.rn = 2
                    WHERE l.rn = 1
                    ORDER BY daily_change_pct DESC NULLS LAST
                """)
                
                stock_performance = [dict(row) for row in cursor.fetchall()]
                
                # Volume analysis
                cursor.execute("""
                    SELECT 
                        symbol,
                        AVG(volume)::bigint as avg_volume,
                        MAX(volume) as max_volume,
                        MIN(volume) as min_volume
                    FROM stock_market.daily_prices
                    WHERE date >= %s
                    GROUP BY symbol
                    ORDER BY avg_volume DESC
                """, (datetime.now().date() - timedelta(days=30),))
                
                volume_analysis = [dict(row) for row in cursor.fetchall()]
                
                return {
                    'status': 'success',
                    'generated_at': datetime.now().isoformat(),
                    'stock_performance': stock_performance,
                    'volume_analysis': volume_analysis,
                    'message': 'Data report generated successfully'
                }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'message': 'Data report generation failed'
        }


def main():
    """Main validation function"""
    print("🔍 Stock Market Pipeline Validation")
    print("=" * 40)
    
    validation_results = {}
    
    # Environment validation
    print("\n1. Validating Environment...")
    try:
        env_result = validate_environment()
        validation_results['environment'] = env_result
        print(f"✅ Environment: {env_result['status']}")
    except Exception as e:
        validation_results['environment'] = {'status': 'error', 'error': str(e)}
        print(f"❌ Environment: {str(e)}")
    
    # API validation
    print("\n2. Validating API Connectivity...")
    api_result = validate_api_connectivity()
    validation_results['api'] = api_result
    if api_result['status'] == 'success':
        print("✅ API: Connected and functional")
    else:
        print(f"❌ API: {api_result.get('message', 'Failed')}")
    
    # Database validation
    print("\n3. Validating Database...")
    db_result = validate_database_connectivity()
    validation_results['database'] = db_result
    if db_result['status'] == 'success':
        print("✅ Database: Connected")
        print(f"   Tables: {', '.join(db_result['existing_tables'])}")
        for table, count in db_result['record_counts'].items():
            print(f"   {table}: {count:,} records")
    else:
        print(f"❌ Database: {db_result.get('message', 'Failed')}")
    
    # Data quality validation
    print("\n4. Validating Data Quality...")
    quality_result = validate_data_quality()
    validation_results['data_quality'] = quality_result
    if quality_result['status'] == 'success':
        print("✅ Data Quality: Good")
        print(f"   Symbols: {len(quality_result['symbol_statistics'])}")
        print(f"   Recent records: {quality_result['recent_data_count']:,}")
        print(f"   Anomalies: {quality_result['anomaly_count']}")
    else:
        print(f"❌ Data Quality: {quality_result.get('message', 'Failed')}")
    
    # Pipeline runs validation
    print("\n5. Validating Pipeline Runs...")
    pipeline_result = validate_pipeline_runs()
    validation_results['pipeline_runs'] = pipeline_result
    if pipeline_result['status'] == 'success':
        print("✅ Pipeline Runs: Good")
        for status, count in pipeline_result['status_counts'].items():
            print(f"   {status}: {count}")
    else:
        print(f"❌ Pipeline Runs: {pipeline_result.get('message', 'Failed')}")
    
    # Generate data report
    print("\n6. Generating Data Report...")
    report_result = generate_data_report()
    validation_results['data_report'] = report_result
    if report_result['status'] == 'success':
        print("✅ Data Report: Generated")
        print("\n📊 Stock Performance (Latest):")
        for stock in report_result['stock_performance'][:5]:  # Top 5
            change = stock.get('daily_change_pct', 0) or 0
            symbol = f"{stock['symbol']:>6}"
            price = f"${stock['current_price']:>8.2f}"
            change_str = f"{change:>+6.2f}%" if change else "  N/A  "
            print(f"   {symbol}: {price} ({change_str})")
    else:
        print(f"❌ Data Report: {report_result.get('message', 'Failed')}")
    
    # Summary
    print("\n" + "=" * 40)
    successful_checks = sum(1 for result in validation_results.values() 
                          if result.get('status') == 'success')
    total_checks = len(validation_results)
    
    if successful_checks == total_checks:
        print(f"🎉 All {total_checks} validation checks passed!")
        print("Pipeline is healthy and operational.")
        return 0
    else:
        print(f"⚠️  {successful_checks}/{total_checks} validation checks passed.")
        print("Some issues were found. Check the details above.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)