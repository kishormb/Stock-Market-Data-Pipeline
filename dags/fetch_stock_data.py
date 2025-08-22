from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os

# Airflow default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "DVZ14VH82RFKLVF4")
SYMBOLS = ["AAPL", "GOOG", "MSFT"]

def fetch_and_store():
    conn = psycopg2.connect(
        host="postgres",      # service name from docker-compose
        dbname="stock_data",  # database name
        user="airflow",       # matches your docker env
        password="airflow",   # matches your docker env
        port=5432
    )
    cur = conn.cursor()

    for symbol in SYMBOLS:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}&outputsize=compact"
        response = requests.get(url)
        data = response.json().get("Time Series (Daily)", {})

        for date_str, values in data.items():
            cur.execute("""
                INSERT INTO daily_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (
                symbol,
                date_str,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"])
            ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='fetch_stock_data',
    default_args=default_args,
    schedule_interval='@daily',  # run once every day
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_store_stock_data',
        python_callable=fetch_and_store
    )
