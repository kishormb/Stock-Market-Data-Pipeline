📈 Stock Market Data Pipeline

A Dockerized data pipeline using Apache Airflow to automatically fetch, process, and store stock market data from the Alpha Vantage API into a PostgreSQL database.

🚀 Features

⏱️ Automated hourly data fetching

📊 Data validation & anomaly detection

💾 PostgreSQL data storage with upserts

🔄 Docker Compose orchestration

🔐 Environment-secured credentials

👀 Airflow UI & Flower monitoring

⚙️ Easily configurable stock symbols

📦 Tech Stack

Apache Airflow (Celery Executor)

PostgreSQL

Redis

Docker + Docker Compose

Alpha Vantage API

📋 Prerequisites

Docker & Docker Compose installed

Alpha Vantage API key

Recommended: 4GB+ RAM, 2+ CPU cores

⚙️ Quick Start
1. Clone the Repository
git clone <repository-url>
cd stock-market-pipeline
cp .env.example .env

2. Set Environment Variables

Edit .env:

ALPHA_VANTAGE_API_KEY=your_api_key
POSTGRES_PASSWORD=your_pg_pass
STOCK_DB_PASSWORD=your_pg_pass
AIRFLOW_FERNET_KEY=<generate using Python>
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow


Generate the Fernet key:

python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

3. Build and Start the Pipeline
docker-compose up airflow-init
docker-compose up -d


Access Airflow UI at: http://localhost:8080
 (admin/admin)

📁 Project Structure
stock-market-pipeline/
├── airflow/              # Dockerfile, configs, requirements
├── dags/                 # Airflow DAGs
├── scripts/              # Stock data logic and utils
├── sql/                  # Postgres table setup
├── .env.example          # Env config template
├── docker-compose.yml    # All service definitions
└── README.md

✅ Usage
Run the Pipeline

Open Airflow UI: http://localhost:8080

Enable the fetch_stock_data DAG

Monitor tasks in real-time

Check Output

Run queries in Postgres (via pgAdmin or terminal):

SELECT * FROM stock_market.daily_prices ORDER BY date DESC LIMIT 20;

🛠️ Troubleshooting
Airflow Login Error?

Recreate the admin user:

docker-compose run airflow-webserver airflow users create \
  --username admin \
  --firstname admin \
  --lastname user \
  --role Admin \
  --email admin@example.com \
  --password admin


Then restart:

docker-compose down
docker-compose up -d

📈 Extend the Pipeline

Add more symbols via .env or Airflow Variables

Modify the DAG schedule in dags/stock_market_dag.py

Extend scripts/stock_fetcher.py to add new APIs

🧪 Testing

To validate the environment and APIs inside Airflow:

docker-compose exec airflow-scheduler bash
python -c "
from scripts.utils import validate_environment
from scripts.stock_fetcher import StockDataFetcher
validate_environment()
print('Environment OK')
print('API Status:', StockDataFetcher().get_api_status())
"
