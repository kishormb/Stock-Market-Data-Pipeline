#!/bin/bash

# Stock Market Pipeline Setup Script
# This script helps setup the environment and start the pipeline

set -e

echo "🚀 Stock Market Data Pipeline Setup"
echo "=================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created. Please edit it with your configuration."
    echo ""
    echo "Required configuration:"
    echo "  - ALPHA_VANTAGE_API_KEY: Get from https://www.alphavantage.co/support/#api-key"
    echo "  - POSTGRES_PASSWORD: Set a secure password"
    echo "  - STOCK_DB_PASSWORD: Set a secure password"
    echo "  - AIRFLOW_FERNET_KEY: Generate with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
    echo ""
    read -p "Press Enter to continue after editing .env file..."
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if required environment variables are set
echo "🔍 Validating environment configuration..."

required_vars=("ALPHA_VANTAGE_API_KEY" "POSTGRES_PASSWORD" "STOCK_DB_PASSWORD")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ Missing required environment variables:"
    printf '   - %s\n' "${missing_vars[@]}"
    echo "Please edit .env file and set these variables."
    exit 1
fi

# Generate Airflow UID for Linux/Mac
if [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "darwin"* ]]; then
    if ! grep -q "AIRFLOW_UID" .env; then
        echo "AIRFLOW_UID=$(id -u)" >> .env
        echo "✅ Added AIRFLOW_UID to .env file"
    fi
fi

# Make scripts executable
echo "🔧 Setting up permissions..."
chmod +x sql/create-multiple-dbs.sh
echo "✅ Made database scripts executable"

# Create necessary directories
echo "📁 Creating required directories..."
mkdir -p logs postgres-data
echo "✅ Created logs and postgres-data directories"

# Check Docker daemon
echo "🐳 Checking Docker daemon..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker daemon is not running. Please start Docker first."
    exit 1
fi
echo "✅ Docker daemon is running"

# Build and start services
echo "🏗️  Building and starting services..."
echo "This may take several minutes on first run..."

# Pull images first to show progress
docker-compose pull

# Build custom images
docker-compose build

# Start services
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 30

# Check service health
echo "🩺 Checking service health..."
services=("postgres" "redis" "airflow-webserver" "airflow-scheduler")
all_healthy=true

for service in "${services[@]}"; do
    if docker-compose ps "$service" | grep -q "Up"; then
        echo "✅ $service is running"
    else
        echo "❌ $service is not running"
        all_healthy=false
    fi
done

if [ "$all_healthy" = true ]; then
    echo ""
    echo "🎉 Pipeline setup completed successfully!"
    echo ""
    echo "Access Points:"
    echo "  📊 Airflow Web UI: http://localhost:8080"
    echo "     Username: ${_AIRFLOW_WWW_USER_USERNAME:-admin}"
    echo "     Password: ${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
    echo ""
    echo "  🌸 Flower (Celery Monitor): http://localhost:5555"
    echo ""
    echo "Useful Commands:"
    echo "  📋 View logs: docker-compose logs -f [service-name]"
    echo "  📈 Check status: docker-compose ps"
    echo "  🛑 Stop services: docker-compose down"
    echo "  🔄 Restart services: docker-compose restart"
    echo ""
    echo "The pipeline will automatically:"
    echo "  • Fetch stock data every hour"
    echo "  • Store data in PostgreSQL"
    echo "  • Perform quality checks"
    echo "  • Handle errors gracefully"
    echo ""
    echo "Default stock symbols: AAPL, GOOGL, MSFT, AMZN, TSLA"
    echo "You can customize symbols in Airflow Web UI under Admin → Variables"
else
    echo ""
    echo "❌ Some services failed to start properly."
    echo "Check logs with: docker-compose logs [service-name]"
    echo ""
    echo "Common solutions:"
    echo "  1. Ensure Docker has enough memory (4GB+ recommended)"
    echo "  2. Check .env file configuration"
    echo "  3. Verify API key is valid"
    echo "  4. Try: docker-compose down && docker-compose up -d"
    exit 1
fi