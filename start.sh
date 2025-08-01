#!/bin/bash

echo "=================================="
echo "Smart City Data Quality Monitor"
echo "=================================="

# 환경 확인
if [ ! -f ".env" ]; then
    echo "Creating .env file from example..."
    cp .env.example .env
    echo "Please configure .env file before running"
fi

# Docker 환경 시작
echo "Starting Docker services..."
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 30

# 의존성 설치
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "=================================="
echo "Services Status:"
echo "=================================="
echo "Kafka UI: http://localhost:8080"
echo "Grafana: http://localhost:3000 (admin/admin)"
echo "ClickHouse: localhost:9000"
echo "=================================="

echo "To start the producer: python src/data_producer/producer.py"
echo "To start the quality monitor: python src/main.py"
