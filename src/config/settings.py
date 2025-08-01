import os
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    weather_topic: str = 'weather-data'
    quality_metrics_topic: str = 'quality-metrics'
    alerts_topic: str = 'data-quality-alerts'

@dataclass
class ClickHouseConfig:
    host: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port: int = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    database: str = os.getenv('CLICKHOUSE_DB', 'smart_city_dq')
    user: str = os.getenv('CLICKHOUSE_USER', 'default')
    password: str = os.getenv('CLICKHOUSE_PASSWORD', '')

@dataclass
class QualityRules:
    temperature_range: tuple = (-20.0, 40.0)  # 시카고 온도 범위
    humidity_range: tuple = (0.0, 100.0)
    wind_speed_range: tuple = (0.0, 50.0)
    max_missing_percent: float = 20.0  # 20% 결측치 임계값
    time_window_minutes: int = 5  # 품질 평가 시간 윈도우
    
@dataclass
class SlackConfig:
    webhook_url: str = os.getenv('SLACK_WEBHOOK_URL', '')
    channel: str = os.getenv('SLACK_CHANNEL', '#smart-city-alerts')

@dataclass
class SparkConfig:
    app_name: str = 'SmartCityDataQuality'
    batch_duration: int = 30  # seconds
    checkpoint_dir: str = './checkpoint'
