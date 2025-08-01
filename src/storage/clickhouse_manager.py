from clickhouse_driver import Client
from typing import List, Dict, Any
import logging
from datetime import datetime
from ..data_quality.evaluator import QualityMetric

logger = logging.getLogger(__name__)

class ClickHouseManager:
    """ClickHouse 데이터베이스 관리자"""
    
    def __init__(self, host: str, port: int, database: str, user: str = 'default', password: str = ''):
        self.client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.database = database
        self._ensure_database_exists()
        self._create_tables()
    
    def _ensure_database_exists(self):
        """데이터베이스 생성 확인"""
        try:
            self.client.execute(f'CREATE DATABASE IF NOT EXISTS {self.database}')
            logger.info(f"Database {self.database} is ready")
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
    
    def _create_tables(self):
        """필요한 테이블들 생성"""
        
        # 원시 센서 데이터 테이블
        raw_data_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.sensor_data (
            timestamp DateTime64(3),
            sensor_id String,
            temperature Nullable(Float32),
            humidity Nullable(Float32),
            wind_speed Nullable(Float32),
            received_at DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        ORDER BY (sensor_id, timestamp)
        TTL timestamp + INTERVAL 30 DAY
        """
        
        # 품질 메트릭 테이블
        quality_metrics_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.quality_metrics (
            timestamp DateTime64(3),
            sensor_id String,
            metric_name String,
            metric_value Float32,
            threshold Float32,
            status Enum8('PASS' = 1, 'WARN' = 2, 'FAIL' = 3),
            window_start DateTime64(3),
            window_end DateTime64(3),
            created_at DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        ORDER BY (sensor_id, metric_name, timestamp)
        TTL timestamp + INTERVAL 90 DAY
        """
        
        # 알림 로그 테이블
        alerts_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.alerts (
            timestamp DateTime64(3),
            sensor_id String,
            alert_type String,
            severity Enum8('LOW' = 1, 'MEDIUM' = 2, 'HIGH' = 3, 'CRITICAL' = 4),
            message String,
            metric_name String,
            metric_value Float32,
            threshold Float32,
            resolved UInt8 DEFAULT 0,
            created_at DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, sensor_id, severity)
        TTL timestamp + INTERVAL 180 DAY
        """
        
        try:
            self.client.execute(raw_data_table)
            self.client.execute(quality_metrics_table)
            self.client.execute(alerts_table)
            logger.info("All tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
    
    def insert_sensor_data(self, data: Dict[str, Any]) -> bool:
        """센서 데이터 삽입"""
        try:
            insert_query = f"""
            INSERT INTO {self.database}.sensor_data 
            (timestamp, sensor_id, temperature, humidity, wind_speed) 
            VALUES
            """
            
            self.client.execute(insert_query, [{
                'timestamp': datetime.fromisoformat(data['timestamp']),
                'sensor_id': data['sensor_id'],
                'temperature': data.get('temperature'),
                'humidity': data.get('humidity'),
                'wind_speed': data.get('wind_speed')
            }])
            return True
        except Exception as e:
            logger.error(f"Failed to insert sensor data: {e}")
            return False
    
    def insert_quality_metrics(self, metrics: List[QualityMetric]) -> bool:
        """품질 메트릭 삽입"""
        try:
            if not metrics:
                return True
                
            insert_query = f"""
            INSERT INTO {self.database}.quality_metrics 
            (timestamp, sensor_id, metric_name, metric_value, threshold, status, window_start, window_end) 
            VALUES
            """
            
            data_to_insert = []
            for metric in metrics:
                data_to_insert.append({
                    'timestamp': metric.timestamp,
                    'sensor_id': metric.sensor_id,
                    'metric_name': metric.metric_name,
                    'metric_value': metric.value,
                    'threshold': metric.threshold,
                    'status': metric.status,
                    'window_start': metric.window_start,
                    'window_end': metric.window_end
                })
            
            self.client.execute(insert_query, data_to_insert)
            return True
        except Exception as e:
            logger.error(f"Failed to insert quality metrics: {e}")
            return False
    
    def insert_alert(self, sensor_id: str, alert_type: str, severity: str, 
                     message: str, metric_name: str, metric_value: float, threshold: float) -> bool:
        """알림 삽입"""
        try:
            insert_query = f"""
            INSERT INTO {self.database}.alerts 
            (timestamp, sensor_id, alert_type, severity, message, metric_name, metric_value, threshold) 
            VALUES
            """
            
            self.client.execute(insert_query, [{
                'timestamp': datetime.now(),
                'sensor_id': sensor_id,
                'alert_type': alert_type,
                'severity': severity,
                'message': message,
                'metric_name': metric_name,
                'metric_value': metric_value,
                'threshold': threshold
            }])
            return True
        except Exception as e:
            logger.error(f"Failed to insert alert: {e}")
            return False
    
    def get_sensor_data_count(self, sensor_id: str = None, hours: int = 24) -> int:
        """센서 데이터 개수 조회"""
        try:
            where_clause = f"WHERE timestamp >= now() - INTERVAL {hours} HOUR"
            if sensor_id:
                where_clause += f" AND sensor_id = '{sensor_id}'"
            
            query = f"SELECT count() FROM {self.database}.sensor_data {where_clause}"
            result = self.client.execute(query)
            return result[0][0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get sensor data count: {e}")
            return 0
    
    def get_quality_metrics_summary(self, hours: int = 24) -> List[Dict[str, Any]]:
        """품질 메트릭 요약 조회"""
        try:
            query = f"""
            SELECT 
                sensor_id,
                metric_name,
                avg(metric_value) as avg_value,
                min(metric_value) as min_value,
                max(metric_value) as max_value,
                countIf(status = 'FAIL') as fail_count,
                countIf(status = 'PASS') as pass_count
            FROM {self.database}.quality_metrics 
            WHERE timestamp >= now() - INTERVAL {hours} HOUR
            GROUP BY sensor_id, metric_name
            ORDER BY sensor_id, metric_name
            """
            
            result = self.client.execute(query)
            return [
                {
                    'sensor_id': row[0],
                    'metric_name': row[1],
                    'avg_value': row[2],
                    'min_value': row[3],
                    'max_value': row[4],
                    'fail_count': row[5],
                    'pass_count': row[6]
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Failed to get quality metrics summary: {e}")
            return []
    
    def get_active_alerts(self, hours: int = 24) -> List[Dict[str, Any]]:
        """활성 알림 조회"""
        try:
            query = f"""
            SELECT 
                timestamp,
                sensor_id,
                alert_type,
                severity,
                message,
                metric_name,
                metric_value,
                threshold
            FROM {self.database}.alerts 
            WHERE timestamp >= now() - INTERVAL {hours} HOUR
            AND resolved = 0
            ORDER BY timestamp DESC
            """
            
            result = self.client.execute(query)
            return [
                {
                    'timestamp': row[0],
                    'sensor_id': row[1],
                    'alert_type': row[2],
                    'severity': row[3],
                    'message': row[4],
                    'metric_name': row[5],
                    'metric_value': row[6],
                    'threshold': row[7]
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Failed to get active alerts: {e}")
            return []
