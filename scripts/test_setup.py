"""
데이터베이스 초기화 및 테스트 스크립트
"""
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.storage.clickhouse_manager import ClickHouseManager
from src.config.settings import ClickHouseConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_clickhouse_connection():
    """ClickHouse 연결 테스트"""
    try:
        config = ClickHouseConfig()
        ch = ClickHouseManager(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        
        # 테스트 쿼리
        count = ch.get_sensor_data_count()
        logger.info(f"ClickHouse connection successful! Current sensor data count: {count}")
        
        # 품질 메트릭 요약
        metrics = ch.get_quality_metrics_summary(hours=1)
        logger.info(f"Quality metrics in last hour: {len(metrics)} records")
        
        # 활성 알림
        alerts = ch.get_active_alerts(hours=1)
        logger.info(f"Active alerts in last hour: {len(alerts)} alerts")
        
        return True
        
    except Exception as e:
        logger.error(f"ClickHouse connection failed: {e}")
        return False

def initialize_test_data():
    """테스트 데이터 삽입"""
    try:
        from datetime import datetime
        
        config = ClickHouseConfig()
        ch = ClickHouseManager(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        
        # 테스트 센서 데이터
        test_data = {
            'timestamp': datetime.now().isoformat(),
            'sensor_id': 'TEST-001',
            'temperature': 25.5,
            'humidity': 65.0,
            'wind_speed': 8.2
        }
        
        success = ch.insert_sensor_data(test_data)
        if success:
            logger.info("Test sensor data inserted successfully")
        else:
            logger.error("Failed to insert test sensor data")
            
        return success
        
    except Exception as e:
        logger.error(f"Error initializing test data: {e}")
        return False

if __name__ == "__main__":
    logger.info("Testing ClickHouse connection and database setup...")
    
    if test_clickhouse_connection():
        logger.info("✅ ClickHouse connection test passed")
        
        if initialize_test_data():
            logger.info("✅ Test data initialization passed")
        else:
            logger.error("❌ Test data initialization failed")
    else:
        logger.error("❌ ClickHouse connection test failed")
        logger.info("Make sure ClickHouse is running: docker-compose up -d")
