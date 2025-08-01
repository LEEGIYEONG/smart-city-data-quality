import logging
import signal
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.config.settings import KafkaConfig, ClickHouseConfig, QualityRules, SlackConfig
from src.stream_processor.quality_processor import QualityStreamProcessor
from src.notifications.slack_notifier import SlackNotifier

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('quality_processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class SmartCityQualityMonitor:
    """스마트시티 데이터 품질 모니터링 메인 애플리케이션"""
    
    def __init__(self):
        # 설정 초기화
        self.kafka_config = KafkaConfig()
        self.clickhouse_config = ClickHouseConfig()
        self.quality_rules = QualityRules()
        self.slack_config = SlackConfig()
        
        # 알림 시스템 초기화
        self.slack_notifier = SlackNotifier(
            webhook_url=self.slack_config.webhook_url,
            channel=self.slack_config.channel
        )
        
        # 품질 처리기 초기화
        self.processor = QualityStreamProcessor(
            kafka_config=self.kafka_config,
            clickhouse_config=self.clickhouse_config,
            quality_rules=self.quality_rules,
            slack_notifier=self.slack_notifier
        )
        
        # 종료 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.running = False
    
    def start(self):
        """모니터링 시스템 시작"""
        logger.info("Starting Smart City Data Quality Monitor...")
        
        try:
            # 시작 알림
            self.slack_notifier.send_system_alert(
                alert_type="SYSTEM_START",
                message="스마트시티 데이터 품질 모니터링 시스템이 시작되었습니다.",
                severity="LOW"
            )
            
            # 품질 처리 시작
            self.running = True
            logger.info("Quality processor starting...")
            self.processor.start_processing()
            
        except Exception as e:
            logger.error(f"Error starting monitor: {e}")
            self.slack_notifier.send_system_alert(
                alert_type="SYSTEM_ERROR",
                message=f"시스템 시작 중 오류 발생: {str(e)}",
                severity="CRITICAL"
            )
            raise
    
    def stop(self):
        """모니터링 시스템 중단"""
        logger.info("Stopping Smart City Data Quality Monitor...")
        
        self.running = False
        
        if hasattr(self, 'processor'):
            self.processor.stop_processing()
        
        # 종료 알림
        stats = self.processor.get_processing_stats() if hasattr(self, 'processor') else {}
        
        self.slack_notifier.send_system_alert(
            alert_type="SYSTEM_STOP",
            message=f"시스템이 정상적으로 종료되었습니다. 처리된 데이터: {stats.get('processed_count', 0)}건",
            severity="LOW"
        )
        
        logger.info("Monitor stopped")
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)
    
    def health_check(self) -> dict:
        """시스템 상태 확인"""
        try:
            stats = self.processor.get_processing_stats() if hasattr(self, 'processor') else {}
            
            # ClickHouse 연결 확인
            sensor_count = self.processor.clickhouse.get_sensor_data_count(hours=1) if hasattr(self, 'processor') else 0
            
            return {
                'status': 'healthy' if self.running else 'stopped',
                'timestamp': datetime.now().isoformat(),
                'processed_count': stats.get('processed_count', 0),
                'active_sensors': stats.get('evaluator_sensor_count', 0),
                'recent_data_count': sensor_count,
                'processor_running': stats.get('running', False)
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }


def main():
    """메인 실행 함수"""
    try:
        # 시스템 시작
        monitor = SmartCityQualityMonitor()
        
        logger.info("=" * 60)
        logger.info("Smart City Data Quality Monitor")
        logger.info("=" * 60)
        logger.info(f"Kafka Bootstrap Servers: {monitor.kafka_config.bootstrap_servers}")
        logger.info(f"ClickHouse Host: {monitor.clickhouse_config.host}:{monitor.clickhouse_config.port}")
        logger.info(f"Quality Rules: {monitor.quality_rules}")
        logger.info(f"Slack Notifications: {'Enabled' if monitor.slack_notifier.enabled else 'Disabled'}")
        logger.info("=" * 60)
        
        # 상태 확인
        health = monitor.health_check()
        logger.info(f"Initial health check: {health}")
        
        # 시작
        monitor.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
