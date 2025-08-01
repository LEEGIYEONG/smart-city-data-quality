import json
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from kafka import KafkaConsumer, KafkaProducer
import threading
from datetime import datetime

from ..config.settings import SparkConfig, KafkaConfig, ClickHouseConfig, QualityRules
from ..data_quality.evaluator import StreamDaQEvaluator
from ..storage.clickhouse_manager import ClickHouseManager
from ..notifications.slack_notifier import SlackNotifier

logger = logging.getLogger(__name__)

class QualityStreamProcessor:
    """실시간 데이터 품질 처리기"""
    
    def __init__(self, kafka_config: KafkaConfig, clickhouse_config: ClickHouseConfig, 
                 quality_rules: QualityRules, slack_notifier: SlackNotifier):
        self.kafka_config = kafka_config
        self.clickhouse_config = clickhouse_config
        self.quality_rules = quality_rules
        self.slack_notifier = slack_notifier
        
        # 초기화
        self.evaluator = StreamDaQEvaluator(quality_rules.__dict__)
        self.clickhouse = ClickHouseManager(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            database=clickhouse_config.database,
            user=clickhouse_config.user,
            password=clickhouse_config.password
        )
        
        # Kafka 설정
        self.consumer = KafkaConsumer(
            kafka_config.weather_topic,
            bootstrap_servers=kafka_config.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id='quality-processor',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.running = False
        self.processed_count = 0
        
    def start_processing(self):
        """실시간 처리 시작"""
        self.running = True
        logger.info("Quality stream processor started")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    data = message.value
                    self._process_data_point(data)
                    self.processed_count += 1
                    
                    if self.processed_count % 100 == 0:
                        logger.info(f"Processed {self.processed_count} data points")
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop_processing()
    
    def _process_data_point(self, data: Dict[str, Any]):
        """개별 데이터 포인트 처리"""
        sensor_id = data.get('sensor_id')
        if not sensor_id:
            logger.warning("Received data without sensor_id")
            return
        
        # 1. 원시 데이터 저장
        self.clickhouse.insert_sensor_data(data)
        
        # 2. 품질 평가기에 데이터 추가
        self.evaluator.add_data_point(data)
        
        # 3. 품질 메트릭 평가
        quality_metrics = self.evaluator.evaluate_all_metrics(sensor_id)
        
        # 4. 품질 메트릭 저장
        if quality_metrics:
            self.clickhouse.insert_quality_metrics(quality_metrics)
        
        # 5. 실패한 메트릭에 대한 알림 처리
        failed_metrics = [m for m in quality_metrics if m.status == 'FAIL']
        if failed_metrics:
            self._handle_quality_alerts(sensor_id, failed_metrics)
        
        # 6. 품질 메트릭 Kafka로 발행
        self._publish_quality_metrics(quality_metrics)
    
    def _handle_quality_alerts(self, sensor_id: str, failed_metrics: list):
        """품질 알림 처리"""
        try:
            # 1. 알림 데이터베이스에 저장
            for metric in failed_metrics:
                severity = self._get_alert_severity(metric)
                message = self._generate_alert_message(metric)
                
                self.clickhouse.insert_alert(
                    sensor_id=sensor_id,
                    alert_type='QUALITY_ALERT',
                    severity=severity,
                    message=message,
                    metric_name=metric.metric_name,
                    metric_value=metric.value,
                    threshold=metric.threshold
                )
            
            # 2. Slack 알림 전송 (중요도가 높은 경우)
            high_severity_metrics = [m for m in failed_metrics 
                                   if self._get_alert_severity(m) in ['HIGH', 'CRITICAL']]
            
            if high_severity_metrics:
                self.slack_notifier.send_quality_alert(sensor_id, high_severity_metrics)
            
        except Exception as e:
            logger.error(f"Error handling quality alerts: {e}")
    
    def _publish_quality_metrics(self, metrics: list):
        """품질 메트릭을 Kafka로 발행"""
        try:
            for metric in metrics:
                metric_data = {
                    'timestamp': metric.timestamp.isoformat(),
                    'sensor_id': metric.sensor_id,
                    'metric_name': metric.metric_name,
                    'value': metric.value,
                    'threshold': metric.threshold,
                    'status': metric.status,
                    'window_start': metric.window_start.isoformat(),
                    'window_end': metric.window_end.isoformat()
                }
                
                self.producer.send(self.kafka_config.quality_metrics_topic, value=metric_data)
                
        except Exception as e:
            logger.error(f"Error publishing quality metrics: {e}")
    
    def _get_alert_severity(self, metric) -> str:
        """메트릭 기반 알림 심각도 결정"""
        if metric.metric_name == 'completeness' and metric.value < 50:
            return 'CRITICAL'
        elif metric.metric_name == 'timeliness' and metric.value < 70:
            return 'HIGH'
        elif 'validity' in metric.metric_name and metric.value < 80:
            return 'HIGH'
        elif metric.metric_name == 'consistency' and metric.value < 60:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _generate_alert_message(self, metric) -> str:
        """알림 메시지 생성"""
        messages = {
            'completeness': f"데이터 완전성이 {metric.value:.1f}%로 임계값 {metric.threshold:.1f}% 미만입니다.",
            'timeliness': f"데이터 적시성이 {metric.value:.1f}%로 임계값 {metric.threshold:.1f}% 미만입니다.",
            'temperature_validity': f"온도 데이터 유효성이 {metric.value:.1f}%로 임계값 {metric.threshold:.1f}% 미만입니다.",
            'humidity_validity': f"습도 데이터 유효성이 {metric.value:.1f}%로 임계값 {metric.threshold:.1f}% 미만입니다.",
            'wind_speed_validity': f"풍속 데이터 유효성이 {metric.value:.1f}%로 임계값 {metric.threshold:.1f}% 미만입니다.",
            'consistency': f"데이터 일관성이 {metric.value:.1f}%로 임계값 {metric.threshold:.1f}% 미만입니다."
        }
        
        return messages.get(metric.metric_name, 
                          f"{metric.metric_name} 메트릭이 임계값을 벗어났습니다: {metric.value:.1f} (임계값: {metric.threshold:.1f})")
    
    def stop_processing(self):
        """처리 중단"""
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'producer'):
            self.producer.close()
        logger.info("Quality stream processor stopped")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """처리 통계 반환"""
        return {
            'processed_count': self.processed_count,
            'running': self.running,
            'evaluator_sensor_count': len(self.evaluator.window_data)
        }


class SparkStreamProcessor:
    """Spark Streaming 기반 고급 처리기 (추가 분석용)"""
    
    def __init__(self, spark_config: SparkConfig, kafka_config: KafkaConfig):
        self.spark_config = spark_config
        self.kafka_config = kafka_config
        
        # Spark 세션 생성
        self.spark = SparkSession.builder \
            .appName(spark_config.app_name) \
            .config("spark.sql.streaming.checkpointLocation", spark_config.checkpoint_dir) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # 스키마 정의
        self.weather_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("temperature", FloatType(), True),
            StructField("humidity", FloatType(), True),
            StructField("wind_speed", FloatType(), True)
        ])
    
    def start_aggregation_stream(self):
        """집계 스트림 시작"""
        try:
            # Kafka 스트림 생성
            kafka_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config.bootstrap_servers) \
                .option("subscribe", self.kafka_config.weather_topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            # JSON 파싱
            parsed_stream = kafka_stream \
                .selectExpr("CAST(value AS STRING) as json_value") \
                .select(from_json(col("json_value"), self.weather_schema).alias("data")) \
                .select("data.*") \
                .withColumn("timestamp", col("timestamp").cast(TimestampType()))
            
            # 시간 윈도우별 집계
            windowed_aggregation = parsed_stream \
                .withWatermark("timestamp", "2 minutes") \
                .groupBy(
                    window(col("timestamp"), "5 minutes", "1 minute"),
                    col("sensor_id")
                ) \
                .agg(
                    count("*").alias("record_count"),
                    avg("temperature").alias("avg_temperature"),
                    avg("humidity").alias("avg_humidity"),
                    avg("wind_speed").alias("avg_wind_speed"),
                    stddev("temperature").alias("temp_stddev")
                )
            
            # 콘솔 출력 (개발용)
            query = windowed_aggregation \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime=f"{self.spark_config.batch_duration} seconds") \
                .start()
            
            return query
            
        except Exception as e:
            logger.error(f"Error starting Spark aggregation stream: {e}")
            return None
    
    def stop(self):
        """Spark 세션 종료"""
        if hasattr(self, 'spark'):
            self.spark.stop()
