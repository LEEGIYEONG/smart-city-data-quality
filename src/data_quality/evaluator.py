from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class QualityMetric:
    """데이터 품질 메트릭 정의"""
    metric_name: str
    value: float
    threshold: float
    status: str  # 'PASS', 'WARN', 'FAIL'
    timestamp: datetime
    sensor_id: str
    window_start: datetime
    window_end: datetime

class StreamDaQEvaluator:
    """Stream DaQ 프레임워크 기반 실시간 데이터 품질 평가기"""
    
    def __init__(self, quality_rules: Dict[str, Any]):
        self.quality_rules = quality_rules
        self.window_data = {}  # sensor_id별 윈도우 데이터 저장
        
    def add_data_point(self, data: Dict[str, Any]) -> None:
        """데이터 포인트를 윈도우에 추가"""
        sensor_id = data['sensor_id']
        timestamp = datetime.fromisoformat(data['timestamp'])
        
        if sensor_id not in self.window_data:
            self.window_data[sensor_id] = []
            
        self.window_data[sensor_id].append({
            'timestamp': timestamp,
            'temperature': data.get('temperature'),
            'humidity': data.get('humidity'),
            'wind_speed': data.get('wind_speed')
        })
        
        # 윈도우 크기 관리 (5분 윈도우)
        cutoff_time = timestamp - timedelta(minutes=self.quality_rules.get('time_window_minutes', 5))
        self.window_data[sensor_id] = [
            point for point in self.window_data[sensor_id] 
            if point['timestamp'] > cutoff_time
        ]
    
    def evaluate_completeness(self, sensor_id: str) -> QualityMetric:
        """완전성 평가: 결측값 비율"""
        if sensor_id not in self.window_data or not self.window_data[sensor_id]:
            return QualityMetric(
                metric_name='completeness',
                value=0.0,
                threshold=self.quality_rules.get('max_missing_percent', 20.0),
                status='FAIL',
                timestamp=datetime.now(),
                sensor_id=sensor_id,
                window_start=datetime.now() - timedelta(minutes=5),
                window_end=datetime.now()
            )
        
        data_points = self.window_data[sensor_id]
        total_points = len(data_points)
        
        # 각 필드별 결측값 계산
        missing_temp = sum(1 for p in data_points if p['temperature'] is None)
        missing_humidity = sum(1 for p in data_points if p['humidity'] is None)
        missing_wind = sum(1 for p in data_points if p['wind_speed'] is None)
        
        total_missing = missing_temp + missing_humidity + missing_wind
        total_fields = total_points * 3  # 3개 측정 필드
        
        missing_percent = (total_missing / total_fields) * 100 if total_fields > 0 else 100
        completeness = 100 - missing_percent
        
        threshold = self.quality_rules.get('max_missing_percent', 20.0)
        status = 'PASS' if missing_percent <= threshold else 'FAIL'
        
        return QualityMetric(
            metric_name='completeness',
            value=completeness,
            threshold=100 - threshold,
            status=status,
            timestamp=datetime.now(),
            sensor_id=sensor_id,
            window_start=data_points[0]['timestamp'] if data_points else datetime.now(),
            window_end=data_points[-1]['timestamp'] if data_points else datetime.now()
        )
    
    def evaluate_validity(self, sensor_id: str) -> List[QualityMetric]:
        """유효성 평가: 값 범위 검증"""
        metrics = []
        
        if sensor_id not in self.window_data or not self.window_data[sensor_id]:
            return metrics
        
        data_points = self.window_data[sensor_id]
        
        # 온도 유효성 검사
        temp_values = [p['temperature'] for p in data_points if p['temperature'] is not None]
        if temp_values:
            temp_min, temp_max = self.quality_rules.get('temperature_range', (-20, 40))
            invalid_temp = sum(1 for t in temp_values if not (temp_min <= t <= temp_max))
            validity_rate = ((len(temp_values) - invalid_temp) / len(temp_values)) * 100
            
            metrics.append(QualityMetric(
                metric_name='temperature_validity',
                value=validity_rate,
                threshold=95.0,  # 95% 이상 유효해야 함
                status='PASS' if validity_rate >= 95.0 else 'FAIL',
                timestamp=datetime.now(),
                sensor_id=sensor_id,
                window_start=data_points[0]['timestamp'],
                window_end=data_points[-1]['timestamp']
            ))
        
        # 습도 유효성 검사
        humidity_values = [p['humidity'] for p in data_points if p['humidity'] is not None]
        if humidity_values:
            hum_min, hum_max = self.quality_rules.get('humidity_range', (0, 100))
            invalid_hum = sum(1 for h in humidity_values if not (hum_min <= h <= hum_max))
            validity_rate = ((len(humidity_values) - invalid_hum) / len(humidity_values)) * 100
            
            metrics.append(QualityMetric(
                metric_name='humidity_validity',
                value=validity_rate,
                threshold=95.0,
                status='PASS' if validity_rate >= 95.0 else 'FAIL',
                timestamp=datetime.now(),
                sensor_id=sensor_id,
                window_start=data_points[0]['timestamp'],
                window_end=data_points[-1]['timestamp']
            ))
        
        # 풍속 유효성 검사
        wind_values = [p['wind_speed'] for p in data_points if p['wind_speed'] is not None]
        if wind_values:
            wind_min, wind_max = self.quality_rules.get('wind_speed_range', (0, 50))
            invalid_wind = sum(1 for w in wind_values if not (wind_min <= w <= wind_max))
            validity_rate = ((len(wind_values) - invalid_wind) / len(wind_values)) * 100
            
            metrics.append(QualityMetric(
                metric_name='wind_speed_validity',
                value=validity_rate,
                threshold=95.0,
                status='PASS' if validity_rate >= 95.0 else 'FAIL',
                timestamp=datetime.now(),
                sensor_id=sensor_id,
                window_start=data_points[0]['timestamp'],
                window_end=data_points[-1]['timestamp']
            ))
        
        return metrics
    
    def evaluate_timeliness(self, sensor_id: str) -> QualityMetric:
        """적시성 평가: 데이터 수신 지연"""
        if sensor_id not in self.window_data or not self.window_data[sensor_id]:
            return QualityMetric(
                metric_name='timeliness',
                value=0.0,
                threshold=90.0,
                status='FAIL',
                timestamp=datetime.now(),
                sensor_id=sensor_id,
                window_start=datetime.now() - timedelta(minutes=5),
                window_end=datetime.now()
            )
        
        data_points = self.window_data[sensor_id]
        now = datetime.now()
        
        # 최근 5분 내 데이터 포인트 개수 (5초 간격이므로 60개 예상)
        expected_points = 60  # 5분 * 60초 / 5초 간격
        actual_points = len(data_points)
        
        timeliness_rate = min((actual_points / expected_points) * 100, 100)
        
        return QualityMetric(
            metric_name='timeliness',
            value=timeliness_rate,
            threshold=90.0,  # 90% 이상 수신되어야 함
            status='PASS' if timeliness_rate >= 90.0 else 'FAIL',
            timestamp=now,
            sensor_id=sensor_id,
            window_start=data_points[0]['timestamp'] if data_points else now,
            window_end=data_points[-1]['timestamp'] if data_points else now
        )
    
    def evaluate_consistency(self, sensor_id: str) -> QualityMetric:
        """일관성 평가: 값의 급격한 변화 탐지"""
        if sensor_id not in self.window_data or len(self.window_data[sensor_id]) < 2:
            return QualityMetric(
                metric_name='consistency',
                value=100.0,
                threshold=80.0,
                status='PASS',
                timestamp=datetime.now(),
                sensor_id=sensor_id,
                window_start=datetime.now() - timedelta(minutes=5),
                window_end=datetime.now()
            )
        
        data_points = self.window_data[sensor_id]
        inconsistent_count = 0
        total_transitions = 0
        
        # 온도 변화율 검사
        for i in range(1, len(data_points)):
            if (data_points[i]['temperature'] is not None and 
                data_points[i-1]['temperature'] is not None):
                temp_diff = abs(data_points[i]['temperature'] - data_points[i-1]['temperature'])
                if temp_diff > 10:  # 10도 이상 급변 시 비일관성
                    inconsistent_count += 1
                total_transitions += 1
        
        consistency_rate = 100.0
        if total_transitions > 0:
            consistency_rate = ((total_transitions - inconsistent_count) / total_transitions) * 100
        
        return QualityMetric(
            metric_name='consistency',
            value=consistency_rate,
            threshold=80.0,
            status='PASS' if consistency_rate >= 80.0 else 'FAIL',
            timestamp=datetime.now(),
            sensor_id=sensor_id,
            window_start=data_points[0]['timestamp'],
            window_end=data_points[-1]['timestamp']
        )
    
    def evaluate_all_metrics(self, sensor_id: str) -> List[QualityMetric]:
        """모든 품질 메트릭 평가"""
        metrics = []
        
        # 완전성 평가
        metrics.append(self.evaluate_completeness(sensor_id))
        
        # 유효성 평가
        metrics.extend(self.evaluate_validity(sensor_id))
        
        # 적시성 평가
        metrics.append(self.evaluate_timeliness(sensor_id))
        
        # 일관성 평가
        metrics.append(self.evaluate_consistency(sensor_id))
        
        return metrics
