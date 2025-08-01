
import os
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timedelta
import threading
import signal
import sys

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 시카고 지역별 센서 위치 시뮬레이션
CHICAGO_AREAS = [
    {"area": "Downtown", "base_temp": 2.0, "sensor_count": 20},
    {"area": "North Side", "base_temp": 0.0, "sensor_count": 15},
    {"area": "South Side", "base_temp": -1.0, "sensor_count": 15},
    {"area": "West Side", "base_temp": -0.5, "sensor_count": 12},
    {"area": "O'Hare", "base_temp": -2.0, "sensor_count": 8}
]

class WeatherDataGenerator:
    """시카고 날씨 데이터 생성기"""
    
    def __init__(self):
        self.running = False
        self.sensors = self._initialize_sensors()
        self.base_time = datetime.now()
        self.data_quality_issues = {
            'missing_data_probability': 0.05,  # 5% 결측 확률
            'invalid_data_probability': 0.02,  # 2% 잘못된 데이터 확률
            'delay_probability': 0.03,         # 3% 지연 확률
            'sensor_failure_probability': 0.001 # 0.1% 센서 고장 확률
        }
        self.failed_sensors = set()
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _initialize_sensors(self):
        """센서 초기화"""
        sensors = []
        sensor_id = 1
        
        for area in CHICAGO_AREAS:
            for i in range(area["sensor_count"]):
                sensor = {
                    'sensor_id': f"CHI-{area['area'][:3].upper()}-{sensor_id:03d}",
                    'area': area['area'],
                    'base_temp': area['base_temp'],
                    'last_temp': 15.0 + area['base_temp'],
                    'last_humidity': 60.0,
                    'last_wind_speed': 5.0,
                    'drift_factor': random.uniform(0.8, 1.2)  # 센서별 편차
                }
                sensors.append(sensor)
                sensor_id += 1
        
        return sensors
    
    def _generate_realistic_weather(self, sensor, current_time):
        """현실적인 날씨 데이터 생성"""
        # 시간대별 온도 변화 (낮에 높고 밤에 낮음)
        hour = current_time.hour
        seasonal_temp = self._get_seasonal_temperature(current_time)
        daily_temp_cycle = 8 * math.sin((hour - 6) * math.pi / 12)  # 오후 6시 최고온도
        
        # 기본 온도 계산
        base_temp = seasonal_temp + daily_temp_cycle + sensor['base_temp']
        
        # 센서 드리프트와 노이즈 추가
        temp_noise = random.gauss(0, 1.5)  # 가우시안 노이즈
        new_temp = sensor['drift_factor'] * (base_temp + temp_noise)
        
        # 급격한 변화 방지 (현실적인 변화율)
        temp_diff = new_temp - sensor['last_temp']
        if abs(temp_diff) > 5:  # 5도 이상 급변 방지
            new_temp = sensor['last_temp'] + (temp_diff / abs(temp_diff)) * 2
        
        # 습도 생성 (온도와 반상관)
        humidity_base = 80 - (new_temp * 1.5)  # 온도 높으면 습도 낮음
        humidity_noise = random.gauss(0, 10)
        new_humidity = max(20, min(95, humidity_base + humidity_noise))
        
        # 풍속 생성 (계절성 고려)
        wind_base = 8 if current_time.month in [11, 12, 1, 2, 3] else 5  # 겨울철 바람
        wind_noise = random.gauss(0, 2)
        new_wind_speed = max(0, wind_base + wind_noise)
        
        # 센서 상태 업데이트
        sensor['last_temp'] = new_temp
        sensor['last_humidity'] = new_humidity
        sensor['last_wind_speed'] = new_wind_speed
        
        return {
            'temperature': round(new_temp, 2),
            'humidity': round(new_humidity, 2),
            'wind_speed': round(new_wind_speed, 2)
        }
    
    def _get_seasonal_temperature(self, current_time):
        """계절별 기본 온도"""
        month = current_time.month
        seasonal_temps = {
            1: -5, 2: -3, 3: 3, 4: 10, 5: 16, 6: 22,
            7: 25, 8: 24, 9: 20, 10: 13, 11: 5, 12: -2
        }
        return seasonal_temps.get(month, 15)
    
    def _apply_quality_issues(self, data, sensor):
        """데이터 품질 문제 시뮬레이션"""
        issues = self.data_quality_issues
        
        # 센서 고장 시뮬레이션
        if (random.random() < issues['sensor_failure_probability'] or 
            sensor['sensor_id'] in self.failed_sensors):
            
            self.failed_sensors.add(sensor['sensor_id'])
            # 고장난 센서는 데이터 전송 안함 또는 이상한 값
            if random.random() < 0.7:  # 70% 확률로 데이터 전송 안함
                return None
            else:  # 30% 확률로 이상한 값 전송
                data['temperature'] = random.choice([999.9, -999.9, None])
                data['humidity'] = random.choice([999.9, None])
                data['wind_speed'] = random.choice([999.9, None])
        
        # 결측 데이터 시뮬레이션
        if random.random() < issues['missing_data_probability']:
            field = random.choice(['temperature', 'humidity', 'wind_speed'])
            data[field] = None
        
        # 잘못된 데이터 시뮬레이션
        if random.random() < issues['invalid_data_probability']:
            field = random.choice(['temperature', 'humidity', 'wind_speed'])
            if field == 'temperature':
                data[field] = random.uniform(-50, 60)  # 극한 온도
            elif field == 'humidity':
                data[field] = random.uniform(-10, 120)  # 잘못된 습도
            elif field == 'wind_speed':
                data[field] = random.uniform(-5, 100)  # 잘못된 풍속
        
        return data
    
    def generate_weather_data(self):
        """시카고 날씨 데이터 생성 및 전송"""
        print(f"Starting weather data generation for {len(self.sensors)} sensors...")
        print("Press Ctrl+C to stop")
        
        self.running = True
        sent_count = 0
        error_count = 0
        
        try:
            while self.running:
                current_time = datetime.now()
                
                for sensor in self.sensors:
                    if not self.running:
                        break
                    
                    try:
                        # 기본 데이터 생성
                        weather_data = self._generate_realistic_weather(sensor, current_time)
                        
                        # 품질 문제 적용
                        final_data = self._apply_quality_issues(weather_data.copy(), sensor)
                        
                        if final_data is None:  # 센서 고장으로 데이터 없음
                            continue
                        
                        # 최종 메시지 구성
                        message = {
                            'timestamp': current_time.isoformat(),
                            'sensor_id': sensor['sensor_id'],
                            'area': sensor['area'],
                            **final_data
                        }
                        
                        # 지연 시뮬레이션
                        if random.random() < self.data_quality_issues['delay_probability']:
                            delay_seconds = random.uniform(10, 60)  # 10-60초 지연
                            delayed_time = current_time - timedelta(seconds=delay_seconds)
                            message['timestamp'] = delayed_time.isoformat()
                        
                        # Kafka로 전송
                        producer.send('weather-data', value=message)
                        
                        sent_count += 1
                        if sent_count % 50 == 0:
                            print(f"Sent {sent_count} messages, Errors: {error_count}, Failed sensors: {len(self.failed_sensors)}")
                        
                    except Exception as e:
                        error_count += 1
                        print(f"Error sending data for sensor {sensor['sensor_id']}: {e}")
                
                # 센서 복구 시뮬레이션 (1% 확률로 복구)
                if self.failed_sensors and random.random() < 0.01:
                    recovered_sensor = random.choice(list(self.failed_sensors))
                    self.failed_sensors.remove(recovered_sensor)
                    print(f"Sensor {recovered_sensor} recovered")
                
                time.sleep(5)  # 5초 간격
                
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """생성기 중단"""
        self.running = False
        producer.close()
        print(f"\nData generation stopped. Total messages sent: {sent_count}")
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        print(f"\nReceived signal {signum}, stopping data generation...")
        self.stop()
        sys.exit(0)

# 수학 모듈 import 추가
import math

if __name__ == "__main__":
    generator = WeatherDataGenerator()
    generator.generate_weather_data()
