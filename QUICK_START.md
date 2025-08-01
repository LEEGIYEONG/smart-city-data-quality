# 🚀 Smart City Data Quality Monitor - 실행 가이드

## 단계별 실행 방법

### 1️⃣ 환경 준비
```cmd
# Windows PowerShell에서 실행
cd c:\Users\LEEGIYEONG\Documents\Paper\smart_city_dq

# Docker 서비스 시작
docker-compose up -d

# 서비스 준비 대기 (약 30초)
timeout /t 30

# Python 의존성 설치
pip install -r requirements.txt
```

### 2️⃣ 데이터베이스 초기화 확인
```cmd
python scripts\test_setup.py
```

### 3️⃣ 데이터 생성기 시작 (터미널 1)
```cmd
python src\data_producer\producer.py
```
- 70개 센서에서 5초마다 데이터 생성
- 시카고 5개 지역 시뮬레이션
- 품질 문제 자동 시뮬레이션 (결측값, 이상값, 지연, 센서 고장)

### 4️⃣ 품질 모니터링 시스템 시작 (터미널 2)
```cmd
python src\main.py
```
- 실시간 데이터 품질 평가
- ClickHouse 데이터 저장
- Slack 알림 (설정된 경우)

### 5️⃣ 모니터링 대시보드 확인

**Grafana 대시보드**: http://localhost:3000
- 계정: admin / admin
- "Smart City Data Quality Dashboard" 선택

**Kafka UI**: http://localhost:8080
- Topic: weather-data, quality-metrics 확인

**시스템 통계 확인**:
```cmd
python scripts\monitor.py
```

## 🎯 예상 결과

### 정상 작동 시 확인사항:

1. **Producer 로그**:
```
Starting weather data generation for 70 sensors...
Sent 50 messages, Errors: 0, Failed sensors: 0
Sent 100 messages, Errors: 0, Failed sensors: 1
Sensor CHI-DOW-005 recovered
```

2. **Quality Monitor 로그**:
```
Smart City Data Quality Monitor
Kafka Bootstrap Servers: localhost:9092
ClickHouse Host: localhost:9000
Quality processor starting...
Processed 100 data points
Quality alert: CHI-NOR-012 - 2 metrics failed
```

3. **Grafana 대시보드**:
- 실시간 온도 그래프
- 데이터 완전성 게이지 (80-100%)
- 품질 상태 파이차트
- 최근 알림 테이블

## 🔧 문제 해결

### Docker 서비스 확인:
```cmd
docker-compose ps
```

### 로그 확인:
```cmd
# ClickHouse 로그
docker-compose logs clickhouse

# Kafka 로그  
docker-compose logs kafka

# 애플리케이션 로그
type quality_processor.log
```

### 서비스 재시작:
```cmd
docker-compose restart
```

## 📊 성능 모니터링

- **데이터 처리량**: 초당 14개 센서 × 5초 간격 = 약 170 메시지/분
- **품질 평가**: 센서당 4개 메트릭 × 70개 센서 = 280개 메트릭/분  
- **알림 발생**: 품질 문제 시 실시간 Slack 알림

## 🎉 프로젝트 완료!

이 시스템은 GEMINI.md의 "프로젝트 1: 스마트시티 센서 데이터 품질 모니터링 시스템"을 완전히 구현한 것입니다:

✅ Stream DaQ 프레임워크 적용  
✅ 윈도우 기반 실시간 품질 평가  
✅ Kafka + ClickHouse + Grafana 스택  
✅ 시카고 센서 데이터 시뮬레이션  
✅ 상황별 Slack 알림 시스템  
✅ 실시간 모니터링 대시보드
