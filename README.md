
# 스마트시티 센서 데이터 품질 모니터링 시스템

![Smart City Data Quality](https://img.shields.io/badge/Smart%20City-Data%20Quality-blue)
![Python](https://img.shields.io/badge/Python-3.8%2B-green)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-orange)
![ClickHouse](https://img.shields.io/badge/ClickHouse-OLAP-red)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-blue)

## 프로젝트 개요

본 프로젝트는 스마트시티의 다양한 센서로부터 수집되는 데이터의 품질을 실시간으로 모니터링하는 시스템입니다. **Stream DaQ 프레임워크**의 개념을 적용하여, 시간적 컨텍스트를 고려한 데이터 품질 평가 및 알림 기능을 제공합니다.

### 프로젝트 특징

- **실시간 데이터 품질 평가**: 5분 시간 윈도우 기반 품질 메트릭 계산
- **Stream DaQ 적용**: 2025년 최신 연구 기반 스트리밍 데이터 품질 모니터링
- **다차원 품질 평가**: 완전성, 유효성, 적시성, 일관성 평가
- **지능형 알림 시스템**: Slack 연동 상황별 알림
- **실시간 대시보드**: Grafana 기반 실시간 모니터링
- **확장 가능한 아키텍처**: 마이크로서비스 기반 설계

##  아키텍처

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   센서 데이터    │───▶│    Kafka     │───▶│  품질 평가 엔진   │
│    Producer     │    │   Streaming  │    │ (Stream DaQ)   │
└─────────────────┘    └──────────────┘    └─────────────────┘
                                                      │
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Slack 알림     │◀───│  ClickHouse  │◀───│   데이터 저장    │
│    System       │    │   Database   │    │   & 분석 엔진    │
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │
                       ┌──────────────┐
                       │   Grafana    │
                       │  Dashboard   │
                       └──────────────┘
```

##  기술 스택

- **데이터 수집**: Apache Kafka, Schema Registry
- **실시간 처리**: Python (Kafka Consumer), Apache Spark Streaming
- **시계열 데이터베이스**: ClickHouse
- **시각화 대시보드**: Grafana
- **알림 시스템**: Slack API
- **컨테이너화**: Docker, Docker Compose

##  데이터 품질 메트릭

### 1. 완전성 (Completeness)
- **측정**: 5분 윈도우 내 결측값 비율
- **임계값**: 80% 이상 (결측률 20% 미만)
- **알림**: 임계값 미달 시 알림

### 2. 유효성 (Validity)
- **온도**: -20°C ~ 40°C (시카고 기후 고려)
- **습도**: 0% ~ 100%
- **풍속**: 0 ~ 50 m/s
- **임계값**: 95% 이상 유효

### 3. 적시성 (Timeliness)
- **측정**: 예상 데이터 수신율 (5초 간격 기준)
- **임계값**: 90% 이상 수신
- **지연 감지**: 10초 이상 지연 시 경고

### 4. 일관성 (Consistency)
- **측정**: 급격한 값 변화 탐지
- **온도 변화율**: 연속 측정 간 10°C 이상 변화 감지
- **임계값**: 80% 이상 일관성

##  설치 및 실행

### 사전 요구사항

- Python 3.8+
- Docker & Docker Compose
- 8GB+ RAM (권장)

### 1. 저장소 클론

```bash
git clone <repository-url>
cd smart_city_dq
```

### 2. 환경 설정

```bash
# 환경 변수 파일 생성
cp .env.example .env

# 필요시 .env 파일 편집 (Slack 웹훅 URL 등)
notepad .env  # Windows
# 또는
nano .env     # Linux/Mac
```

### 3. 서비스 시작

**Windows:**
```cmd
start.bat
```

**Linux/Mac:**
```bash
chmod +x start.sh
./start.sh
```

**또는 수동 실행:**
```bash
# Docker 서비스 시작
docker-compose up -d

# Python 의존성 설치
pip install -r requirements.txt

# 서비스가 준비될 때까지 대기 (약 30초)
```

### 4. 애플리케이션 실행

**터미널 1: 데이터 생성기**
```bash
python src/data_producer/producer.py
```

**터미널 2: 품질 모니터링 시스템**
```bash
python src/main.py
```

## 📊 모니터링 대시보드

시스템 실행 후 다음 URL에서 모니터링 가능:

- **Grafana 대시보드**: http://localhost:3000
  - 계정: admin / admin
  - Smart City Data Quality Dashboard 확인
  
- **Kafka UI**: http://localhost:8080
  - Kafka 토픽 및 메시지 모니터링
  
- **ClickHouse**: localhost:9000
  - 데이터베이스 직접 쿼리 가능

##  실시간 모니터링 기능

### 1. 실시간 메트릭 대시보드
- 센서별 품질 점수 실시간 표시
- 시간대별 품질 트렌드 분석
- 지역별 센서 상태 맵

### 2. 지능형 알림 시스템
- **LOW**: 단일 메트릭 실패
- **MEDIUM**: 2개 메트릭 실패
- **HIGH**: 3개 이상 메트릭 실패
- **CRITICAL**: 완전성 50% 미만

### 3. 센서 상태 추적
- 온라인/오프라인 센서 모니터링
- 센서 고장 자동 감지
- 복구 상태 실시간 알림

##  고급 기능

### 1. 동적 임계값 조정
```python
# 시간대별, 계절별 동적 임계값 적용
dynamic_thresholds = {
    'winter': {'temp_range': (-30, 10)},
    'summer': {'temp_range': (10, 45)},
    'night': {'activity_threshold': 0.7},
    'day': {'activity_threshold': 0.9}
}
```

### 2. 센서 클러스터링
- 지역별 센서 그룹 관리
- 그룹별 품질 점수 계산
- 지역 간 품질 비교 분석

### 3. 예측적 품질 분석
- 품질 저하 패턴 학습
- 센서 고장 예측
- 유지보수 일정 최적화

## 📊 성능 지표

- **처리량**: 초당 1,000+ 메시지 처리
- **지연시간**: 평균 100ms 이하
- **가용성**: 99.9% 업타임 목표
- **확장성**: 수평적 확장 지원

##  문제 해결

### 일반적인 문제

1. **Kafka 연결 실패**
   ```bash
   # Kafka 서비스 상태 확인
   docker-compose ps
   
   # Kafka 로그 확인
   docker-compose logs kafka
   ```

2. **ClickHouse 연결 실패**
   ```bash
   # ClickHouse 서비스 확인
   docker exec -it clickhouse clickhouse-client
   ```

3. **메모리 부족**
   ```bash
   # Docker 메모리 할당 증가
   # Docker Desktop > Settings > Resources > Memory
   ```

### 로그 확인

```bash
# 애플리케이션 로그
tail -f quality_processor.log

# Docker 서비스 로그
docker-compose logs -f
```

##  기여하기

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

##  참고 자료

- [Stream DaQ: Stream-First Data Quality Monitoring](https://arxiv.org/html/2506.06147v1/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Grafana Documentation](https://grafana.com/docs/)

---

**Smart City Data Quality Monitoring System** - 실시간 센서 데이터의 품질을 보장하여 스마트시티의 신뢰성을 높입니다.
