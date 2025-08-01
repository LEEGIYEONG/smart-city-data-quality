"""
시스템 상태 모니터링 및 통계 조회 스크립트
"""
import sys
import os
from datetime import datetime, timedelta

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.storage.clickhouse_manager import ClickHouseManager
from src.config.settings import ClickHouseConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_system_statistics():
    """시스템 통계 조회"""
    try:
        config = ClickHouseConfig()
        ch = ClickHouseManager(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        
        print("=" * 60)
        print("Smart City Data Quality System Statistics")
        print("=" * 60)
        
        # 1. 전체 데이터 통계
        total_sensors = ch.get_sensor_data_count(hours=24*30)  # 30일
        recent_sensors = ch.get_sensor_data_count(hours=1)     # 1시간
        
        print(f"📊 Data Volume:")
        print(f"  - Total sensor data (30 days): {total_sensors:,}")
        print(f"  - Recent sensor data (1 hour): {recent_sensors:,}")
        
        # 2. 품질 메트릭 요약
        quality_metrics = ch.get_quality_metrics_summary(hours=24)
        
        print(f"\n🎯 Quality Metrics (24 hours):")
        if quality_metrics:
            for metric in quality_metrics[:10]:  # Top 10
                print(f"  - {metric['sensor_id']} | {metric['metric_name']}: "
                      f"Avg={metric['avg_value']:.1f}%, "
                      f"Fails={metric['fail_count']}, "
                      f"Passes={metric['pass_count']}")
        else:
            print("  - No quality metrics available")
        
        # 3. 활성 알림
        alerts = ch.get_active_alerts(hours=24)
        
        print(f"\n🚨 Active Alerts (24 hours): {len(alerts)}")
        if alerts:
            severity_count = {}
            for alert in alerts:
                severity = alert['severity']
                severity_count[severity] = severity_count.get(severity, 0) + 1
            
            for severity, count in severity_count.items():
                print(f"  - {severity}: {count} alerts")
                
            print(f"\n🔥 Recent Critical Alerts:")
            critical_alerts = [a for a in alerts if a['severity'] == 'CRITICAL'][:5]
            for alert in critical_alerts:
                print(f"  - {alert['timestamp']}: {alert['sensor_id']} - {alert['message']}")
        
        # 4. 센서별 상태
        print(f"\n📡 Sensor Health Summary:")
        
        # 센서별 실패율 계산
        sensor_stats = {}
        for metric in quality_metrics:
            sensor_id = metric['sensor_id']
            if sensor_id not in sensor_stats:
                sensor_stats[sensor_id] = {'total_checks': 0, 'failed_checks': 0}
            
            sensor_stats[sensor_id]['total_checks'] += metric['pass_count'] + metric['fail_count']
            sensor_stats[sensor_id]['failed_checks'] += metric['fail_count']
        
        # 상위 문제 센서들
        problematic_sensors = []
        for sensor_id, stats in sensor_stats.items():
            if stats['total_checks'] > 0:
                failure_rate = stats['failed_checks'] / stats['total_checks'] * 100
                problematic_sensors.append({
                    'sensor_id': sensor_id,
                    'failure_rate': failure_rate,
                    'total_checks': stats['total_checks'],
                    'failed_checks': stats['failed_checks']
                })
        
        problematic_sensors.sort(key=lambda x: x['failure_rate'], reverse=True)
        
        print(f"  - Total active sensors: {len(sensor_stats)}")
        print(f"  - Top problematic sensors:")
        for sensor in problematic_sensors[:5]:
            print(f"    {sensor['sensor_id']}: {sensor['failure_rate']:.1f}% failure rate "
                  f"({sensor['failed_checks']}/{sensor['total_checks']} checks)")
        
        print("=" * 60)
        
        return {
            'total_data_points': total_sensors,
            'recent_data_points': recent_sensors,
            'quality_metrics_count': len(quality_metrics),
            'active_alerts_count': len(alerts),
            'active_sensors_count': len(sensor_stats),
            'top_problematic_sensors': problematic_sensors[:5]
        }
        
    except Exception as e:
        logger.error(f"Error getting system statistics: {e}")
        return None

def export_quality_report(filename=None):
    """품질 보고서 내보내기"""
    if filename is None:
        filename = f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    try:
        stats = get_system_statistics()
        if stats:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Smart City Data Quality Report\n")
                f.write(f"Generated: {datetime.now().isoformat()}\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"Summary:\n")
                f.write(f"- Total data points: {stats['total_data_points']:,}\n")
                f.write(f"- Recent data points (1h): {stats['recent_data_points']:,}\n")
                f.write(f"- Quality metrics: {stats['quality_metrics_count']}\n")
                f.write(f"- Active alerts: {stats['active_alerts_count']}\n")
                f.write(f"- Active sensors: {stats['active_sensors_count']}\n\n")
                
                f.write("Top Problematic Sensors:\n")
                for sensor in stats['top_problematic_sensors']:
                    f.write(f"- {sensor['sensor_id']}: {sensor['failure_rate']:.1f}% failure rate\n")
            
            print(f"📋 Quality report exported to: {filename}")
            return True
            
    except Exception as e:
        logger.error(f"Error exporting quality report: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='System monitoring and statistics')
    parser.add_argument('--export', '-e', action='store_true', 
                       help='Export quality report to file')
    parser.add_argument('--filename', '-f', type=str,
                       help='Output filename for report')
    
    args = parser.parse_args()
    
    if args.export:
        export_quality_report(args.filename)
    else:
        get_system_statistics()
