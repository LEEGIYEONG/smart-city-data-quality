import requests
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
from ..data_quality.evaluator import QualityMetric

logger = logging.getLogger(__name__)

class SlackNotifier:
    """Slack 알림 전송기"""
    
    def __init__(self, webhook_url: str, channel: str = '#smart-city-alerts'):
        self.webhook_url = webhook_url
        self.channel = channel
        self.enabled = bool(webhook_url)
        
        if not self.enabled:
            logger.warning("Slack webhook URL not configured. Notifications will be logged only.")
    
    def send_quality_alert(self, sensor_id: str, failed_metrics: List[QualityMetric]) -> bool:
        """데이터 품질 알림 전송"""
        if not failed_metrics:
            return True
        
        # 심각도 결정
        severity = self._determine_severity(failed_metrics)
        color = self._get_color_by_severity(severity)
        
        # 메시지 구성
        title = f"🚨 데이터 품질 알림 - {sensor_id}"
        
        fields = []
        for metric in failed_metrics:
            fields.append({
                "title": f"{metric.metric_name.replace('_', ' ').title()}",
                "value": f"현재값: {metric.value:.2f}\n임계값: {metric.threshold:.2f}\n상태: {metric.status}",
                "short": True
            })
        
        attachment = {
            "color": color,
            "title": title,
            "fields": fields,
            "footer": "Smart City Data Quality System",
            "ts": int(datetime.now().timestamp())
        }
        
        message = {
            "channel": self.channel,
            "username": "DataQualityBot",
            "icon_emoji": ":warning:",
            "text": f"센서 {sensor_id}에서 데이터 품질 문제가 감지되었습니다.",
            "attachments": [attachment]
        }
        
        return self._send_message(message)
    
    def send_system_alert(self, alert_type: str, message: str, severity: str = "MEDIUM") -> bool:
        """시스템 알림 전송"""
        color = self._get_color_by_severity(severity)
        emoji = self._get_emoji_by_alert_type(alert_type)
        
        attachment = {
            "color": color,
            "title": f"{emoji} 시스템 알림 - {alert_type}",
            "text": message,
            "footer": "Smart City Data Quality System",
            "ts": int(datetime.now().timestamp())
        }
        
        slack_message = {
            "channel": self.channel,
            "username": "SystemBot",
            "icon_emoji": ":robot_face:",
            "attachments": [attachment]
        }
        
        return self._send_message(slack_message)
    
    def send_daily_summary(self, summary: Dict[str, Any]) -> bool:
        """일일 요약 보고서 전송"""
        title = "📊 일일 데이터 품질 요약 보고서"
        
        fields = [
            {
                "title": "총 센서 수",
                "value": str(summary.get('total_sensors', 0)),
                "short": True
            },
            {
                "title": "처리된 데이터 건수",
                "value": f"{summary.get('total_records', 0):,}",
                "short": True
            },
            {
                "title": "품질 평가 수행 횟수",
                "value": f"{summary.get('quality_checks', 0):,}",
                "short": True
            },
            {
                "title": "실패한 품질 검사",
                "value": f"{summary.get('failed_checks', 0):,}",
                "short": True
            }
        ]
        
        # 상위 문제 센서들
        if summary.get('top_problematic_sensors'):
            problem_sensors = summary['top_problematic_sensors'][:5]
            sensor_list = "\n".join([f"• {s['sensor_id']}: {s['fail_count']}건" for s in problem_sensors])
            fields.append({
                "title": "문제 센서 Top 5",
                "value": sensor_list,
                "short": False
            })
        
        attachment = {
            "color": "good",
            "title": title,
            "fields": fields,
            "footer": "Smart City Data Quality System",
            "ts": int(datetime.now().timestamp())
        }
        
        message = {
            "channel": self.channel,
            "username": "ReportBot",
            "icon_emoji": ":bar_chart:",
            "attachments": [attachment]
        }
        
        return self._send_message(message)
    
    def _send_message(self, message: Dict[str, Any]) -> bool:
        """실제 메시지 전송"""
        if not self.enabled:
            logger.info(f"Slack notification (disabled): {message.get('text', 'No text')}")
            return True
        
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(message),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
                return True
            else:
                logger.error(f"Failed to send Slack notification: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending Slack notification: {e}")
            return False
    
    def _determine_severity(self, metrics: List[QualityMetric]) -> str:
        """메트릭 실패 수에 따른 심각도 결정"""
        fail_count = len([m for m in metrics if m.status == 'FAIL'])
        
        if fail_count >= 3:
            return "CRITICAL"
        elif fail_count >= 2:
            return "HIGH"
        elif fail_count >= 1:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _get_color_by_severity(self, severity: str) -> str:
        """심각도별 색상 반환"""
        colors = {
            "LOW": "good",
            "MEDIUM": "warning", 
            "HIGH": "danger",
            "CRITICAL": "#FF0000"
        }
        return colors.get(severity, "warning")
    
    def _get_emoji_by_alert_type(self, alert_type: str) -> str:
        """알림 타입별 이모지 반환"""
        emojis = {
            "QUALITY_ALERT": "⚠️",
            "SYSTEM_ERROR": "🔥",
            "SENSOR_OFFLINE": "📵",
            "DATA_DELAY": "⏰",
            "THRESHOLD_BREACH": "📊"
        }
        return emojis.get(alert_type, "ℹ️")
