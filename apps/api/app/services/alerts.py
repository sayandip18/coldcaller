"""Campaign-finished notifications for other systems.

`AlertSink` is the seam: with no SNS_TOPIC_ARN the alert is only logged; with one it is
published to that SNS topic. Alerts are best-effort (a failed publish is logged, never raised),
because the database already records the campaign as done and webhooks cover per-user needs.
"""

import json
import logging
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.db.models import Call, Campaign

log = logging.getLogger(__name__)


class AlertSink(Protocol):
    def publish(self, subject: str, message: dict) -> None: ...


class LogSink:
    def publish(self, subject: str, message: dict) -> None:
        log.info("alert: %s %s", subject, json.dumps(message))


class SnsSink:
    def __init__(self, topic_arn: str, client=None):
        if client is None:
            import boto3  # imported lazily so the dependency is only touched when SNS is used

            client = boto3.client("sns")
        self.topic_arn, self.client = topic_arn, client

    def publish(self, subject: str, message: dict) -> None:
        self.client.publish(
            TopicArn=self.topic_arn,
            Subject=subject[:100],  # SNS limit
            Message=json.dumps(message),
            MessageAttributes={"event": {"DataType": "String", "StringValue": message["event"]}},
        )


def get_sink(settings: Settings | None = None) -> AlertSink:
    s = settings or get_settings()
    return SnsSink(s.sns_topic_arn) if s.sns_topic_arn else LogSink()


def campaign_finished_message(db: Session, campaign_id: str) -> dict | None:
    campaign = db.get(Campaign, campaign_id)
    if campaign is None:
        return None
    counts = dict(
        db.execute(
            select(Call.status, func.count()).where(Call.campaign_id == campaign_id).group_by(Call.status)
        ).all()
    )
    return {
        "event": "campaign.done",
        "campaign_id": campaign.id,
        "name": campaign.name,
        "owner_id": campaign.owner_id,
        "total": sum(counts.values()),
        "counts": counts,
    }


def alert_campaign_finished(db: Session, campaign_id: str, sink: AlertSink) -> bool:
    """Publish the alert; returns False (and logs) instead of raising on any failure."""
    try:
        message = campaign_finished_message(db, campaign_id)
        if message is None:
            return False
        sink.publish(f"Campaign finished: {message['name']}", message)
        return True
    except Exception:
        log.exception("could not publish the finished alert for campaign %s", campaign_id)
        return False
