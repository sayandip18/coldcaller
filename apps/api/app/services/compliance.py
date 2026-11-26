from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo


def next_allowed_time(now: datetime, tz: str, start: time, end: time) -> datetime:
    """Earliest instant >= now inside the daily [start, end) window in `tz` (UTC-aware)."""
    zone = ZoneInfo(tz)
    local = now.astimezone(zone)
    t = local.time().replace(tzinfo=None)
    if start <= t < end:
        return now
    day = local.date()
    if t >= end:
        day += timedelta(days=1)
    return datetime.combine(day, start, tzinfo=zone).astimezone(UTC)
