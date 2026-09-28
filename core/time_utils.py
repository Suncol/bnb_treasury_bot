from datetime import datetime, timezone


def utc(value: datetime) -> datetime:
    """Naive historical fixtures are interpreted as UTC, never local time."""
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def age_seconds(now: datetime, then: datetime) -> float:
    return (utc(now) - utc(then)).total_seconds()


def fresh(ts: datetime | None, now: datetime, max_age: int) -> bool:
    return ts is not None and 0 <= age_seconds(now, ts) <= max_age
