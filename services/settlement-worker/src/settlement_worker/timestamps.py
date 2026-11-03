"""Timestamps in D-097's format: UTC, exactly six fractional digits, Z (dlq.failed_at, D-111; the
reconciliation events' as_of and drift windows, D-114)."""

from datetime import datetime, timezone


def utc_text(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
