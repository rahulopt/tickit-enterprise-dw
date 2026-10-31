"""Pure, AWS-free validation for the isolated acceptance workflow.

The test business timezone is UTC. Naive CSV timestamps represent UTC instants.
This handler must run before ParallelBronzeToSilver in the isolated workflow.
"""
import re
from datetime import date, datetime, timezone


def validate_input(event, now=None):
    if not isinstance(event, dict):
        raise ValueError('Input must be a JSON object')
    raw = event.get('load_date')
    if not isinstance(raw, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', raw):
        raise ValueError('load_date must use YYYY-MM-DD')
    try:
        day = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError('load_date must be a valid calendar date') from exc
    if event.get('business_timezone', 'UTC') != 'UTC':
        raise ValueError('The isolated test contract requires business_timezone=UTC')
    instant = now if now is not None else datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise ValueError('The validation clock must be timezone-aware')
    if day > instant.astimezone(timezone.utc).date():
        raise ValueError('Future load dates are not permitted')
    return dict(event, business_timezone='UTC')


def lambda_handler(event, context):
    return validate_input(event)
