"""Which caller IDs (the number the customer sees) this deployment may present.

Promotional calls in India must come from numbers the telecom regulator has allotted for that
purpose, and carriers reject or flag anything else. Which numbers those are depends on the
carrier agreement, so this is configuration, not hard-coded:

    ALLOWED_CALLER_IDS=+911400000001,+91140*      exact numbers, or a prefix ending in *
    DEFAULT_CALLER_ID=+911400000001               used when a campaign/call names none

With no allow-list (dev) any caller ID is accepted. With one, a campaign must use a listed
number, and the worker refuses to dial with anything else.
"""

import re

from app.config import Settings, get_settings

_NON_DIGITS = re.compile(r"[^\d+]")


def normalize(raw: str) -> str:
    """Keep digits and a leading +; '+91 140-0000001' -> '+911400000001'."""
    cleaned = _NON_DIGITS.sub("", (raw or "").strip())
    return ("+" if cleaned.startswith("+") else "") + cleaned.lstrip("+")


def _patterns(settings: Settings) -> list[str]:
    return [normalize(p.rstrip("*")) + ("*" if p.strip().endswith("*") else "")
            for p in settings.allowed_caller_ids.split(",") if p.strip()]


def effective(caller_id: str, settings: Settings | None = None) -> str:
    """The caller ID that will actually be presented: the given one, else the default."""
    s = settings or get_settings()
    return normalize(caller_id) or normalize(s.default_caller_id)


def caller_id_error(caller_id: str, settings: Settings | None = None) -> str | None:
    """None if the caller ID may be used, else a human-readable reason."""
    s = settings or get_settings()
    patterns = _patterns(s)
    value = effective(caller_id, s)
    if not patterns:
        return None
    if not value:
        return "A caller ID is required (none was given and DEFAULT_CALLER_ID is not set)"
    for pattern in patterns:
        if pattern.endswith("*"):
            if value.startswith(pattern[:-1]):
                return None
        elif value == pattern:
            return None
    return f"Caller ID {value} is not on this deployment's allowed list"
