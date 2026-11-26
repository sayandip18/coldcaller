import phonenumbers


def normalize_in_number(raw: str) -> str:
    """Normalize to Indian E.164 (+91XXXXXXXXXX). Raises ValueError if invalid."""
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("empty number")
    try:
        parsed = phonenumbers.parse(raw, "IN")
    except phonenumbers.NumberParseException as exc:
        raise ValueError("unparseable number") from exc
    if parsed.country_code != 91 or not phonenumbers.is_valid_number(parsed):
        raise ValueError("not a valid Indian number")
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
