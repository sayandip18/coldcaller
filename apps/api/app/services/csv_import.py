import csv
import io
from dataclasses import dataclass, field

from app.services.phone import normalize_in_number

MAX_ROWS = 10_000
PHONE_KEYS = ("phone", "phone_number", "mobile", "number")
NAME_KEYS = ("name", "contact_name")


class CsvError(ValueError):
    pass


@dataclass
class ParsedRow:
    to_number: str
    contact_name: str
    custom_fields: dict[str, str]


@dataclass
class ParseResult:
    rows: list[ParsedRow] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)


def parse_contacts_csv(data: bytes) -> ParseResult:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvError("File must be UTF-8 encoded") from exc

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise CsvError("CSV has no header row")
    headers = {h: h.strip().lower() for h in reader.fieldnames if h}
    phone_col = next((h for h, n in headers.items() if n in PHONE_KEYS), None)
    if phone_col is None:
        raise CsvError(f"CSV needs a phone column (one of: {', '.join(PHONE_KEYS)})")
    name_col = next((h for h, n in headers.items() if n in NAME_KEYS), None)

    result = ParseResult()
    seen: set[str] = set()
    for line_no, raw in enumerate(reader, start=2):
        if len(result.rows) + len(result.rejected) >= MAX_ROWS:
            raise CsvError(f"CSV exceeds {MAX_ROWS} rows")
        raw_phone = (raw.get(phone_col) or "").strip()
        if not raw_phone and not any((v or "").strip() for v in raw.values() if isinstance(v, str)):
            continue  # blank line
        try:
            number = normalize_in_number(raw_phone)
        except ValueError as exc:
            result.rejected.append({"line": line_no, "phone": raw_phone, "reason": str(exc)})
            continue
        if number in seen:
            result.rejected.append({"line": line_no, "phone": raw_phone, "reason": "duplicate"})
            continue
        seen.add(number)
        custom = {
            headers[h]: (v or "").strip()
            for h, v in raw.items()
            if h in headers and h not in (phone_col, name_col) and isinstance(v, str)
        }
        name = (raw.get(name_col) or "").strip() if name_col else ""
        result.rows.append(ParsedRow(number, name, custom))
    return result
