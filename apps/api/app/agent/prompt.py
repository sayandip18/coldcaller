"""Builds the agent's system prompt from the campaign script and the contact's fields."""

import re

_PLACEHOLDER = re.compile(r"\{(\w+)\}")
MAX_FIELD_CHARS = 200

BASE_RULES = """\
You are a polite, concise voice assistant making an outbound phone call on behalf of a business.

How to speak
- This is a live phone call. Reply in one or two short sentences. No lists, no markdown, no emojis, no symbols.
- Speak plain English suited to India. Say numbers and amounts the way a person would say them aloud.
- Ask one question at a time and let the person answer.

Honesty and consent
- In your first message say hello, say you are an automated AI assistant calling on behalf of the company named in the script, give the purpose of the call in a few words, and ask if it is a good time to talk.
- Never pretend to be a human. If asked, say you are an AI assistant.
- Only state facts that are in the script. If you do not know something, say a colleague will follow up.
- Never ask for or accept card numbers, OTPs, passwords, PINs or ID numbers. If offered, tell them not to share it.

Ending the call
- If the person is busy, offer to call back at a better time, note it, and end politely.
- If they are not interested, thank them and end. Do not push more than once.
- If they ask you not to call again or to remove their number, apologise, and end with result do_not_call.
- If you reach a voicemail greeting or an automated system, end immediately with result voicemail. Do not leave a message.
- If you have the wrong person, apologise and end with result wrong_person.
- When the conversation is finished, say a brief goodbye and call the end_call tool exactly once. Always call it, with the result that best fits the call.
"""


def clean_value(value: str) -> str:
    """Contact fields come from an uploaded CSV, so keep them to a single short line."""
    return re.sub(r"\s+", " ", str(value)).strip()[:MAX_FIELD_CHARS]


def render_script(script: str, contact_name: str, custom_fields: dict[str, str]) -> str:
    """Fill {name}-style placeholders. A missing value never leaks a raw `{placeholder}`
    into speech: {name} falls back to "there", anything else to an empty string."""
    values = {k.lower(): clean_value(v) for k, v in custom_fields.items()}
    values["name"] = clean_value(contact_name) or values.get("name", "") or "there"

    def sub(match: re.Match[str]) -> str:
        return values.get(match.group(1).lower(), "")

    rendered = _PLACEHOLDER.sub(sub, script)
    return re.sub(r"[ \t]{2,}", " ", rendered).strip()


def build_instructions(script: str, contact_name: str, custom_fields: dict[str, str]) -> str:
    who = clean_value(contact_name) or "unknown"
    return (
        f"{BASE_RULES}\n"
        f"The person you are calling: {who}\n\n"
        f"Campaign script (follow it, but keep the rules above):\n"
        f"{render_script(script, contact_name, custom_fields)}\n"
    )
