"""Deterministic checks on what the customer says.

Opt-out requests are a legal obligation, so they must not depend on an LLM noticing them.
Voicemail detection is also done here so a recorded greeting never gets a sales pitch.
"""

import re
from enum import StrEnum


class Guard(StrEnum):
    opt_out = "opt_out"
    voicemail = "voicemail"


# "Don't call me" alone is ambiguous ("don't call me now, call after five" is a callback), so a
# bare "don't call (me)" only counts when it is not followed by a timing word.
_TIMING = r"(?:now|today|later|back|at|after|before|tomorrow|this|during|on|between|in)"
_OPT_OUT = re.compile(
    r"""
    \bstop\s+(?:calling|ringing|contacting|phoning|messaging)\b
  | \b(?:do\s*not|don'?t|dont)\s+(?:call|ring|phone|contact)(?:\s+me)?\s+(?:again|anymore|any\s+more)\b
  | \b(?:do\s*not|don'?t|dont)\s+(?:call|ring|phone|contact)(?:\s+me)?+\b(?!\s+""" + _TIMING + r""")
  | \bnever\s+(?:call|ring|phone|contact)\b
  | \bremove\s+(?:me|my\s+(?:number|name|details))\b
  | \btake\s+(?:me|my\s+(?:number|name))\s+off\b
  | \bdelete\s+my\s+(?:number|details)\b
  | \bunsubscribe\b
  | \b(?:do\s*not\s*disturb|dnd|do\s*not\s*call\s*(?:list|registry))\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_VOICEMAIL = re.compile(
    r"""
    \bleave\s+(?:me\s+|us\s+)?(?:a\s+|your\s+)?(?:message|name)\b
  | \bafter\s+the\s+(?:tone|beep)\b
  | \bat\s+the\s+(?:tone|beep)\b
  | \bvoice\s*mail\b
  | \bmail\s*box\b
  | \brecord\s+your\s+message\b
  | \bnot\s+available\s+to\s+take\s+your\s+call\b
  | \bis\s+not\s+available\.?\s+please\s+leave\b
  | \bthe\s+person\s+you\s+(?:are|have)\s+(?:calling|called|dialed|dialled)\b
  | \bsubscriber\s+(?:you\s+(?:are|have)\s+(?:calling|called|dialed)|is\s+(?:not|currently))\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# A greeting only sounds like voicemail at the start of the call.
VOICEMAIL_WINDOW_TURNS = 2


def screen_utterance(text: str, turn_number: int) -> Guard | None:
    """Return a Guard if the customer's words need action regardless of the LLM.
    `turn_number` is 1 for the customer's first reply."""
    if not text or not text.strip():
        return None
    if _OPT_OUT.search(text):
        return Guard.opt_out
    if turn_number <= VOICEMAIL_WINDOW_TURNS and _VOICEMAIL.search(text):
        return Guard.voicemail
    return None
