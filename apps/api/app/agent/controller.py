"""Everything about a call that is not audio: outcome, guards, timers, and reporting.

Kept free of LiveKit types (it only needs three callbacks) so it can be tested without a
room, a microphone or an LLM.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from app.agent.guards import Guard, screen_utterance

log = logging.getLogger("agent.call")

OutcomeResult = Literal[
    "interested",
    "not_interested",
    "callback_requested",
    "wrong_person",
    "voicemail",
    "do_not_call",
    "customer_hung_up",
    "no_response",
    "max_duration",
    "error",
]

OPT_OUT_REPLY = "Understood, I'll remove your number right away. Sorry for the disturbance. Goodbye."
STILL_THERE = "Hello, are you still there?"
SILENCE_GOODBYE = "I can't hear you, so I'll end the call now. Goodbye."
TIME_UP_REPLY = "I'm sorry, I have to end the call now. Thank you for your time. Goodbye."


@dataclass
class CallOutcome:
    result: str
    notes: str = ""
    follow_up: str | None = None
    do_not_call: bool = False


class CallController:
    def __init__(
        self,
        *,
        call_id: str,
        report: Callable[..., Awaitable[None]],
        speak: Callable[[str], Awaitable[None]],
        end: Callable[[], Awaitable[None]],
        get_transcript: Callable[[], str],
        max_seconds: float,
    ):
        self.call_id = call_id
        self._report, self._speak, self._end = report, speak, end
        self._get_transcript = get_transcript
        self.max_seconds = max_seconds
        self.outcome: CallOutcome | None = None
        self.turns = 0
        self.away_count = 0
        self._ending = False
        self._reported = False
        self._timer: asyncio.Task | None = None

    # ------------------------------------------------------------------ outcome
    def set_outcome(
        self,
        result: str,
        notes: str = "",
        follow_up: str | None = None,
        do_not_call: bool = False,
    ) -> None:
        """First outcome wins, except that an opt-out is never overwritten or downgraded."""
        do_not_call = do_not_call or result == "do_not_call"
        if self.outcome is not None and not (do_not_call and not self.outcome.do_not_call):
            return
        self.outcome = CallOutcome(
            result="do_not_call" if do_not_call else result,
            notes=notes,
            follow_up=follow_up,
            do_not_call=do_not_call,
        )

    # --------------------------------------------------------------- hang-up flow
    async def hang_up(self) -> None:
        if self._ending:
            return
        self._ending = True
        await self._end()

    async def say_then_hang_up(self, text: str, result: str, **outcome) -> None:
        self.set_outcome(result, **outcome)
        if not self._ending:
            try:
                await self._speak(text)
            except Exception:
                log.exception("could not speak closing line for %s", self.call_id)
        await self.hang_up()

    # --------------------------------------------------------------------- guards
    async def handle_user_text(self, text: str) -> bool:
        """Called for every finished customer turn. Returns True if a guard handled the turn,
        in which case the LLM must not reply."""
        if self._ending:
            return True
        self.turns += 1
        self.away_count = 0
        guard = screen_utterance(text, self.turns)
        if guard is Guard.opt_out:
            log.info("opt-out heard on call %s", self.call_id)
            await self.say_then_hang_up(OPT_OUT_REPLY, "do_not_call", notes=text[:500], do_not_call=True)
            return True
        if guard is Guard.voicemail:
            log.info("voicemail detected on call %s", self.call_id)
            self.set_outcome("voicemail", notes=text[:500])
            await self.hang_up()  # no pitch, no message
            return True
        return False

    async def on_user_away(self) -> None:
        """The line has been silent. Ask once, then give up."""
        if self._ending:
            return
        self.away_count += 1
        if self.away_count == 1:
            await self._speak(STILL_THERE)
        else:
            await self.say_then_hang_up(SILENCE_GOODBYE, "no_response")

    def on_customer_left(self) -> None:
        self.set_outcome("customer_hung_up")

    # --------------------------------------------------------------------- timer
    def start_timer(self) -> None:
        self._timer = asyncio.create_task(self._time_limit())

    def stop_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    async def _time_limit(self) -> None:
        await asyncio.sleep(self.max_seconds)
        log.info("call %s hit the %ss limit", self.call_id, self.max_seconds)
        await self.say_then_hang_up(TIME_UP_REPLY, "max_duration")

    # ------------------------------------------------------------------ reporting
    async def finish(self) -> None:
        """Send the outcome and transcript to the API exactly once."""
        if self._reported:
            return
        self._reported = True
        self.stop_timer()
        outcome = self.outcome or CallOutcome(result="customer_hung_up")
        try:
            await self._report(
                self.call_id,
                result=outcome.result,
                notes=outcome.notes,
                follow_up=outcome.follow_up,
                do_not_call=outcome.do_not_call,
                transcript=self._get_transcript(),
            )
        except Exception:
            log.exception("could not report outcome for %s", self.call_id)
            raise
