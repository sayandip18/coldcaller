from livekit.agents import (
    Agent,
    ChatContext,
    ChatMessage,
    RunContext,
    StopResponse,
    function_tool,
)

from app.agent.controller import CallController, OutcomeResult


def format_transcript(chat_ctx: ChatContext) -> str:
    """Turn the session history into 'Agent: ...' / 'Customer: ...' lines."""
    lines = []
    for item in chat_ctx.items:
        if getattr(item, "type", None) != "message":
            continue
        text = (item.text_content or "").strip()
        if not text or item.role not in ("user", "assistant"):
            continue
        lines.append(f"{'Customer' if item.role == 'user' else 'Agent'}: {text}")
    return "\n".join(lines)


class CallerAgent(Agent):
    def __init__(self, *, instructions: str, controller: CallController):
        super().__init__(instructions=instructions)
        self.ctl = controller

    async def on_user_turn_completed(self, turn_ctx: ChatContext, new_message: ChatMessage) -> None:
        # Opt-outs and voicemail greetings are handled by rules, not by the LLM.
        if await self.ctl.handle_user_text(new_message.text_content or ""):
            raise StopResponse()

    @function_tool
    async def end_call(
        self,
        context: RunContext,
        result: OutcomeResult,
        notes: str = "",
        follow_up: str | None = None,
        do_not_call: bool = False,
    ) -> str:
        """End the call and record how it went. Say your goodbye first, then call this once.

        Args:
            result: How the call ended. interested, not_interested, callback_requested,
                wrong_person, voicemail, do_not_call, or no_response.
            notes: One or two sentences summarising what the customer said.
            follow_up: What should happen next (for example "call back tomorrow after 5pm"), if anything.
            do_not_call: True if the customer asked not to be contacted again.
        """
        self.ctl.set_outcome(result, notes=notes, follow_up=follow_up, do_not_call=do_not_call)
        await context.wait_for_playout()  # let the goodbye finish before the line drops
        await self.ctl.hang_up()
        return "The call has ended."
