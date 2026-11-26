"""How the voice agent talks back to the backend API."""

import httpx

from app.config import Settings, get_settings


class AgentApi:
    def __init__(self, settings: Settings | None = None):
        s = settings or get_settings()
        self._http = httpx.AsyncClient(
            base_url=s.agent_api_url.rstrip("/"),
            headers={"X-Agent-Secret": s.agent_shared_secret},
            timeout=15.0,
        )

    async def get_call(self, call_id: str) -> dict:
        r = await self._http.get(f"/agent/calls/{call_id}")
        r.raise_for_status()
        return r.json()

    async def report_outcome(
        self,
        call_id: str,
        *,
        result: str,
        notes: str = "",
        follow_up: str | None = None,
        do_not_call: bool = False,
        transcript: str | None = None,
    ) -> None:
        r = await self._http.patch(
            f"/agent/calls/{call_id}/outcome",
            json={
                "result": result,
                "notes": notes,
                "follow_up": follow_up,
                "do_not_call": do_not_call,
                "transcript": transcript,
            },
        )
        r.raise_for_status()

    async def aclose(self) -> None:
        await self._http.aclose()
