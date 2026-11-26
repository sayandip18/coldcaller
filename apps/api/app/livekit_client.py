"""Thin async wrapper over the LiveKit server API (SIP dialing, agent dispatch, rooms).

The wrapper exists so the dialer can be tested with a fake and so the rest of the code
never touches protobuf request types.
"""

from livekit import api
from livekit.protocol import agent_dispatch as proto_dispatch
from livekit.protocol import room as proto_room
from livekit.protocol import sip as proto_sip

from app.config import Settings, get_settings

# SIP final responses (RFC 3261) mapped to what they mean for a cold call.
BUSY_CODES = {486, 600, 603}  # busy here / busy everywhere / decline
NO_ANSWER_CODES = {408, 480, 487}  # timeout / temporarily unavailable / request terminated
INVALID_NUMBER_CODES = {404, 410, 484, 604}  # not found / gone / address incomplete / no such number


class SipFailure(Exception):
    def __init__(self, kind: str, reason: str):
        super().__init__(reason)
        self.kind = kind  # busy | no_answer | failed
        self.reason = reason


def classify_twirp_error(exc: api.TwirpError) -> SipFailure | None:
    """Return a SipFailure if the carrier/phone refused the call, None if it looks like an
    infrastructure problem (LiveKit unreachable, auth, etc.) that is worth retrying."""
    meta = exc.metadata or {}
    raw = meta.get("sip_status_code")
    try:
        code = int(raw) if raw is not None else None
    except (TypeError, ValueError):
        code = None
    text = meta.get("sip_status") or exc.message or ""
    if code in BUSY_CODES:
        return SipFailure("busy", f"SIP {code} {text}".strip())
    if code in NO_ANSWER_CODES:
        return SipFailure("no_answer", f"SIP {code} {text}".strip())
    # A phone that never picks up (or an unreachable carrier) shows up with no SIP status at
    # all: LiveKit reports code "canceled"/HTTP 408, message "sip request timed out".
    if code is None and (exc.status == 408 or exc.code in ("canceled", "deadline_exceeded")):
        return SipFailure("no_answer", f"no answer (timeout: {text})".strip())
    if code in INVALID_NUMBER_CODES:
        return SipFailure("failed", f"invalid number (SIP {code} {text})".strip())
    if code is not None:
        return SipFailure("failed", f"SIP {code} {text}".strip())
    return None


class LiveKitClient:
    def __init__(self, settings: Settings | None = None):
        s = settings or get_settings()
        self._api = api.LiveKitAPI(
            url=s.livekit_http_url, api_key=s.livekit_api_key, api_secret=s.livekit_api_secret
        )

    async def dial(
        self,
        *,
        trunk_id: str,
        to_number: str,
        from_number: str,
        room: str,
        identity: str,
        attributes: dict[str, str],
        ring_timeout_s: int,
        max_call_s: int,
    ) -> None:
        """Place an outbound call. Returns only once the callee answers; raises TwirpError if not."""
        req = proto_sip.CreateSIPParticipantRequest(
            sip_trunk_id=trunk_id,
            sip_call_to=to_number,
            sip_number=from_number,
            room_name=room,
            participant_identity=identity,
            participant_name=to_number,
            participant_attributes=attributes,
            wait_until_answered=True,
            ringing_timeout={"seconds": ring_timeout_s},
            max_call_duration={"seconds": max_call_s},
        )
        await self._api.sip.create_sip_participant(req, timeout=ring_timeout_s + 15)

    async def dispatch_agent(self, room: str, agent_name: str, metadata: str) -> None:
        await self._api.agent_dispatch.create_dispatch(
            proto_dispatch.CreateAgentDispatchRequest(agent_name=agent_name, room=room, metadata=metadata)
        )

    async def delete_room(self, room: str) -> None:
        await self._api.room.delete_room(proto_room.DeleteRoomRequest(room=room))

    async def create_outbound_trunk(
        self, *, name: str, address: str, numbers: list[str], username: str = "", password: str = ""
    ) -> str:
        info = proto_sip.SIPOutboundTrunkInfo(
            name=name,
            address=address,
            numbers=numbers,
            auth_username=username,
            auth_password=password,
        )
        res = await self._api.sip.create_sip_outbound_trunk(
            proto_sip.CreateSIPOutboundTrunkRequest(trunk=info)
        )
        return res.sip_trunk_id

    async def list_outbound_trunks(self) -> list[tuple[str, str, str]]:
        res = await self._api.sip.list_sip_outbound_trunk(proto_sip.ListSIPOutboundTrunkRequest())
        return [(t.sip_trunk_id, t.name, t.address) for t in res.items]

    async def aclose(self) -> None:
        await self._api.aclose()
