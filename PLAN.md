# Cold Caller: Low-Level Implementation Plan

Implements the HLD in `architecture.md`. Decisions made:

- Postgres + SQLAlchemy 2 + Alembic
- LiveKit Agents (Python): Deepgram STT, Claude/OpenAI LLM, Cartesia TTS
- Custom asyncio worker on Redis Streams
- Mock carrier first, real Asterisk/Pulse last
- Agent/worker pinned to Python 3.12/3.13 if `livekit-agents` lacks 3.14 support
- Calls go to India; campaigns accept CSV with custom fields injected into prompts
- User accounts (not a shared API key)

## 1. Repo layout

```
apps/
  api/                      one Python project, three process types
    app/
      main.py               FastAPI (process: api)
      config.py             pydantic-settings
      db/                   engine, session, models, alembic/
      auth/                 password hashing, sessions, deps (current_user)
      routers/              auth.py, calls.py, campaigns.py, agent_callbacks.py, webhooks.py
      schemas/
      services/             call_service, campaign_service, compliance, webhook_dispatcher
      queue/                redis client, streams, limiter.py, channels.py
      worker/               main.py (process: worker), dialer.py, finalizer.py
      livekit_client.py     SIP dial, agent dispatch
      agent/                main.py (process: agent), prompts, outcome tool
    tests/
  web/                      Vite React UI
infra/docker-compose.yml    postgres, redis, livekit-server, livekit-sip (dev)
```

Processes: `uvicorn app.main:app`, `python -m app.worker.main`, `python -m app.agent.main dev`.
Root `pnpm dev` starts all three. Postgres, Redis, LiveKit run from compose.

## 2. Data model

- **users**: `id`, `email` (unique), `password_hash` (argon2), `name`, `role` (`admin | member`), `is_active`, timestamps.
- **sessions** (or JWT refresh tokens): `id`, `user_id`, `expires_at`, `revoked_at`.
- **campaigns**: `id`, `owner_id`, `name`, `status` (`draft | running | paused | done | failed`), `script_prompt` (template with `{name}`-style placeholders), `caller_id`, `max_concurrency`, `cps`, `timezone` (default `Asia/Kolkata`), `call_window_start/end`, `total`, `completed`, `webhook_url`, timestamps.
- **calls**: `id`, `owner_id`, `campaign_id` (nullable), `to_number` (E.164, `+91`), `contact_name`, `custom_fields` (JSONB), `status`, `attempt`, `livekit_room`, `sip_call_id`, `outcome` (JSONB), `transcript`, `failure_reason`, `queued_at`, `dialed_at`, `answered_at`, `ended_at`, `idempotency_key`.
- **call_events**: `call_id`, `type`, `payload`, `at`.
- **webhook_deliveries**: `id`, `call_id`, `url`, `status`, `attempts`, `next_retry_at`.
- **dnc_numbers**: `owner_id` (or global), `number`, `source` (`user_request | ncpr | import`), `added_at`.

Call status machine: `queued -> dialing -> ringing -> answered -> in_progress -> completed | no_answer | busy | failed | skipped_dnc | skipped_window`.
Transitions use `UPDATE ... WHERE status = <expected>` so duplicate callbacks are idempotent.

## 3. Auth (user accounts)

- Email + password, argon2 hashing. Login sets an httpOnly, SameSite=Lax session cookie (server-side `sessions` table so logout and revoke work).
- Endpoints: `POST /api/auth/register`, `/login`, `/logout`, `GET /api/auth/me`.
- Every campaign, call and DNC row is scoped by `owner_id`, enforced in a shared query helper rather than per route.
- Agent callback and LiveKit webhook use separate secrets (signed LiveKit webhook token, shared secret for the agent). They are not user-authenticated.
- Registration policy (open signup vs invite-only vs admin-created) is an open item, see section 11.

## 4. Backend API

| Endpoint | Purpose |
|---|---|
| `POST /api/calls` | Single call. Validates E.164, runs compliance checks, writes `queued`, `XADD`. `Idempotency-Key` supported. |
| `POST /api/campaigns` | CSV or JSON upload. Columns: `phone`, `name`, plus any extra columns stored in `custom_fields`. Validates and normalizes numbers, reports rejected rows. Creates `draft` calls. |
| `POST /api/campaigns/{id}/start`, `/pause`, `/resume` | Bulk enqueue and pause control. |
| `GET /api/calls`, `/calls/{id}`, `/campaigns`, `/campaigns/{id}` | Lists and details with progress counters. |
| `GET/POST/DELETE /api/dnc` | Manage the DNC list. |
| `PATCH /api/calls/{id}/outcome` | Agent callback: outcome, transcript, release the channel, enqueue webhook. |
| `POST /api/livekit/webhook` | Room and SIP events. Drives status and releases channels on hang-up and no-answer. |

Prompt rendering: `script_prompt` is rendered per call with `contact_name` and `custom_fields` (missing keys fall back to a blank or a default, never a raw `{placeholder}` spoken aloud). Done at call time by the agent, using data fetched from the API.

## 5. Redis design

- Stream `calls:queue`, consumer group `dialers`. Message is only `{call_id}`; the DB is the source of truth.
- CPS limiter: Lua token bucket per carrier key.
- Channel counter: Lua "increment if below max" for `channels:{campaign_id}` and `channels:global`, with a TTL'd lease key `slot:{call_id}` and a reaper for crashed workers.
- Retries: ZSET `calls:delayed` with backoff up to `max_attempts`, then `calls:dlq`. Replay tooling deferred.
- `XAUTOCLAIM` reclaims messages from dead consumers.

## 6. Worker

Per message:
1. Load call. Skip if status is not `queued`. Delay-requeue if the campaign is paused.
2. Compliance gate (section 8): DNC and calling window. Window miss means requeue to the next window start, not a failure.
3. Acquire CPS token, then channel slot. If unavailable, sleep or requeue without consuming an attempt.
4. Set `dialing`, call LiveKit `CreateSIPParticipant` (room `call-{id}`, call id in metadata).
5. On the SIP participant joining (webhook), dispatch the agent into the room with explicit dispatch.
6. `XACK` after dial. The lifecycle continues through webhooks and the agent callback.

Finalizer: periodically marks campaigns `done` when all calls are terminal, then fires the webhook. SNS sits behind an `AlertSink` interface (DB/webhook first, SNS later).

## 7. Voice agent

- LiveKit Agents `AgentSession`: Deepgram STT (Indian English / Hindi config), LLM, Cartesia TTS, Silero VAD, turn detector.
- Fetches call and campaign from the API at start, renders the prompt with the contact fields.
- Language: confirm Hindi and Hinglish needs (see section 11). It affects the STT, TTS and voice choice.
- Voicemail detection: end the call and report `voicemail`.
- `report_outcome(result, notes, follow_up)` tool, then `PATCH /outcome` with the outcome and transcript on shutdown.
- Guardrails: max duration, silence timeout, opt-out phrase ends the call and adds the number to the DNC list.
- Opening line identifies the caller and the company and states the purpose.

## 8. India compliance (TRAI TCCCPR 2018)

This is an engineering checklist, not legal advice. Confirm the specifics with the carrier (Pulse/ConnectHub) and a legal advisor before going live.

- **Numbers**: normalize to `+91` E.164 and reject invalid numbers. Promotional or telemarketing calls must originate from registered telemarketer series (140-series) and transactional or service calls from 160-series, as assigned by the carrier. `caller_id` is validated against an allow-list configured per environment.
- **NCPR / DND scrub**: promotional calls to numbers on the National Customer Preference Register are not allowed without consent. Implement a `compliance.check(number)` interface (DB DNC list plus a pluggable NCPR scrub provider). Start with the DB list and a stub NCPR provider.
- **Calling hours**: promotional calls only 09:00 to 21:00 IST. Enforced in the worker, default window per campaign, with `skipped_window` and requeue.
- **Consent and opt-out**: store consent source per contact where available, and honor in-call opt-out immediately.
- **Disclosure**: the agent states it is an automated or AI caller at the start. Recording disclosure if recordings are stored.
- **Retention**: configurable retention for transcripts (and recordings, if added).

## 9. Web UI

Pages: Login/Register, Campaigns list, New campaign (CSV upload with preview and rejected-row report, script editor with placeholder helper), Campaign detail (live progress, call table), Call detail (timeline, transcript, outcome), Quick call, DNC list.
Data fetching: TanStack Query with 2 to 5 second polling, protected routes using `/auth/me`, and a client generated from the OpenAPI schema.

## 10. Testing and observability

- pytest with Postgres and Redis (testcontainers or compose). Unit tests for the Lua limiters, the status state machine, CSV parsing and `+91` normalization, and the compliance gate. Integration test of the full flow with a fake LiveKit client. Agent tested in LiveKit text mode. Auth tests for cross-user access.
- Structured logs with `call_id`, `campaign_id`, `user_id`. `/health` and `/ready` per process. Metrics: queue depth, active channels, answer rate.

## 11. Phases

1. **Foundation**: compose infra, config, models and migrations, **auth and user accounts**, call/campaign CRUD with CSV upload, UI shell with login. Demo: sign in, create a campaign, see `queued` rows.
2. **Queue and worker**: Redis streams, CPS and channel Lua, worker with a **fake dialer**, finalizer, calling-window and DNC gates. Demo: 50 calls respect CPS and concurrency.
3. **LiveKit integration** (done): livekit-server, livekit-sip and a sipp fake carrier in compose, real `CreateSIPParticipant` (waits for answer), webhook handler, explicit agent dispatch, a stub agent, and the agent callback endpoints (`GET/PATCH /api/agent/calls/{id}`, pulled forward from Phase 4). Demo: a campaign dialled through LiveKit SIP to a fake phone network, ending in a recorded outcome.
4. **Voice agent** (built and tested with mocks; not yet run with real provider keys): STT/LLM/TTS session, per-contact prompt rendering, `end_call` outcome tool and callback, rule-based opt-out and voicemail handling, silence and time-limit guardrails. Live-audio tuning (voice, endpointing, latency) still to do once keys are available.
5. **Hardening** (done): per-campaign ready queues, durable outbound webhooks with retries and an SSRF guard, an NCPR check, an SNS alert sink, login rate limiting, and DLQ replay.

   Replaced the Phase 2 requeue-on-limit design: each campaign has its own ready queue and a call is popped only when its campaign has a free channel and rate-limit slot (one atomic script), so waiting calls cost nothing and a slow campaign cannot hold up others. Rate limiting now paces calls 1/cps apart with no burst, because carriers enforce CPS strictly.
6. **Real carrier** (bridge and load testing done; real carrier pending): Asterisk bridge configured from environment and proven end to end against simulated carriers (busy, no answer, decline, invalid number, refused, error, unreachable all map correctly), caller-ID allow-list, load-test tool, and a go-live runbook (`docs/going-live.md`). **Still needs a carrier account**: Pulse/ConnectHub credentials, number series, a test number, and a repeat of the load test with the real agent on production hardware.

## Open items

- Signup policy: open signup, invite-only, or admin-created accounts?
- Languages: English only, or also Hindi/Hinglish (affects STT, TTS and voice)?
- Are the calls promotional (140-series, NCPR scrub, 9 to 9 window) or transactional/service (160-series)?
- Do you already have the Pulse/ConnectHub account and a 140/160 number allocated?
