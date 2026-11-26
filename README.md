# Cold Caller

A voice AI agent that cold calls a set of phone numbers provided.
Design: [architecture.md](architecture.md). Implementation plan: [PLAN.md](PLAN.md).

## Quick start

```bash
pnpm install:all
cp .env.example apps/api/.env      # optional, defaults work with docker-compose
pnpm infra:up                      # Postgres + Redis
pnpm db:migrate
pnpm create-admin --email you@example.com   # prints a generated password once
pnpm dev                           # API :8000, worker, web :5173
```

There is no public signup: an admin creates accounts under **Users** in the UI.

## Commands

| Command | What it does |
| --- | --- |
| `pnpm test:api` | Backend tests (needs `pnpm infra:up`; uses the `coldcaller_test` database) |
| `pnpm dev:worker` | Run only the dial worker (`pnpm dev` already includes it) |
| `pnpm lint` | ruff, tsc, eslint |
| `pnpm create-admin --email ... [--password ...] [--reset-password]` | Create or reset an admin |

## Dialing (Phase 2)

Starting a campaign or posting a call puts the call id on a Redis stream. The worker
(`apps/api/app/worker`) pulls it, applies the DNC, calling-window, concurrency and CPS
limits, then dials. Until LiveKit is wired in (Phase 3) `DIALER=fake` simulates ringing,
answers and hang-ups so the whole pipeline can be exercised.

Limits come from the campaign (`max_concurrency`, `cps`) and from `CARRIER_CPS` /
`MAX_GLOBAL_CHANNELS` in the environment.

Redis is published on host port **6380** (not 6379) because WSL setups often already have
a Redis on 6379 that shadows the container.

## Real calls with LiveKit (Phase 3)

```bash
pnpm infra:livekit        # LiveKit server, SIP gateway, voice agent, and a fake "phone network"
pnpm livekit-trunk create --address sipp:5070 --number +911140000000
# put DIALER=livekit and LIVEKIT_SIP_TRUNK_ID=<printed id> in apps/api/.env, then:
pnpm dev
```

Flow: the worker asks LiveKit to dial through the SIP trunk and waits for the callee to
answer. On answer it marks the call `answered` and dispatches the agent into the room
(`call-<id>`). The agent reports an outcome to `PATCH /api/agent/calls/{id}/outcome`. LiveKit
webhooks (`/api/livekit/webhook`) mark the agent joining and the room finishing, and free the
call's concurrency slot. The `sipp` container answers every call, so no real carrier is needed.

* The agent has two modes, see below. Without provider keys it runs as a stub.
* The agent runs in a container because LiveKit media (WebRTC) stays inside the Docker network.
* To use a real carrier or Asterisk bridge, create a trunk with its address and credentials instead.
* `AGENT_SHARED_SECRET` and the LiveKit key/secret in `infra/` are dev defaults. Change them
  before exposing anything.

## Voice agent (Phase 4)

The agent (`apps/api/app/agent`) is a LiveKit agent: Deepgram speech-to-text, an LLM
(OpenAI `gpt-4o-mini` by default; Claude is supported), and Cartesia text-to-speech. It runs in the `agent` container.

**API keys go in `infra/agent.env`** (copy `infra/agent.env.example`; the file is gitignored and
only the agent container reads it):

```
DEEPGRAM_API_KEY=...
CARTESIA_API_KEY=...
OPENAI_API_KEY=...        # or AGENT_LLM_PROVIDER=anthropic with ANTHROPIC_API_KEY
```

Then `pnpm infra:livekit` to rebuild and restart the agent. `AGENT_MODE=auto` (the default) runs
the real conversation when all three keys are present and the stub otherwise; `AGENT_MODE=live`
fails the call with an `error` outcome if a key is missing, which is safer for production.

What the agent does on a call: opens by saying it is an AI assistant, who it is calling for and
why; follows the campaign script (with `{name}` and CSV columns filled in); and ends by calling an
`end_call` tool with a result (`interested`, `not_interested`, `callback_requested`, `wrong_person`,
`voicemail`, `do_not_call`, ...), notes and a follow-up. Rules in code, not left to the LLM:

* an opt-out ("stop calling me", "remove my number", ...) apologises, hangs up and adds the number to
  the DNC list
* a voicemail greeting early in the call ends the call without leaving a message
* 12 seconds of silence asks "are you still there?", then hangs up
* a hard stop after `AGENT_MAX_CALL_SECONDS` (default 300) with a polite goodbye

The transcript and outcome are saved on the call and shown on its page.

## Operations and hardening (Phase 5)

**Queueing.** Each campaign has its own ready queue in Redis. The worker pops a call only when
its campaign has a free channel and a CPS slot, in one atomic step, so calls waiting behind a
limit cost nothing and a slow campaign never holds up another. `CARRIER_CPS` and each campaign's
`cps` pace calls one every 1/cps seconds with no burst, because carriers enforce CPS strictly.

**Webhooks.** A campaign's `webhook_url` receives `call.completed` for each call that ends and
`campaign.done` when all calls have ended. Delivery is at-least-once with exponential backoff
(8 attempts), so de-duplicate on the `X-Coldcaller-Delivery` header. Verify each request with
`X-Coldcaller-Signature: v1=<hex>`, the HMAC-SHA256 of `"<X-Coldcaller-Timestamp>.<raw body>"`
keyed with `WEBHOOK_SIGNING_SECRET`. URLs must be public `https` (private, loopback and
link-local addresses are refused, at save time and again at delivery); set
`WEBHOOK_ALLOW_PRIVATE=true` in dev only. Status of each delivery: `GET /api/webhooks/deliveries`.

**Do-not-disturb (NCPR).** An admin uploads the register as text, one number per line:
`POST /api/ncpr/import` (multipart `file`). Numbers on it are never dialled, checked when queued and
again just before dialling. `NCPR_ENFORCE=false` disables the check.

**Alerts.** Set `SNS_TOPIC_ARN` (and AWS credentials in the worker's environment) to publish a
message when a campaign finishes; without it the alert is only logged.

**Login limits.** Failed logins are limited per email+IP (5), per IP (20) and per email (30) over
15 minutes, answered with `429` and `Retry-After`.

**Dead-letter queue.** Calls the worker gave up on (for example LiveKit was down) are listed with
with the CLI:

```
uv run --directory apps/api python -m app.cli dlq list
uv run --directory apps/api python -m app.cli dlq replay --dry-run
uv run --directory apps/api python -m app.cli dlq replay            # or --call-id <id> / --limit N
```
