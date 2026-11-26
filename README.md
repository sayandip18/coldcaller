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
