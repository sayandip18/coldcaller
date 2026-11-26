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
pnpm dev                           # API on :8000, web on :5173
```

There is no public signup: an admin creates accounts under **Users** in the UI.

## Commands

| Command | What it does |
| --- | --- |
| `pnpm test:api` | Backend tests (needs `pnpm infra:up`; uses the `coldcaller_test` database) |
| `pnpm lint` | ruff, tsc, eslint |
| `pnpm create-admin --email ... [--password ...] [--reset-password]` | Create or reset an admin |
