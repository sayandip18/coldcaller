.PHONY: dev api web lint

dev:
	pnpm dev

api:
	pnpm dev:api

web:
	pnpm dev:web

lint:
	pnpm lint
