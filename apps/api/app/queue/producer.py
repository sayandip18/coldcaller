"""Sync producer used by the API process to put queued calls on the stream."""

import logging
from functools import lru_cache

import redis

from app.config import get_settings
from app.queue import keys

log = logging.getLogger(__name__)


@lru_cache
def _client() -> redis.Redis:
    return redis.Redis.from_url(get_settings().redis_url, decode_responses=True)


def enqueue_calls(call_ids: list[str]) -> None:
    """Best effort. The DB row is the source of truth; if Redis is down the worker's
    orphan sweeper re-enqueues any `queued` call that never made it onto the stream."""
    if not call_ids:
        return
    try:
        pipe = _client().pipeline()
        for cid in call_ids:
            pipe.sadd(keys.ENQUEUED, cid)
            pipe.xadd(keys.STREAM, {"call_id": cid})
        pipe.execute()
    except redis.RedisError:
        log.exception("enqueue failed for %d calls; sweeper will recover them", len(call_ids))


def release_channels(call_id: str, campaign_id: str | None) -> None:
    """Free a call's concurrency slots (used by the API process on hang-up webhooks)."""
    try:
        pipe = _client().pipeline()
        pipe.zrem(keys.GLOBAL_CHANNELS, call_id)
        if campaign_id:
            pipe.zrem(keys.campaign_channels(campaign_id), call_id)
        pipe.execute()
    except redis.RedisError:
        # The worker's lease reconciliation (and lease expiry) will free it.
        log.exception("could not release channels for %s", call_id)


def redis_client() -> redis.Redis:
    """Shared sync Redis client for API-side features (rate limits, DLQ tooling)."""
    return _client()
