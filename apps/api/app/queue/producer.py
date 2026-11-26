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
