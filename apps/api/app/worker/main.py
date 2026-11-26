import asyncio
import logging

from redis.asyncio import Redis

from app.config import get_settings
from app.worker.worker import Worker


async def _main() -> None:
    settings = get_settings()
    r = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await Worker(r, settings=settings).run()
    finally:
        await r.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
