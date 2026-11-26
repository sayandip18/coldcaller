STREAM = "calls:queue"
GROUP = "dialers"
DELAYED = "calls:delayed"  # ZSET call_id -> due time (ms, Redis clock)
DLQ = "calls:dlq"
ENQUEUED = "calls:enqueued"  # SET of call ids known to be in the stream/delayed set
HANDLER_ERRORS = "calls:handler_errors"  # HASH call_id -> unexpected-error count

GLOBAL_CHANNELS = "channels:global"  # ZSET call_id -> lease expiry (ms)
GLOBAL_CPS = "cps:global"


def campaign_channels(campaign_id: str) -> str:
    return f"channels:c:{campaign_id}"


def campaign_cps(campaign_id: str) -> str:
    return f"cps:c:{campaign_id}"
