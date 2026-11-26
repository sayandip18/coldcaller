"""Lua scripts. All time comes from Redis TIME so workers on different hosts agree."""

_NOW = "local t = redis.call('TIME'); local now = t[1] * 1000 + math.floor(t[2] / 1000)\n"

# KEYS: one token-bucket hash per limit. ARGV[i]: rate (calls/sec) for KEYS[i].
# Bucket capacity == rate, so cps=1 means strictly one new call per second.
# Returns 0 and consumes a token from every bucket, or the ms to wait (nothing consumed).
TAKE_TOKENS = (
    _NOW
    + """
local wait = 0
local tokens_after = {}
for i, key in ipairs(KEYS) do
  local rate = tonumber(ARGV[i])
  local h = redis.call('HMGET', key, 'tokens', 'ts')
  local tokens = tonumber(h[1])
  local ts = tonumber(h[2])
  if tokens == nil then tokens = rate; ts = now end
  tokens = math.min(rate, tokens + (now - ts) * rate / 1000)
  tokens_after[i] = tokens
  if tokens < 1 then
    local w = math.ceil((1 - tokens) * 1000 / rate)
    if w < 1 then w = 1 end
    if w > wait then wait = w end
  end
end
if wait > 0 then return wait end
for i, key in ipairs(KEYS) do
  redis.call('HSET', key, 'tokens', tostring(tokens_after[i] - 1), 'ts', now)
  redis.call('PEXPIRE', key, 60000)
end
return 0
"""
)

# KEYS: lease ZSETs (member = call_id, score = lease expiry ms).
# ARGV[1] call_id, ARGV[2] lease ttl ms, ARGV[2+i] max leases for KEYS[i].
# Expired leases are dropped on every acquire, so a crashed worker cannot leak capacity.
ACQUIRE_CHANNEL = (
    _NOW
    + """
local call_id = ARGV[1]
local ttl = tonumber(ARGV[2])
for i, key in ipairs(KEYS) do
  redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
  local limit = tonumber(ARGV[2 + i])
  if redis.call('ZSCORE', key, call_id) == false and redis.call('ZCARD', key) >= limit then
    return 0
  end
end
for _, key in ipairs(KEYS) do
  redis.call('ZADD', key, now + ttl, call_id)
  redis.call('PEXPIRE', key, ttl + 60000)
end
return 1
"""
)

# KEYS[1] delayed ZSET, KEYS[2] stream. ARGV[1] max items. Moves due items onto the stream.
MOVE_DUE = (
    _NOW
    + """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('XADD', KEYS[2], '*', 'call_id', id)
end
return #ids
"""
)
