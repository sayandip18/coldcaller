"""Lua scripts. All time comes from Redis TIME so workers on different hosts agree."""

_NOW = "local t = redis.call('TIME'); local now = t[1] * 1000 + math.floor(t[2] / 1000)\n"

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

# Pop the next waiting call of one campaign -- but only if the campaign may start a call right
# now. Checks the global and campaign channel caps and both token buckets, and changes nothing
# unless every check passes. Waiting calls therefore cost nothing while blocked.
#
# KEYS: 1 ready list, 2 active set, 3 global leases, 4 campaign leases, 5 global bucket,
#       6 campaign bucket, 7 handoff zset
# ARGV: 1 queue id, 2 lease ms, 3 global max, 4 campaign max (0 = none), 5 global rate,
#       6 campaign rate (0 = none), 7 handoff ms
# Returns {code, value}: 1 popped (value = call id) | 2 campaign channels full |
#       3 global channels full | 4 rate limited (value = ms to wait) | 5 queue empty
CLAIM_NEXT = (
    _NOW
    + """
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[2], ARGV[1])
  return {5, 0}
end
local lease_ms = tonumber(ARGV[2])
local camp_max = tonumber(ARGV[4])
local g_rate = tonumber(ARGV[5])
local c_rate = tonumber(ARGV[6])

redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
if redis.call('ZCARD', KEYS[3]) >= tonumber(ARGV[3]) then return {3, 0} end
if camp_max > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', now)
  if redis.call('ZCARD', KEYS[4]) >= camp_max then return {2, 0} end
end

local function refill(key, rate)
  local h = redis.call('HMGET', key, 'tokens', 'ts')
  local tokens = tonumber(h[1])
  local ts = tonumber(h[2])
  if tokens == nil then tokens = 1; ts = now end
  -- Capacity is one token plus up to 15 ms worth (at most half a token). The extra lets a call
  -- that was started a little late be paid back, so the configured rate is actually reached;
  -- without it every scheduling delay is lost and the rate falls ~20% short. Capacity stays
  -- below 2, so calls are still paced one at a time and never burst.
  local cap = 1 + math.min(0.5, 0.015 * rate)
  return math.min(cap, tokens + (now - ts) * rate / 1000)
end

local wait = 0
local g_tokens = refill(KEYS[5], g_rate)
if g_tokens < 1 then wait = math.max(1, math.ceil((1 - g_tokens) * 1000 / g_rate)) end
local c_tokens = 0
if c_rate > 0 then
  c_tokens = refill(KEYS[6], c_rate)
  if c_tokens < 1 then
    local w = math.max(1, math.ceil((1 - c_tokens) * 1000 / c_rate))
    if w > wait then wait = w end
  end
end
if wait > 0 then return {4, wait} end

local id = redis.call('LPOP', KEYS[1])
redis.call('ZADD', KEYS[3], now + lease_ms, id)
redis.call('PEXPIRE', KEYS[3], lease_ms + 60000)
if camp_max > 0 then
  redis.call('ZADD', KEYS[4], now + lease_ms, id)
  redis.call('PEXPIRE', KEYS[4], lease_ms + 60000)
end
redis.call('HSET', KEYS[5], 'tokens', tostring(g_tokens - 1), 'ts', now)
redis.call('PEXPIRE', KEYS[5], 60000)
if c_rate > 0 then
  redis.call('HSET', KEYS[6], 'tokens', tostring(c_tokens - 1), 'ts', now)
  redis.call('PEXPIRE', KEYS[6], 60000)
end
redis.call('ZADD', KEYS[7], now + tonumber(ARGV[7]), id)
return {1, id}
"""
)
