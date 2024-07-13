-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the done jobs set
-- KEYS[3]: the job data hash

-- ARGV[1]: the job ID
-- ARGV[2]: the current time
-- ARGV[3]: the result of the job

-- Returns: bool

-- Remove the job from this consumer's inflight set
local removed = redis.call("srem", KEYS[1], ARGV[1])
local ns = "::result"
if removed == 1 then
  -- Push the job on to the done jobs set
  redis.call("zadd", KEYS[2], ARGV[2], ARGV[1])
  redis.call("hmset", KEYS[3].. ns, ARGV[1], ARGV[3] )
  return true
end

return false
