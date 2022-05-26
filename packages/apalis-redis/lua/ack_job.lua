-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the done jobs set

-- ARGV[1]: the job ID
-- ARGV[2]: the current time

-- Returns: nil

-- Remove the job from this consumer's inflight set
local removed = redis.call("srem", KEYS[1], ARGV[1])

if removed == 1 then
  -- Delete the job data from the job hash
  -- redis.call("hdel", KEYS[2], ARGV[1])

  -- Push the job on to the done jobs set
  redis.call("zadd", KEYS[2], ARGV[2], ARGV[1])
  return true
end

return false
