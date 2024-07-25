-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the scheduled jobs set
-- KEYS[3]: the job data hash

-- ARGV[1]: the job ID
-- ARGV[2]: the time at which to retry
-- ARGV[3]: the result of the job

-- Returns: nil

-- Remove the job from this consumer's inflight set
local removed = redis.call("srem", KEYS[1], ARGV[1])

if removed == 1 then
  -- Push the job on to the scheduled set
  redis.call("zadd", KEYS[2], ARGV[2], ARGV[1])

  local job = redis.call('HGET', KEYS[3], ARGV[1])

  -- Reset the job data
  redis.call("hset", KEYS[3], ARGV[1], job)

  -- Save the result of the job
  local ns = "::result"
  redis.call("hmset", KEYS[3].. ns, ARGV[1], ARGV[4] )

end

return removed
