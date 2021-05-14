-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the dead jobs set
-- KEYS[3]: the job data hash

-- ARGV[1]: the job ID
-- ARGV[2]: the current time
-- ARGV[3]: the serialized job data

-- Returns: nil

-- Remove the job from this consumer's inflight set
local removed = redis.call("srem", KEYS[1], ARGV[1])

if removed == 1 then
  -- Push the job on to the dead jobs set
  redis.call("zadd", KEYS[2], ARGV[2], ARGV[1])

  -- Reset the job data
  redis.call("hset", KEYS[3], ARGV[1], ARGV[3])

  return 1
end

return 0
