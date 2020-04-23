-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the job data hash

-- ARGV[1]: the job ID

-- Returns: nil

-- Remove the job from this consumer's inflight set
local removed = redis.call("srem", KEYS[1], ARGV[1])

if removed == 1 then
  -- Delete the job data from the job hash
  redis.call("hdel", KEYS[2], ARGV[1])
  return true
end

return false
