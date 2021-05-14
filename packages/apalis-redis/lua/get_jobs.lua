-- KEYS[1]: the active consumers set
-- KEYS[2]: the active job list
-- KEYS[3]: this consumer's inflight set
-- KEYS[4]: the job data hash
-- KEYS[5]: the signal list

-- ARGV[1]: the max number of jobs to get
-- ARGV[2]: this consumer's inflight set

-- Returns: the jobs

-- Ensure the consumer is registered
local registered = redis.call("zscore", KEYS[1], ARGV[2])
if not registered then
  error("consumer not registered")
end

-- Get the jobs out of the active job list
local job_ids = redis.call("lrange", KEYS[2], 0, ARGV[1] - 1)
local count = table.getn(job_ids)
local results = {}

if count > 0 then
  -- Add the jobs to the active set
  redis.call("sadd", KEYS[3], unpack(job_ids))

  -- Remove the jobs from the active job list
  redis.call("ltrim", KEYS[2], count, -1)

  -- Return the job data
  results = redis.call("hmget", KEYS[4], unpack(job_ids))
end

-- Signal to the other consumers to wait
if count < tonumber(ARGV[1]) then
  redis.call("del", KEYS[5])
end

return results
