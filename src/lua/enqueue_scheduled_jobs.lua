-- KEYS[1]: the scheduled jobs set
-- KEYS[2]: the active job list
-- KEYS[3]: the signal list

-- ARGV[1]: the current timestamp
-- ARGV[2]: the max number of jobs to schedule

-- Returns: nil

-- Get the jobs out of the scheduled set
local job_ids = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1], "LIMIT", 0, ARGV[2])
local count = table.getn(job_ids)

if count > 0 then
  -- Push them on to the active list
  redis.call("rpush", KEYS[2], unpack(job_ids))

  -- Remove the jobs from the scheduled set
  redis.call("zremrangebyrank", KEYS[1], 0, count - 1)

  -- Signal that there are jobs in the queue
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
end

return count
