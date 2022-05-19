-- KEYS[1]: the consumer set
-- KEYS[2]: the active job list
-- KEYS[3]: the signal list

-- ARGV[1]: the timestamp before which a consumer is considered expired
-- ARGV[2]: the max number of jobs to process in a given run

-- Returns: 0 if all orphaned jobs have been rescheduled, 1 if there are more to process

-- Find expired consumers
local consumers = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1], "LIMIT", 0, ARGV[2])
redis.replicate_commands()
-- Pull jobs from the consumer's inflight set and reschedule up to limit
local limit = tonumber(ARGV[2])
for _,consumer in ipairs(consumers) do
  local jobs = redis.call("spop", consumer, limit)
  local count = table.getn(jobs)

  -- Push any orphaned jobs on to the message list
  if count > 0 then
    redis.call("rpush", KEYS[2], unpack(jobs))
  end

  -- Delete the consumer if all of its jobs have been rescheduled
  if count < limit then
    redis.call("zrem", KEYS[1], consumer)
  end


  -- Don't keep looping if we can't process any more jobs
  limit = limit - count
  if limit <= 0 then
    break
  end
end

local processed = tonumber(ARGV[2]) - limit

if processed > 0 then
  -- Signal that there are jobs in the queue
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
end

return processed
