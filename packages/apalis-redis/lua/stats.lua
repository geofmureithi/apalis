-- KEYS[1]: the pending jobs set ( aka active job list )
-- KEYS[2]: the consumer set
-- KEYS[3]: the dead jobs set
-- KEYS[4]: the failed jobs set
-- KEYS[5]: the success jobs set

-- Returns: nil

-- Returns the number of jobs in each state

local pending_jobs_set = KEYS[1]
local consumer_set = KEYS[2]
local dead_jobs_set = KEYS[3]
local failed_jobs_set = KEYS[4]
local success_jobs_set = KEYS[5]

local consumers = redis.call("zrangebyscore", consumer_set, 0, "+inf")

local running_count = 0
for _,consumer_inlfight_set in ipairs(consumers) do
  running_count = running_count + redis.call("SCARD", consumer_inlfight_set)
end

local pending_count = redis.call('LLEN', pending_jobs_set)
local dead_count = redis.call('ZCARD', dead_jobs_set)
local failed_count = redis.call('ZCARD', failed_jobs_set)
local success_count = redis.call('ZCARD', success_jobs_set)

return {pending_count, running_count, dead_count, failed_count, success_count}
