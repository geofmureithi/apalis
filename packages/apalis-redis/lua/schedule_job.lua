-- KEYS[1]: the job data hash
-- KEYS[2]: the scheduled set

-- ARGV[1]: the job ID
-- ARGV[2]: the serialized job data
-- ARGV[3]: the time to schedule the job

-- Returns: 1 if the job was newly scheduled, 0 if it already exists

-- Set job data in hash
local set = redis.call("hsetnx", KEYS[1], ARGV[1], ARGV[2])
redis.call("zadd", KEYS[2], ARGV[3], ARGV[1])
return set
