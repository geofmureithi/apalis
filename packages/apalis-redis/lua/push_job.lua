-- KEYS[1]: the task data hash
-- KEYS[2]: the active task list
-- KEYS[3]: the signal list
-- KEYS[4]: the task metadata prefix (used to build task:meta:<task_id>)

-- ARGV[1]: the task ID
-- ARGV[2]: the serialized task data
-- ARGV[3]: max_attempts (e.g. 3)

-- Returns: 1 if the task was newly enqueued, 0 if it already exists

-- Set task data if not exists
local set = redis.call("hsetnx", KEYS[1], ARGV[1], ARGV[2])

if set == 1 then
  -- Metadata hash key
  local meta_key = KEYS[4] .. ':' .. ARGV[1]

  -- Write flat metadata
  redis.call("hmset", meta_key,
    "attempts", 0,
    "max_attempts", tonumber(ARGV[3]),
    "status", "Pending"
  )

  -- Push onto active list
  redis.call("rpush", KEYS[2], ARGV[1])

  -- Signal workers
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
  redis.call("publish", "tasks:" .. KEYS[2] .. ':available', ARGV[1])

end

return set
