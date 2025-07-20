-- KEYS[1]: the task data hash
-- KEYS[2]: the active task list
-- KEYS[3]: the signal list
-- KEYS[4]: the metadata prefix (e.g. "task_meta")

-- ARGV: [task_id1, task_data1, task_id2, task_data2, ...]

local newly_enqueued = 0

for i = 1, #ARGV, 4 do
  local task_id = ARGV[i]
  local task_data = ARGV[i + 1]
  local attempts = ARGV[i + 2]
  local max_attempts = ARGV[i + 3]

  if task_id and task_data then
    local set = redis.call("hsetnx", KEYS[1], task_id, task_data)

    if set == 1 then
      redis.call("rpush", KEYS[2], task_id)

      local meta_key = KEYS[4] .. ':' .. task_id
      redis.call("hmset", meta_key,
        "attempts", tonumber(attempts),
        "max_attempts", tonumber(max_attempts),
        "status", "Pending"
      )
        redis.call("publish", "tasks:" .. KEYS[2] .. ':available', task_id)
      newly_enqueued = newly_enqueued + 1
    end
  end
end

if newly_enqueued > 0 then
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
end

return newly_enqueued
