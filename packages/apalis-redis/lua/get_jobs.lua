-- KEYS[1]: the active consumers set
-- KEYS[2]: the active task list
-- KEYS[3]: this consumer's inflight set
-- KEYS[4]: the task data hash
-- KEYS[5]: the signal list
-- KEYS[6]: the task meta hash
-- ARGV[1]: the max number of tasks to get
-- ARGV[2]: this consumer's inflight set
-- Returns: the tasks
-- Ensure the consumer is registered
local registered = redis.call("zscore", KEYS[1], ARGV[2])
if not registered then
    error("consumer not registered")
end

-- Get the tasks out of the active task list
local task_ids = redis.call("lrange", KEYS[2], 0, ARGV[1] - 1)
local count = table.getn(task_ids)
local results = {}
local meta = {}

if count > 0 then
    -- Add the tasks to this consumer's inflight set
    redis.call("sadd", KEYS[3], unpack(task_ids))

    -- Remove the tasks from the active task list
    redis.call("ltrim", KEYS[2], count, -1)

    -- Return the task data
    results = redis.call("hmget", KEYS[4], unpack(task_ids))

    for i, task_id in ipairs(task_ids) do
        local meta_key = KEYS[6] .. ':' .. task_id
        local fields = redis.call("hmget", meta_key, "attempts", "max_attempts", "status")
        table.insert(fields, 1, task_id)
        table.insert(meta, fields)
    end

end

-- Signal to the other consumers to wait
if count < tonumber(ARGV[1]) then
    redis.call("del", KEYS[5])
end

return {results, meta}
