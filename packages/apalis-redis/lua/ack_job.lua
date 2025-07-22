-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the done tasks set
-- KEYS[3]: the dead tasks set
-- KEYS[4]: the task meta hash

-- ARGV[1]: the task ID
-- ARGV[2]: the current time
-- ARGV[3]: the serialized result (Result<T, E>)
-- ARGV[4]: "ok" or "err"
-- ARGV[5]: "the current attempt"

-- Returns: 1 if successfully acknowledged and stored, 0 otherwise
local task_id = ARGV[1]
local ts = ARGV[2]
local result = ARGV[3]
local status = ARGV[4]
local attempt = ARGV[5]


-- Remove from inflight
local removed = redis.call("srem", KEYS[1], task_id)
if removed == 0 then
    return 0
end

local task_meta_key = KEYS[4] .. ':' .. task_id

-- Save result in hash namespace (same as before)
local ns = ":result"
redis.call("hset", KEYS[4] .. ns, task_id, result)

-- Store the current attempt
redis.call("hset", task_meta_key, "attempts", attempt)

-- Route to correct status set
if status == "ok" then
    redis.call("zadd", KEYS[2], ts, task_id)
    redis.call("hset", task_meta_key, "status", "Done")
    
else
    redis.call("zadd", KEYS[3], ts, task_id)
    redis.call("hset", task_meta_key, "status", "Failed")

end

return 1
