-- KEYS[1]: the job data hash
-- KEYS[2]: the active job list
-- KEYS[3]: the signal list

-- ARGV: [job_id1, job_data1, job_id2, job_data2, ..., job_idN, job_dataN]

-- Returns: number of jobs newly enqueued
local newly_enqueued = 0

for i = 1, #ARGV, 2 do
  local job_id = ARGV[i]
  local job_data = ARGV[i + 1]

  local set = redis.call("hsetnx", KEYS[1], job_id, job_data)

  if set == 1 then
    redis.call("rpush", KEYS[2], job_id)
    newly_enqueued = newly_enqueued + 1
  end
end

if newly_enqueued > 0 then
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
end

return newly_enqueued
