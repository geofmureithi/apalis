-- Lua script to clean up data in Redis

-- Define the keys
local done_list_key = KEYS[1]
local data_hash = KEYS[2]

-- Iterate through done_list
local done_list_ids = redis.call('ZRANGE', done_list_key, 0, -1)

-- Initialize a variable to count the number of removed items
local removed_items_count = 0

for _, id in ipairs(done_list_ids) do

    local is_member = redis.call('HEXISTS', data_hash, id)
    if is_member == 1 then
        -- Remove entry from data_hash
        redis.call('HDEL', data_hash, id)
        removed_items_count = removed_items_count + 1
    end
end

-- Clean the done_list
redis.call('DEL', done_list_key)

return removed_items_count
