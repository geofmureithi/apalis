-- KEYS[1]: the active consumers set

-- ARGV[1]: the current time
-- ARGV[2]: this consumer's inflight set

-- Returns: nil

-- Update the consumer in the active consumer set
redis.call("zadd", KEYS[1], ARGV[1], ARGV[2])
return true
