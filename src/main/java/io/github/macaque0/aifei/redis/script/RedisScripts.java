package io.github.macaque0.aifei.redis.script;

public final class RedisScripts {

    private RedisScripts() {
    }

    public static final String OFFER_READY =
            "if redis.call('hexists', KEYS[2], ARGV[1]) == 1 then return 0 end;" +
            "redis.call('hset', KEYS[2], ARGV[1], ARGV[2]);" +
            "redis.call('hset', KEYS[3], ARGV[1], ARGV[3]);" +
            "redis.call('rpush', KEYS[1], ARGV[1]);" +
            "return 1;";

    public static final String OFFER_READY_LIMITED =
            "local max_length = tonumber(ARGV[4]) or 0;" +
            "if redis.call('hexists', KEYS[3], ARGV[1]) == 1 then return 0 end;" +
            "if max_length > 0 and redis.call('llen', KEYS[1]) >= max_length then " +
            "if ARGV[5] == 'DROP_NEWEST' then return 0 end;" +
            "if ARGV[5] == 'REJECT' then return -1 end;" +
            "while redis.call('llen', KEYS[1]) >= max_length do " +
            "local old = redis.call('lpop', KEYS[1]);" +
            "if not old then break end;" +
            "redis.call('zrem', KEYS[2], old);" +
            "redis.call('hdel', KEYS[3], old);" +
            "redis.call('hdel', KEYS[4], old);" +
            "redis.call('hdel', KEYS[5], old);" +
            "redis.call('zrem', KEYS[6], old);" +
            "redis.call('zrem', KEYS[7], old);" +
            "end;" +
            "end;" +
            "redis.call('hset', KEYS[3], ARGV[1], ARGV[2]);" +
            "redis.call('hset', KEYS[4], ARGV[1], ARGV[3]);" +
            "redis.call('rpush', KEYS[1], ARGV[1]);" +
            "return 1;";

    public static final String OFFER_READY_BATCH =
            "local max_length = tonumber(ARGV[1]) or 0;" +
            "local policy = ARGV[2];" +
            "local inserted = 0;" +
            "local function cleanup(id) " +
            "redis.call('zrem', KEYS[2], id);" +
            "redis.call('hdel', KEYS[3], id);" +
            "redis.call('hdel', KEYS[4], id);" +
            "redis.call('hdel', KEYS[5], id);" +
            "redis.call('zrem', KEYS[6], id);" +
            "redis.call('zrem', KEYS[7], id);" +
            "end;" +
            "local function add(id, body, meta) " +
            "redis.call('hset', KEYS[3], id, body);" +
            "redis.call('hset', KEYS[4], id, meta);" +
            "redis.call('rpush', KEYS[1], id);" +
            "inserted = inserted + 1;" +
            "end;" +
            "local i = 3;" +
            "while i <= #ARGV do " +
            "local id = ARGV[i]; local body = ARGV[i + 1]; local meta = ARGV[i + 2];" +
            "i = i + 3;" +
            "if redis.call('hexists', KEYS[3], id) == 0 then " +
            "if max_length > 0 and redis.call('llen', KEYS[1]) >= max_length then " +
            "if policy == 'DROP_NEWEST' then " +
            "inserted = inserted;" +
            "elseif policy == 'REJECT' then " +
            "return -1;" +
            "else " +
            "while redis.call('llen', KEYS[1]) >= max_length do " +
            "local old = redis.call('lpop', KEYS[1]);" +
            "if not old then break end;" +
            "cleanup(old);" +
            "end;" +
            "add(id, body, meta);" +
            "end;" +
            "else " +
            "add(id, body, meta);" +
            "end;" +
            "end;" +
            "end;" +
            "return inserted;";

    public static final String OFFER_DELAY =
            "if redis.call('hexists', KEYS[3], ARGV[1]) == 1 then return 0 end;" +
            "redis.call('hset', KEYS[3], ARGV[1], ARGV[2]);" +
            "redis.call('hset', KEYS[4], ARGV[1], ARGV[4]);" +
            "redis.call('zadd', KEYS[1], ARGV[3], ARGV[1]);" +
            "return 1;";

    public static final String PROMOTE_DELAY =
            "if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then " +
            "redis.call('rpush', KEYS[2], ARGV[1]); return 1; end;" +
            "return 0;";

    public static final String POLL_READY =
            "local id = redis.call('lpop', KEYS[1]);" +
            "if not id then return nil end;" +
            "local body = redis.call('hget', KEYS[2], id);" +
            "local meta = redis.call('hget', KEYS[3], id);" +
            "redis.call('hdel', KEYS[2], id);" +
            "redis.call('hdel', KEYS[3], id);" +
            "if not body then return nil end;" +
            "return {id, body, meta or ''};";

    public static final String UNLOCK =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "return redis.call('del', KEYS[1]); end;" +
            "return 0;";

    public static final String RENEW_LOCK =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "return redis.call('pexpire', KEYS[1], ARGV[2]); end;" +
            "return 0;";

    public static final String RESERVE =
            "local max_scan = tonumber(ARGV[3]);" +
            "local now = tonumber(ARGV[4]);" +
            "local ttl = tonumber(ARGV[5]);" +
            "for i = 1, max_scan do " +
            "local id = redis.call('lpop', KEYS[1]);" +
            "if not id then return nil end;" +
            "local body = redis.call('hget', KEYS[3], id);" +
            "if body then " +
            "local oldmeta = redis.call('hget', KEYS[4], id) or '';" +
            "local created = string.match(oldmeta, '^([^|]*)') or '';" +
            "local available = string.match(oldmeta, '^[^|]*|([^|]*)') or '';" +
            "local created_num = tonumber(created) or 0;" +
            "if ttl > 0 and created_num > 0 and now - created_num >= ttl then " +
            "redis.call('zadd', KEYS[6], now, id);" +
            "redis.call('hset', KEYS[4], id, created .. '|' .. available .. '|0|0|expired');" +
            "else " +
            "local attempts = redis.call('hincrby', KEYS[5], id, 1);" +
            "redis.call('zadd', KEYS[2], ARGV[1], id);" +
            "local newmeta = created .. '|' .. available .. '|' .. ARGV[1] .. '|' .. attempts .. '|' .. ARGV[2];" +
            "redis.call('hset', KEYS[4], id, newmeta);" +
            "return {id, body, attempts, newmeta};" +
            "end;" +
            "else " +
            "redis.call('hdel', KEYS[4], id);" +
            "redis.call('hdel', KEYS[5], id);" +
            "end;" +
            "end;" +
            "return nil;";

    public static final String RESERVE_BATCH =
            "local reserved_until = ARGV[1];" +
            "local consumer_id = ARGV[2];" +
            "local max_scan = tonumber(ARGV[3]) or 1;" +
            "local now = tonumber(ARGV[4]) or 0;" +
            "local ttl = tonumber(ARGV[5]) or 0;" +
            "local count = tonumber(ARGV[6]) or 1;" +
            "local ret = {};" +
            "local collected = 0;" +
            "local scanned = 0;" +
            "while collected < count and scanned < max_scan do " +
            "scanned = scanned + 1;" +
            "local id = redis.call('lpop', KEYS[1]);" +
            "if not id then return ret end;" +
            "local body = redis.call('hget', KEYS[3], id);" +
            "if body then " +
            "local oldmeta = redis.call('hget', KEYS[4], id) or '';" +
            "local created = string.match(oldmeta, '^([^|]*)') or '';" +
            "local available = string.match(oldmeta, '^[^|]*|([^|]*)') or '';" +
            "local created_num = tonumber(created) or 0;" +
            "if ttl > 0 and created_num > 0 and now - created_num >= ttl then " +
            "redis.call('zadd', KEYS[6], now, id);" +
            "redis.call('hset', KEYS[4], id, created .. '|' .. available .. '|0|0|expired');" +
            "else " +
            "local attempts = redis.call('hincrby', KEYS[5], id, 1);" +
            "redis.call('zadd', KEYS[2], reserved_until, id);" +
            "local newmeta = created .. '|' .. available .. '|' .. reserved_until .. '|' .. attempts .. '|' .. consumer_id;" +
            "redis.call('hset', KEYS[4], id, newmeta);" +
            "table.insert(ret, id);" +
            "table.insert(ret, body);" +
            "table.insert(ret, attempts);" +
            "table.insert(ret, newmeta);" +
            "collected = collected + 1;" +
            "end;" +
            "else " +
            "redis.call('hdel', KEYS[4], id);" +
            "redis.call('hdel', KEYS[5], id);" +
            "end;" +
            "end;" +
            "return ret;";

    public static final String ACK =
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zrem', KEYS[6], ARGV[1]);" +
            "redis.call('hdel', KEYS[3], ARGV[1]);" +
            "redis.call('hdel', KEYS[4], ARGV[1]);" +
            "redis.call('hdel', KEYS[5], ARGV[1]);" +
            "return 1;";

    public static final String ACK_BATCH =
            "local count = 0;" +
            "for _, id in ipairs(ARGV) do " +
            "redis.call('zrem', KEYS[2], id);" +
            "redis.call('zrem', KEYS[6], id);" +
            "redis.call('hdel', KEYS[3], id);" +
            "redis.call('hdel', KEYS[4], id);" +
            "redis.call('hdel', KEYS[5], id);" +
            "count = count + 1;" +
            "end;" +
            "return count;";

    public static final String RETRY_NOW =
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('rpush', KEYS[1], ARGV[1]);" +
            "return 1; end;" +
            "return 0;";

    public static final String RETRY_LATER =
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zadd', KEYS[6], ARGV[2], ARGV[1]);" +
            "return 1; end;" +
            "return 0;";

    public static final String DEAD =
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "local oldmeta = redis.call('hget', KEYS[4], ARGV[1]) or '';" +
            "local created = string.match(oldmeta, '^([^|]*)') or '';" +
            "local available = string.match(oldmeta, '^[^|]*|([^|]*)') or '';" +
            "if not tonumber(created) then created = ARGV[2]; end;" +
            "if not tonumber(available) then available = created; end;" +
            "local attempts = redis.call('hget', KEYS[5], ARGV[1]) or '0';" +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zrem', KEYS[6], ARGV[1]);" +
            "redis.call('zadd', KEYS[7], ARGV[2], ARGV[1]);" +
            "redis.call('hset', KEYS[4], ARGV[1], created .. '|' .. available .. '|0|' .. attempts .. '|' .. ARGV[3]);" +
            "return 1; end;" +
            "return 0;";

    public static final String REPLAY_DEAD =
            "if redis.call('zrem', KEYS[7], ARGV[1]) == 1 then " +
            "local now = ARGV[2] or '0';" +
            "local oldmeta = redis.call('hget', KEYS[4], ARGV[1]) or '';" +
            "local created = string.match(oldmeta, '^([^|]*)') or '';" +
            "local available = string.match(oldmeta, '^[^|]*|([^|]*)') or '';" +
            "if not tonumber(created) then created = now; end;" +
            "if not tonumber(available) then available = created; end;" +
            "redis.call('hdel', KEYS[5], ARGV[1]);" +
            "redis.call('hset', KEYS[4], ARGV[1], created .. '|' .. available .. '|0|0|replay');" +
            "redis.call('rpush', KEYS[1], ARGV[1]); return 1; end;" +
            "return 0;";

    public static final String PRIORITY_OFFER =
            "local max_length = tonumber(ARGV[6]) or 0;" +
            "if redis.call('hexists', KEYS[3], ARGV[1]) == 1 then return 0 end;" +
            "if max_length > 0 and redis.call('zcard', KEYS[1]) >= max_length then " +
            "if ARGV[7] == 'DROP_NEWEST' then return 0 end;" +
            "if ARGV[7] == 'REJECT' then return -1 end;" +
            "local oldest = redis.call('zrevrange', KEYS[1], 0, 0);" +
            "for _, old in ipairs(oldest) do " +
            "redis.call('zrem', KEYS[1], old);" +
            "redis.call('zrem', KEYS[2], old);" +
            "redis.call('hdel', KEYS[3], old);" +
            "redis.call('hdel', KEYS[4], old);" +
            "redis.call('hdel', KEYS[5], old);" +
            "redis.call('zrem', KEYS[6], old);" +
            "redis.call('zrem', KEYS[7], old);" +
            "redis.call('hdel', KEYS[8], old);" +
            "end;" +
            "end;" +
            "redis.call('hset', KEYS[3], ARGV[1], ARGV[2]);" +
            "redis.call('hset', KEYS[4], ARGV[1], ARGV[3]);" +
            "redis.call('hset', KEYS[8], ARGV[1], ARGV[5]);" +
            "redis.call('zadd', KEYS[1], ARGV[4], ARGV[1]);" +
            "return 1;";

    public static final String PRIORITY_RESERVE =
            "local max_scan = tonumber(ARGV[3]) or 1;" +
            "local now = tonumber(ARGV[4]) or 0;" +
            "local ttl = tonumber(ARGV[5]) or 0;" +
            "for i = 1, max_scan do " +
            "local ids = redis.call('zrange', KEYS[1], 0, 0);" +
            "if #ids == 0 then return nil end;" +
            "local id = ids[1];" +
            "if redis.call('zrem', KEYS[1], id) == 1 then " +
            "local body = redis.call('hget', KEYS[3], id);" +
            "if body then " +
            "local oldmeta = redis.call('hget', KEYS[4], id) or '';" +
            "local created = string.match(oldmeta, '^([^|]*)') or '';" +
            "local available = string.match(oldmeta, '^[^|]*|([^|]*)') or '';" +
            "local created_num = tonumber(created) or 0;" +
            "if ttl > 0 and created_num > 0 and now - created_num >= ttl then " +
            "redis.call('zadd', KEYS[7], now, id);" +
            "redis.call('hdel', KEYS[8], id);" +
            "redis.call('hset', KEYS[4], id, created .. '|' .. available .. '|0|0|expired');" +
            "else " +
            "local attempts = redis.call('hincrby', KEYS[5], id, 1);" +
            "redis.call('zadd', KEYS[2], ARGV[1], id);" +
            "local newmeta = created .. '|' .. available .. '|' .. ARGV[1] .. '|' .. attempts .. '|' .. ARGV[2];" +
            "redis.call('hset', KEYS[4], id, newmeta);" +
            "return {id, body, attempts, newmeta};" +
            "end;" +
            "else " +
            "redis.call('hdel', KEYS[4], id);" +
            "redis.call('hdel', KEYS[5], id);" +
            "redis.call('hdel', KEYS[8], id);" +
            "end;" +
            "end;" +
            "end;" +
            "return nil;";

    public static final String PRIORITY_ACK =
            "redis.call('zrem', KEYS[1], ARGV[1]);" +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('hdel', KEYS[3], ARGV[1]);" +
            "redis.call('hdel', KEYS[4], ARGV[1]);" +
            "redis.call('hdel', KEYS[5], ARGV[1]);" +
            "redis.call('zrem', KEYS[6], ARGV[1]);" +
            "redis.call('zrem', KEYS[7], ARGV[1]);" +
            "redis.call('hdel', KEYS[8], ARGV[1]);" +
            "return 1;";

    public static final String PRIORITY_RETRY_NOW =
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "local factor = tonumber(ARGV[3]);" +
            "local priority = tonumber(redis.call('hget', KEYS[8], ARGV[1]) or '0') or 0;" +
            "local sequence = tonumber(ARGV[2]) % factor;" +
            "local score = -1 * priority * factor + sequence;" +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zrem', KEYS[6], ARGV[1]);" +
            "redis.call('zadd', KEYS[1], score, ARGV[1]);" +
            "return 1; end;" +
            "return 0;";

    public static final String PRIORITY_RETRY_LATER =
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "redis.call('zrem', KEYS[1], ARGV[1]);" +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zadd', KEYS[6], ARGV[2], ARGV[1]);" +
            "return 1; end;" +
            "return 0;";

    public static final String PRIORITY_DEAD =
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "local oldmeta = redis.call('hget', KEYS[4], ARGV[1]) or '';" +
            "local created = string.match(oldmeta, '^([^|]*)') or '';" +
            "local available = string.match(oldmeta, '^[^|]*|([^|]*)') or '';" +
            "if not tonumber(created) then created = ARGV[2]; end;" +
            "if not tonumber(available) then available = created; end;" +
            "local attempts = redis.call('hget', KEYS[5], ARGV[1]) or '0';" +
            "redis.call('zrem', KEYS[1], ARGV[1]);" +
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zrem', KEYS[6], ARGV[1]);" +
            "redis.call('zadd', KEYS[7], ARGV[2], ARGV[1]);" +
            "redis.call('hdel', KEYS[8], ARGV[1]);" +
            "redis.call('hset', KEYS[4], ARGV[1], created .. '|' .. available .. '|0|' .. attempts .. '|' .. ARGV[3]);" +
            "return 1; end;" +
            "return 0;";

    public static final String PRIORITY_PROMOTE_DELAY =
            "if redis.call('zrem', KEYS[6], ARGV[1]) == 1 then " +
            "if redis.call('hget', KEYS[3], ARGV[1]) then " +
            "local factor = tonumber(ARGV[3]);" +
            "local priority = tonumber(redis.call('hget', KEYS[8], ARGV[1]) or '0') or 0;" +
            "local sequence = tonumber(ARGV[2]) % factor;" +
            "local score = -1 * priority * factor + sequence;" +
            "redis.call('zadd', KEYS[1], score, ARGV[1]);" +
            "return 1; else " +
            "redis.call('hdel', KEYS[4], ARGV[1]);" +
            "redis.call('hdel', KEYS[5], ARGV[1]);" +
            "redis.call('hdel', KEYS[8], ARGV[1]);" +
            "return 0; end;" +
            "end;" +
            "return 0;";
}
