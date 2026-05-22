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

    public static final String ACK =
            "redis.call('zrem', KEYS[2], ARGV[1]);" +
            "redis.call('zrem', KEYS[6], ARGV[1]);" +
            "redis.call('hdel', KEYS[3], ARGV[1]);" +
            "redis.call('hdel', KEYS[4], ARGV[1]);" +
            "redis.call('hdel', KEYS[5], ARGV[1]);" +
            "return 1;";

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
}
