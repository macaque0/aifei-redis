package io.github.macaque0.aifei.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

class DefaultRedis implements Redis {

    private final RedisConfig config;
    private final JedisPool pool;

    DefaultRedis(RedisConfig config, JedisPool pool) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        if (pool == null) {
            throw new IllegalArgumentException("pool can not be null");
        }
        this.config = config;
        this.pool = pool;
    }

    @Override
    public String key(String key) {
        key = requireKey(key);
        String prefix = trimToNull(config.getKeyPrefix());
        return prefix == null ? key : prefix + ":" + key;
    }

    @Override
    public String get(String key) {
        return execute(jedis -> jedis.get(requireKey(key)));
    }

    @Override
    public String set(String key, String value) {
        return execute(jedis -> jedis.set(requireKey(key), value));
    }

    @Override
    public String setex(String key, long seconds, String value) {
        if (seconds <= 0) {
            throw new IllegalArgumentException("seconds must be positive");
        }
        return execute(jedis -> jedis.setex(requireKey(key), (int) seconds, value));
    }

    @Override
    public Long del(String... keys) {
        requireKeys(keys);
        return execute(jedis -> jedis.del(keys));
    }

    @Override
    public Boolean exists(String key) {
        return execute(jedis -> jedis.exists(requireKey(key)));
    }

    @Override
    public Long expire(String key, long seconds) {
        if (seconds < 0) {
            throw new IllegalArgumentException("seconds can not be negative");
        }
        return execute(jedis -> jedis.expire(requireKey(key), (int) seconds));
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("milliseconds can not be negative");
        }
        return execute(jedis -> jedis.pexpire(requireKey(key), milliseconds));
    }

    @Override
    public Long ttl(String key) {
        return execute(jedis -> jedis.ttl(requireKey(key)));
    }

    @Override
    public Long pttl(String key) {
        return execute(jedis -> jedis.pttl(requireKey(key)));
    }

    @Override
    public Long incr(String key) {
        return execute(jedis -> jedis.incr(requireKey(key)));
    }

    @Override
    public Long incrBy(String key, long delta) {
        return execute(jedis -> jedis.incrBy(requireKey(key), delta));
    }

    @Override
    public Long decr(String key) {
        return execute(jedis -> jedis.decr(requireKey(key)));
    }

    @Override
    public Long decrBy(String key, long delta) {
        return execute(jedis -> jedis.decrBy(requireKey(key), delta));
    }

    @Override
    public Long hset(String key, String field, String value) {
        return execute(jedis -> jedis.hset(requireKey(key), requireField(field), value));
    }

    @Override
    public String hget(String key, String field) {
        return execute(jedis -> jedis.hget(requireKey(key), requireField(field)));
    }

    @Override
    public Long hdel(String key, String... fields) {
        requireFields(fields);
        return execute(jedis -> jedis.hdel(requireKey(key), fields));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return execute(jedis -> jedis.hgetAll(requireKey(key)));
    }

    @Override
    public Long lpush(String key, String... values) {
        requireValues(values);
        return execute(jedis -> jedis.lpush(requireKey(key), values));
    }

    @Override
    public Long rpush(String key, String... values) {
        requireValues(values);
        return execute(jedis -> jedis.rpush(requireKey(key), values));
    }

    @Override
    public String lpop(String key) {
        return execute(jedis -> jedis.lpop(requireKey(key)));
    }

    @Override
    public String rpop(String key) {
        return execute(jedis -> jedis.rpop(requireKey(key)));
    }

    @Override
    public Long llen(String key) {
        return execute(jedis -> jedis.llen(requireKey(key)));
    }

    @Override
    public List<String> lrange(String key, long start, long stop) {
        return execute(jedis -> jedis.lrange(requireKey(key), start, stop));
    }

    @Override
    public Long sadd(String key, String... members) {
        requireValues(members);
        return execute(jedis -> jedis.sadd(requireKey(key), members));
    }

    @Override
    public Set<String> smembers(String key) {
        return execute(jedis -> jedis.smembers(requireKey(key)));
    }

    @Override
    public Long srem(String key, String... members) {
        requireValues(members);
        return execute(jedis -> jedis.srem(requireKey(key), members));
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return execute(jedis -> jedis.zadd(requireKey(key), score, member));
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        return execute(jedis -> jedis.zrange(requireKey(key), start, stop));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return execute(jedis -> jedis.zrangeByScore(requireKey(key), min, max));
    }

    @Override
    public Long zrem(String key, String... members) {
        requireValues(members);
        return execute(jedis -> jedis.zrem(requireKey(key), members));
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        return execute(jedis -> jedis.scan(cursor, params));
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        if (script == null || script.trim().isEmpty()) {
            throw new IllegalArgumentException("script can not be blank");
        }
        return execute(jedis -> jedis.eval(script, keys, args));
    }

    @Override
    public <T> T execute(RedisExecutor<T> executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor can not be null");
        }
        try (Jedis jedis = pool.getResource()) {
            return executor.execute(jedis);
        } catch (RuntimeException e) {
            if (e instanceof RedisException) {
                throw e;
            }
            throw new RedisException("Redis command failed", e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    static String requireKey(String key) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("key can not be blank");
        }
        return key;
    }

    static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static String requireField(String field) {
        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("field can not be blank");
        }
        return field;
    }

    private static void requireKeys(String[] keys) {
        if (keys == null || keys.length == 0) {
            throw new IllegalArgumentException("keys can not be empty");
        }
        for (String key : keys) {
            requireKey(key);
        }
    }

    private static void requireFields(String[] fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("fields can not be empty");
        }
        for (String field : fields) {
            requireField(field);
        }
    }

    private static void requireValues(String[] values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("values can not be empty");
        }
        for (String value : values) {
            if (value == null) {
                throw new IllegalArgumentException("value can not be null");
            }
        }
    }
}
