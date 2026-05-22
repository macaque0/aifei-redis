package io.github.macaque0.aifei.redis;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisKit {

    private static volatile Redis redis;

    public static void init(Redis redis) {
        if (redis == null) {
            throw new IllegalArgumentException("redis can not be null");
        }
        RedisKit.redis = redis;
    }

    public static Redis getRedis() {
        Redis ret = redis;
        if (ret == null) {
            throw new IllegalStateException("RedisKit has not been initialized. Add RedisPlugin first.");
        }
        return ret;
    }

    public static String key(String key) {
        return getRedis().key(key);
    }

    public static String get(String key) {
        return getRedis().get(key);
    }

    public static String set(String key, String value) {
        return getRedis().set(key, value);
    }

    public static String setex(String key, long seconds, String value) {
        return getRedis().setex(key, seconds, value);
    }

    public static Long del(String... keys) {
        return getRedis().del(keys);
    }

    public static Boolean exists(String key) {
        return getRedis().exists(key);
    }

    public static Long expire(String key, long seconds) {
        return getRedis().expire(key, seconds);
    }

    public static Long pexpire(String key, long milliseconds) {
        return getRedis().pexpire(key, milliseconds);
    }

    public static Long ttl(String key) {
        return getRedis().ttl(key);
    }

    public static Long pttl(String key) {
        return getRedis().pttl(key);
    }

    public static Long incr(String key) {
        return getRedis().incr(key);
    }

    public static Long incrBy(String key, long delta) {
        return getRedis().incrBy(key, delta);
    }

    public static Long decr(String key) {
        return getRedis().decr(key);
    }

    public static Long decrBy(String key, long delta) {
        return getRedis().decrBy(key, delta);
    }

    public static Long hset(String key, String field, String value) {
        return getRedis().hset(key, field, value);
    }

    public static String hget(String key, String field) {
        return getRedis().hget(key, field);
    }

    public static Long hdel(String key, String... fields) {
        return getRedis().hdel(key, fields);
    }

    public static Map<String, String> hgetAll(String key) {
        return getRedis().hgetAll(key);
    }

    public static Long lpush(String key, String... values) {
        return getRedis().lpush(key, values);
    }

    public static Long rpush(String key, String... values) {
        return getRedis().rpush(key, values);
    }

    public static String lpop(String key) {
        return getRedis().lpop(key);
    }

    public static String rpop(String key) {
        return getRedis().rpop(key);
    }

    public static Long llen(String key) {
        return getRedis().llen(key);
    }

    public static List<String> lrange(String key, long start, long stop) {
        return getRedis().lrange(key, start, stop);
    }

    public static Long sadd(String key, String... members) {
        return getRedis().sadd(key, members);
    }

    public static Set<String> smembers(String key) {
        return getRedis().smembers(key);
    }

    public static Long srem(String key, String... members) {
        return getRedis().srem(key, members);
    }

    public static Long zadd(String key, double score, String member) {
        return getRedis().zadd(key, score, member);
    }

    public static Set<String> zrange(String key, long start, long stop) {
        return getRedis().zrange(key, start, stop);
    }

    public static Set<String> zrangeByScore(String key, double min, double max) {
        return getRedis().zrangeByScore(key, min, max);
    }

    public static Long zrem(String key, String... members) {
        return getRedis().zrem(key, members);
    }

    public static ScanResult<String> scan(String cursor, ScanParams params) {
        return getRedis().scan(cursor, params);
    }

    public static Object eval(String script, List<String> keys, List<String> args) {
        return getRedis().eval(script, keys, args);
    }

    public static <T> T execute(RedisExecutor<T> executor) {
        return getRedis().execute(executor);
    }

    public static void clearInit() {
        redis = null;
    }

    static void clearInit(Redis expected) {
        if (redis == expected) {
            redis = null;
        }
    }
}
