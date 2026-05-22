package io.github.macaque0.aifei.redis;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Redis extends AutoCloseable {

    String key(String key);

    String get(String key);

    String set(String key, String value);

    String setex(String key, long seconds, String value);

    Long del(String... keys);

    Boolean exists(String key);

    Long expire(String key, long seconds);

    Long pexpire(String key, long milliseconds);

    Long ttl(String key);

    Long pttl(String key);

    Long incr(String key);

    Long incrBy(String key, long delta);

    Long decr(String key);

    Long decrBy(String key, long delta);

    Long hset(String key, String field, String value);

    String hget(String key, String field);

    Long hdel(String key, String... fields);

    Map<String, String> hgetAll(String key);

    Long lpush(String key, String... values);

    Long rpush(String key, String... values);

    String lpop(String key);

    String rpop(String key);

    Long llen(String key);

    List<String> lrange(String key, long start, long stop);

    Long sadd(String key, String... members);

    Set<String> smembers(String key);

    Long srem(String key, String... members);

    Long zadd(String key, double score, String member);

    Set<String> zrange(String key, long start, long stop);

    Set<String> zrangeByScore(String key, double min, double max);

    Long zrem(String key, String... members);

    ScanResult<String> scan(String cursor, ScanParams params);

    Object eval(String script, List<String> keys, List<String> args);

    <T> T execute(RedisExecutor<T> executor);

    @Override
    void close();
}
