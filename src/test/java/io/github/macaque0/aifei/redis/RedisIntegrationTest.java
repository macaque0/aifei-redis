package io.github.macaque0.aifei.redis;

import io.github.macaque0.aifei.redis.codec.StringRedisCodec;
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueOptions;
import io.github.macaque0.aifei.redis.queue.RedisReliableQueue;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RedisIntegrationTest {

    private RedisPlugin plugin;
    private String prefix;

    @Before
    public void setUp() {
        Assume.assumeTrue(Boolean.getBoolean("redis.integration"));
        prefix = "aifei-redis-it-" + System.currentTimeMillis();
        RedisConfig config = new RedisConfig()
                .setHost(required("redis.host"))
                .setPort(Integer.getInteger("redis.port", 6379))
                .setUser(blankToNull(System.getProperty("redis.user")))
                .setPassword(blankToNull(System.getProperty("redis.password")))
                .setDatabase(Integer.getInteger("redis.database", 0))
                .setSsl(Boolean.getBoolean("redis.ssl"))
                .setTimeoutMillis(Integer.getInteger("redis.timeoutMillis", 8000))
                .setKeyPrefix(prefix)
                .setQueueMaintainIntervalMillis(100);
        plugin = new RedisPlugin(config);
        plugin.start();
    }

    @After
    public void tearDown() {
        if (plugin != null) {
            cleanup();
            plugin.stop();
        }
    }

    @Test
    public void commonRedisApiWorks() {
        String stringKey = RedisKit.key("common:string");
        assertEquals("OK", RedisKit.set(stringKey, "v1"));
        assertEquals("v1", RedisKit.get(stringKey));
        assertTrue(RedisKit.exists(stringKey));
        assertEquals(Long.valueOf(1), RedisKit.expire(stringKey, 30));
        assertTrue(RedisKit.ttl(stringKey) > 0);
        assertEquals(Long.valueOf(1), RedisKit.pexpire(stringKey, 30000));
        assertTrue(RedisKit.pttl(stringKey) > 0);
        assertEquals("OK", RedisKit.setex(RedisKit.key("common:setex"), 30, "v2"));
        assertEquals("v2", RedisKit.get(RedisKit.key("common:setex")));

        String counterKey = RedisKit.key("common:counter");
        RedisKit.del(counterKey);
        assertEquals(Long.valueOf(1), RedisKit.incr(counterKey));
        assertEquals(Long.valueOf(4), RedisKit.incrBy(counterKey, 3));
        assertEquals(Long.valueOf(3), RedisKit.decr(counterKey));
        assertEquals(Long.valueOf(1), RedisKit.decrBy(counterKey, 2));

        String hashKey = RedisKit.key("common:hash");
        assertEquals(Long.valueOf(1), RedisKit.hset(hashKey, "f1", "a"));
        RedisKit.hset(hashKey, "f2", "b");
        assertEquals("a", RedisKit.hget(hashKey, "f1"));
        assertEquals(2, RedisKit.hgetAll(hashKey).size());
        assertEquals(Long.valueOf(1), RedisKit.hdel(hashKey, "f2"));

        String listKey = RedisKit.key("common:list");
        RedisKit.rpush(listKey, "b", "c");
        RedisKit.lpush(listKey, "a");
        assertEquals(Arrays.asList("a", "b", "c"), RedisKit.lrange(listKey, 0, -1));
        assertEquals("a", RedisKit.lpop(listKey));
        assertEquals("c", RedisKit.rpop(listKey));
        assertEquals(Long.valueOf(1), RedisKit.llen(listKey));

        String setKey = RedisKit.key("common:set");
        assertEquals(Long.valueOf(2), RedisKit.sadd(setKey, "a", "b"));
        assertTrue(RedisKit.smembers(setKey).contains("a"));
        assertEquals(Long.valueOf(1), RedisKit.srem(setKey, "b"));

        String zsetKey = RedisKit.key("common:zset");
        RedisKit.zadd(zsetKey, 1, "a");
        RedisKit.zadd(zsetKey, 2, "b");
        assertEquals(Arrays.asList("a", "b"), new ArrayList<>(RedisKit.zrange(zsetKey, 0, -1)));
        assertTrue(RedisKit.zrangeByScore(zsetKey, 2, 2).contains("b"));
        assertEquals(Long.valueOf(1), RedisKit.zrem(zsetKey, "a"));

        assertEquals("v1", RedisKit.eval("return redis.call('get', KEYS[1])",
                Collections.singletonList(stringKey), Collections.emptyList()));
        assertEquals("PONG", RedisKit.execute(jedis -> jedis.ping()));
        assertTrue(scanContains(prefix + ":common:string"));

        assertTrue(RedisKit.del(stringKey) >= 1);
        assertFalse(RedisKit.exists(stringKey));
    }

    @Test
    public void reliableQueueWorks() {
        RedisReliableQueue<String> queue = RedisQueueKit.reliableQueue("reliable", StringRedisCodec.INSTANCE,
                new RedisQueueOptions().setVisibilityTimeoutMillis(1000).setMaxRetries(2));

        queue.offer("m1", "hello");
        RedisMessage<String> message = queue.reserve("c1", 1000);

        assertNotNull(message);
        assertEquals("m1", message.getId());
        assertEquals("hello", message.getBody());

        queue.ack(message.getId());
        assertEquals(0, queue.reservedSize());
    }

    private void cleanup() {
        RedisKit.execute(jedis -> {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams params = new ScanParams().match(prefix + "*").count(100);
            do {
                ScanResult<String> result = jedis.scan(cursor, params);
                if (!result.getResult().isEmpty()) {
                    jedis.del(result.getResult().toArray(new String[0]));
                }
                cursor = result.getCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            return null;
        });
    }

    private static String required(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private boolean scanContains(String key) {
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams().match(prefix + "*").count(100);
        do {
            ScanResult<String> result = RedisKit.scan(cursor, params);
            if (result.getResult().contains(key)) {
                return true;
            }
            cursor = result.getCursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        return false;
    }
}
