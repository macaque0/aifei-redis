package io.github.macaque0.aifei.redis;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RedisVersionIntegrationTest {

    private RedisPlugin plugin;

    @Before
    public void setUp() {
        Assume.assumeTrue(Boolean.getBoolean("redis.integration"));
        RedisConfig config = new RedisConfig()
                .setHost(required("redis.host"))
                .setPort(Integer.getInteger("redis.port", 6379))
                .setUser(blankToNull(System.getProperty("redis.user")))
                .setPassword(blankToNull(System.getProperty("redis.password")))
                .setDatabase(Integer.getInteger("redis.database", 0))
                .setSsl(Boolean.getBoolean("redis.ssl"))
                .setTimeoutMillis(Integer.getInteger("redis.timeoutMillis", 8000))
                .setKeyPrefix("aifei-redis-version-it");
        plugin = new RedisPlugin(config);
        plugin.start();
    }

    @After
    public void tearDown() {
        if (plugin != null) {
            plugin.stop();
        }
    }

    @Test
    public void redisVersionIsAtLeastFive() {
        String info = RedisKit.execute(jedis -> jedis.info("server"));
        String version = find(info, "redis_version:");
        System.out.println("redis.version=" + version);
        assertTrue("Redis 5.0+ is required, actual version: " + version, atLeast(version, 5, 0));
    }

    private static boolean atLeast(String version, int major, int minor) {
        String[] parts = version.split("\\.");
        int actualMajor = parts.length > 0 ? parse(parts[0]) : 0;
        int actualMinor = parts.length > 1 ? parse(parts[1]) : 0;
        return actualMajor > major || (actualMajor == major && actualMinor >= minor);
    }

    private static int parse(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return 0;
        }
    }

    private static String find(String info, String prefix) {
        String[] lines = info.split("\\r?\\n");
        for (String line : lines) {
            if (line.startsWith(prefix)) {
                return line.substring(prefix.length()).trim();
            }
        }
        throw new IllegalStateException(prefix + " not found in INFO server");
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
}
