package io.github.macaque0.aifei.redis;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RedisConfigTest {

    @Test
    public void defaultsAreUsable() {
        RedisConfig config = new RedisConfig();
        config.validate();

        assertEquals("127.0.0.1", config.getHost());
        assertEquals(6379, config.getPort());
        assertEquals("aifei", config.getKeyPrefix());
        assertEquals(30000, config.getQueueDefaultVisibilityTimeoutMillis());
        assertEquals(16, config.getQueueDefaultMaxRetries());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInvalidPort() {
        new RedisConfig().setPort(0).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankDeadLetterSuffix() {
        new RedisConfig().setQueueDeadLetterSuffix(" ").validate();
    }
}
