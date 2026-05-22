package io.github.macaque0.aifei.redis;

import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerKit;
import org.junit.Test;

import static org.junit.Assert.fail;

public class RedisPluginLifecycleTest {

    @Test
    public void startFailureRollsBackGlobalState() {
        RedisConfig config = new RedisConfig()
                .setHost("127.0.0.1")
                .setPort(1)
                .setTimeoutMillis(50)
                .setQueueMaintainerEnabled(false)
                .setQueueListenerPackages("io.github.macaque0.aifei.redis.badlistener");

        try {
            new RedisPlugin(config).start();
            fail("invalid listener method should fail plugin startup");
        } catch (IllegalArgumentException expected) {
            assertUninitialized();
        }
    }

    private static void assertUninitialized() {
        try {
            RedisKit.getRedis();
            fail("RedisKit should be cleared");
        } catch (IllegalStateException expected) {
        }

        try {
            RedisQueueKit.getQueueFactory();
            fail("RedisQueueKit should be cleared");
        } catch (IllegalStateException expected) {
        }

        try {
            RedisQueueListenerKit.getContainer();
            fail("RedisQueueListenerKit should be cleared");
        } catch (IllegalStateException expected) {
        }
    }
}
