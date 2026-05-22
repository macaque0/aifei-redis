package io.github.macaque0.aifei.redis.queue;

import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;

public class RedisQueueEventKitTest {

    @After
    public void tearDown() {
        RedisQueueEventKit.clearListener();
    }

    @Test
    public void listenerFailureDoesNotEscape() {
        PrintStream oldErr = System.err;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer));
        try {
            RedisQueueEventKit.setListener(new RedisQueueEventListener() {
                @Override
                public void onConsumeStart(String queue, RedisMessage<?> message, String consumerId) {
                    throw new IllegalStateException("listener failure");
                }
            });

            RedisQueueEventKit.consumeStart("jobs", new RedisMessage<String>().setId("1").setBody("job"), "c1");
        } finally {
            System.setErr(oldErr);
        }

        assertTrue(buffer.toString().contains("Redis queue event listener failed"));
    }

    @Test
    public void clearRestoresNoopListener() {
        RedisQueueEventKit.setListener(new RedisQueueEventListener() {
        });

        RedisQueueEventKit.clearListener();

        assertSame(RedisQueueEventListener.NOOP, RedisQueueEventKit.getListener());
    }
}
