package io.github.macaque0.aifei.redis;

import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueEventKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueEventListener;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueListener;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerMode;
import io.github.macaque0.aifei.redis.queue.RedisReliableQueue;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RedisQueueListenerIntegrationTest {

    private RedisPlugin plugin;
    private String prefix;

    @Before
    public void setUp() {
        Assume.assumeTrue(Boolean.getBoolean("redis.integration"));
        prefix = "aifei-redis-listener-it-" + System.currentTimeMillis();
        RedisConfig config = new RedisConfig()
                .setHost(required("redis.host"))
                .setPort(Integer.getInteger("redis.port", 6379))
                .setUser(blankToNull(System.getProperty("redis.user")))
                .setPassword(blankToNull(System.getProperty("redis.password")))
                .setDatabase(Integer.getInteger("redis.database", 0))
                .setSsl(Boolean.getBoolean("redis.ssl"))
                .setTimeoutMillis(Integer.getInteger("redis.timeoutMillis", 8000))
                .setKeyPrefix(prefix)
                .setQueueMaintainIntervalMillis(50);
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
    public void normalListenerPollsAndInvokesBusinessMethod() throws Exception {
        CapturingQueueEventListener events = new CapturingQueueEventListener();
        RedisQueueEventKit.setListener(events);
        NormalListener listener = new NormalListener();
        RedisQueueListenerKit.register(listener);

        RedisQueueKit.queue("anno-normal", String.class).offer("hello");

        waitUntil(() -> listener.values.contains("hello"), 3000);
        waitUntil(() -> events.contains("success:anno-normal"), 3000);
        assertTrue(events.contains("start:anno-normal"));
    }

    @Test
    public void delayListenerPollsDueMessagesAndInvokesBusinessMethod() throws Exception {
        CapturingQueueEventListener events = new CapturingQueueEventListener();
        RedisQueueEventKit.setListener(events);
        DelayListener listener = new DelayListener();
        RedisQueueListenerKit.register(listener);

        RedisQueueKit.delayQueue("anno-delay", String.class).offer("delayed", 300);

        waitUntil(() -> listener.values.contains("delayed"), 3000);
        waitUntil(() -> events.contains("success:anno-delay"), 3000);
        assertTrue(events.contains("start:anno-delay"));
    }

    @Test
    public void reliableListenerRetriesThenAcks() throws Exception {
        CapturingQueueEventListener events = new CapturingQueueEventListener();
        RedisQueueEventKit.setListener(events);
        ReliableListener listener = new ReliableListener();
        RedisQueueListenerKit.register(listener);

        RedisReliableQueue<String> queue = RedisQueueKit.reliableQueue("anno-reliable", String.class);
        queue.offer("r1", "reliable-job");

        waitUntil(() -> listener.values.contains("reliable-job"), 5000);
        waitUntil(() -> queue.reservedSize() == 0, 3000);

        assertEquals(2, listener.attempts.get());
        assertEquals(0, queue.deadSize());
        assertTrue(events.contains("retry:anno-reliable"));
        assertTrue(events.contains("success:anno-reliable"));
    }

    public static class NormalListener {

        final List<String> values = new CopyOnWriteArrayList<>();

        @RedisQueueListener(value = "anno-normal", pollTimeoutMillis = 50, idleSleepMillis = 5)
        public void handle(String body) {
            values.add(body);
        }
    }

    public static class DelayListener {

        final List<String> values = new CopyOnWriteArrayList<>();

        @RedisQueueListener(value = "anno-delay",
                mode = RedisQueueListenerMode.DELAY,
                pollTimeoutMillis = 50,
                idleSleepMillis = 5)
        public void handle(String body) {
            values.add(body);
        }
    }

    public static class ReliableListener {

        final List<String> values = new CopyOnWriteArrayList<>();
        final AtomicInteger attempts = new AtomicInteger();

        @RedisQueueListener(value = "anno-reliable",
                mode = RedisQueueListenerMode.RELIABLE,
                pollTimeoutMillis = 50,
                idleSleepMillis = 5,
                visibilityTimeoutMillis = 500,
                maxRetries = 2,
                retryDelayMillis = 10)
        public void handle(RedisMessage<String> message) {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("planned retry");
            }
            values.add(message.getBody());
        }
    }

    private static class CapturingQueueEventListener implements RedisQueueEventListener {

        final List<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void onConsumeStart(String queue, RedisMessage<?> message, String consumerId) {
            events.add("start:" + queue);
        }

        @Override
        public void onConsumeSuccess(String queue, RedisMessage<?> message, String consumerId, long elapsedMillis) {
            events.add("success:" + queue);
        }

        @Override
        public void onConsumeFailure(String queue, RedisMessage<?> message, String consumerId,
                                     Throwable error, long elapsedMillis) {
            events.add("failure:" + queue);
        }

        @Override
        public void onRetry(String queue, RedisMessage<?> message, String consumerId, Throwable error,
                            long delayMillis, long elapsedMillis) {
            events.add("retry:" + queue);
        }

        @Override
        public void onDead(String queue, RedisMessage<?> message, String consumerId, Throwable error, long elapsedMillis) {
            events.add("dead:" + queue);
        }

        @Override
        public void onConsumerError(String queue, String consumerId, Throwable error) {
            events.add("error:" + queue);
        }

        boolean contains(String event) {
            return events.contains(event);
        }
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

    private static void waitUntil(Check check, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("condition not met within " + timeoutMillis + " ms");
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

    private interface Check {
        boolean ok() throws Exception;
    }
}
