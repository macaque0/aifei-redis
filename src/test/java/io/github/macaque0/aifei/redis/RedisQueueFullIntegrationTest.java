package io.github.macaque0.aifei.redis;

import io.github.macaque0.aifei.redis.codec.StringRedisCodec;
import io.github.macaque0.aifei.redis.queue.FullQueuePolicy;
import io.github.macaque0.aifei.redis.queue.RedisDelayQueue;
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisPriorityQueue;
import io.github.macaque0.aifei.redis.queue.RedisQueue;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueOptions;
import io.github.macaque0.aifei.redis.queue.RedisQueueStats;
import io.github.macaque0.aifei.redis.queue.RedisQueueWorker;
import io.github.macaque0.aifei.redis.queue.RedisReliableQueue;
import io.github.macaque0.aifei.redis.queue.RedisStreamQueue;
import io.github.macaque0.aifei.redis.queue.RetryDelayPolicy;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RedisQueueFullIntegrationTest {

    private RedisPlugin plugin;
    private String prefix;

    @Before
    public void setUp() {
        Assume.assumeTrue(Boolean.getBoolean("redis.integration"));
        prefix = "aifei-redis-full-it-" + System.currentTimeMillis();
        RedisConfig config = new RedisConfig()
                .setHost(required("redis.host"))
                .setPort(Integer.getInteger("redis.port", 6379))
                .setUser(blankToNull(System.getProperty("redis.user")))
                .setPassword(blankToNull(System.getProperty("redis.password")))
                .setDatabase(Integer.getInteger("redis.database", 0))
                .setSsl(Boolean.getBoolean("redis.ssl"))
                .setTimeoutMillis(Integer.getInteger("redis.timeoutMillis", 8000))
                .setKeyPrefix(prefix)
                .setQueueDeadLetterSuffix("failed")
                .setQueueMaintainIntervalMillis(50)
                .setQueueMaintainBatchSize(20);
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
    public void normalQueueSupportsFifoPauseStatsAndCapacity() {
        RedisQueueOptions options = new RedisQueueOptions()
                .setMaxLength(2)
                .setFullQueuePolicy(FullQueuePolicy.DROP_OLDEST);
        RedisQueue<String> queue = RedisQueueKit.queue("normal", StringRedisCodec.INSTANCE, options);

        queue.offer("a");
        queue.offer("b");
        queue.offer("c");

        assertEquals(2, queue.size());
        assertEquals("b", queue.poll(1000).getBody());

        queue.pause();
        assertTrue(queue.isPaused());
        assertNull(queue.poll(0));
        queue.resume();
        assertFalse(queue.isPaused());
        assertEquals("c", queue.poll(1000).getBody());

        RedisQueueStats stats = queue.stats();
        assertEquals("normal", stats.getQueue());
    }

    @Test
    public void normalQueueSupportsDropNewestAndRejectPolicies() {
        RedisQueue<String> dropNewest = RedisQueueKit.queue("normal-drop-newest", StringRedisCodec.INSTANCE,
                new RedisQueueOptions().setMaxLength(1).setFullQueuePolicy(FullQueuePolicy.DROP_NEWEST));
        assertTrue(dropNewest.offer("d1", "a"));
        assertFalse(dropNewest.offer("d2", "b"));
        assertEquals("a", dropNewest.poll(1000).getBody());

        RedisQueue<String> reject = RedisQueueKit.queue("normal-reject", StringRedisCodec.INSTANCE,
                new RedisQueueOptions().setMaxLength(1).setFullQueuePolicy(FullQueuePolicy.REJECT));
        assertTrue(reject.offer("r1", "a"));
        try {
            reject.offer("r2", "b");
            fail("full queue should reject new message");
        } catch (RedisException expected) {
            assertTrue(expected.getMessage().contains("Queue is full"));
        }
    }

    @Test
    public void delayQueueWaitsUntilDueAndCanCancel() throws Exception {
        RedisDelayQueue<String> queue = RedisQueueKit.delayQueue("delay", StringRedisCodec.INSTANCE, new RedisQueueOptions());

        queue.offer("later", 200);
        assertNull(queue.poll(0));
        Thread.sleep(260);
        assertEquals("later", queue.poll(1000).getBody());

        assertTrue(queue.offer("cancel-me", "x", 5000));
        assertTrue(queue.cancel("cancel-me"));
        assertEquals(0, queue.delayedSize());
    }

    @Test
    public void reliableQueueSupportsRetryDeadReplayTtlPauseAndStats() throws Exception {
        RedisReliableQueue<String> queue = RedisQueueKit.reliableQueue("reliable-full", StringRedisCodec.INSTANCE,
                new RedisQueueOptions()
                        .setVisibilityTimeoutMillis(100)
                        .setMaxRetries(1)
                        .setRetryDelayPolicy(RetryDelayPolicy.fixed(10))
                        .setMessageTtlMillis(5000));

        queue.offer("r1", "job");
        RedisMessage<String> first = queue.reserve("c1", 1000);
        assertNotNull(first);
        assertEquals("job", first.getBody());
        assertEquals(1, first.getAttempts());

        Thread.sleep(180);
        RedisMessage<String> second = queue.reserve("c1", 0);
        assertNull(second);
        assertEquals(1, queue.deadSize());
        assertTrue(RedisKit.execute(jedis -> jedis.exists(prefix + ":queue:{reliable-full}:failed")));

        assertTrue(queue.replayDead("r1"));
        RedisMessage<String> replayed = queue.reserve("c1", 1000);
        assertNotNull(replayed);
        assertEquals(1, replayed.getAttempts());
        assertTrue(replayed.getCreatedAtMillis() > 0);
        assertTrue(replayed.getAvailableAtMillis() > 0);
        queue.ack(replayed.getId());
        assertEquals(0, queue.reservedSize());

        queue.offer("pause-me", "p");
        queue.pause();
        assertTrue(queue.isPaused());
        assertNull(queue.reserve("c1", 0));
        queue.resume();
        assertNotNull(queue.reserve("c1", 1000));

        RedisQueueStats stats = queue.stats();
        assertEquals("reliable-full", stats.getQueue());
    }

    @Test
    public void reliableQueueSupportsManualNackRetryLaterAndTtl() throws Exception {
        RedisReliableQueue<String> queue = RedisQueueKit.reliableQueue("reliable-manual", StringRedisCodec.INSTANCE,
                new RedisQueueOptions()
                        .setVisibilityTimeoutMillis(1000)
                        .setMaxRetries(3)
                        .setRetryDelayPolicy(RetryDelayPolicy.fixed(10))
                        .setMessageTtlMillis(1000));

        queue.offer("nack-id", "nack");
        RedisMessage<String> first = queue.reserve("manual-c", 1000);
        assertNotNull(first);
        queue.nack(first.getId());
        RedisMessage<String> retried = queue.reserve("manual-c", 1000);
        assertNotNull(retried);
        assertEquals(2, retried.getAttempts());
        queue.ack(retried.getId());

        queue.offer("later-id", "later");
        RedisMessage<String> later = queue.reserve("manual-c", 1000);
        assertNotNull(later);
        queue.retryLater(later.getId(), 200);
        assertNull(queue.reserve("manual-c", 0));
        Thread.sleep(260);
        RedisMessage<String> due = queue.reserve("manual-c", 1000);
        assertNotNull(due);
        assertEquals("later", due.getBody());
        queue.ack(due.getId());

        RedisReliableQueue<String> ttlQueue = RedisQueueKit.reliableQueue("reliable-ttl", StringRedisCodec.INSTANCE,
                new RedisQueueOptions().setVisibilityTimeoutMillis(1000).setMaxRetries(3).setMessageTtlMillis(80));
        ttlQueue.offer("ttl-id", "expired");
        Thread.sleep(120);
        assertNull(ttlQueue.reserve("manual-c", 0));
        assertEquals(1, ttlQueue.deadSize());
    }

    @Test
    public void reliableQueueSupportsBatchOperations() {
        RedisReliableQueue<String> queue = RedisQueueKit.reliableQueue("reliable-batch", StringRedisCodec.INSTANCE,
                new RedisQueueOptions().setVisibilityTimeoutMillis(1000));

        queue.offerBatch(Arrays.asList("a", "b", "c"));
        List<RedisMessage<String>> messages = queue.reserveBatch("batch-c", 2, 1000);

        assertEquals(2, messages.size());
        queue.ackBatch(messages.get(0).getId(), messages.get(1).getId());
        assertEquals(0, queue.reservedSize());
    }

    @Test
    public void priorityQueueConsumesHighPriorityFirst() {
        RedisPriorityQueue<String> queue = RedisQueueKit.priorityQueue("priority", StringRedisCodec.INSTANCE,
                new RedisQueueOptions().setVisibilityTimeoutMillis(1000));

        queue.offer("low", "low", 1);
        queue.offer("high", "high", 10);

        RedisMessage<String> first = queue.reserve("pc", 1000);
        assertNotNull(first);
        assertEquals("high", first.getBody());
        queue.ack(first.getId());
        assertFalse(RedisKit.execute(jedis -> jedis.hexists(prefix + ":queue:{priority}:priority-value", first.getId())));

        RedisMessage<String> second = queue.reserve("pc", 1000);
        assertNotNull(second);
        assertEquals("low", second.getBody());
        queue.ack(second.getId());
    }

    @Test
    public void priorityQueueMovesExpiredRetriesToDead() throws Exception {
        RedisPriorityQueue<String> queue = RedisQueueKit.priorityQueue("priority-retry", StringRedisCodec.INSTANCE,
                new RedisQueueOptions()
                        .setVisibilityTimeoutMillis(80)
                        .setMaxRetries(1)
                        .setRetryDelayPolicy(RetryDelayPolicy.fixed(10)));

        queue.offer("p1", "will-dead", 5);
        RedisMessage<String> first = queue.reserve("pc", 1000);
        assertNotNull(first);
        Thread.sleep(120);
        assertNull(queue.reserve("pc", 0));
        assertEquals(1, queue.deadSize());
    }

    @Test
    public void streamQueueSupportsGroupsPendingClaimPauseAndStats() throws Exception {
        RedisStreamQueue<String> stream = RedisQueueKit.streamQueue("stream", StringRedisCodec.INSTANCE, new RedisQueueOptions());

        stream.createGroup("g1");
        stream.createGroup("g2");
        stream.add("business-1", "body");

        List<RedisMessage<String>> g1 = stream.readGroup("g1", "c1", 1, 1000);
        List<RedisMessage<String>> g2 = stream.readGroup("g2", "c2", 1, 1000);
        assertEquals(1, g1.size());
        assertEquals(1, g2.size());

        List<RedisMessage<String>> pending = stream.pending("g1", "c1", 10);
        assertEquals(1, pending.size());
        Thread.sleep(5);
        List<RedisMessage<String>> claimed = stream.claimIdle("g1", "c3", 1, 10);
        assertEquals(1, claimed.size());

        stream.ack("g1", claimed.get(0).getId());
        stream.ack("g2", g2.get(0).getId());

        stream.pause();
        assertTrue(stream.isPaused());
        stream.add("business-2", "body2");
        assertTrue(stream.readGroup("g1", "c1", 1, 0).isEmpty());
        stream.resume();

        RedisQueueStats stats = stream.stats();
        assertTrue(stats.getStreamLength() >= 1);
    }

    @Test
    public void workerAutoAcksSuccessfulMessages() throws Exception {
        List<String> handled = new CopyOnWriteArrayList<>();
        RedisQueueWorker<String> worker = RedisQueueKit.worker("worker", StringRedisCodec.INSTANCE,
                        new RedisQueueOptions().setVisibilityTimeoutMillis(1000).setMaxRetries(2))
                .consumerId("worker-c")
                .concurrency(1)
                .pollTimeoutMillis(100)
                .idleSleepMillis(10)
                .handler(handled::add);

        worker.start();
        try {
            worker.getQueue().offer("w1", "hello");
            waitUntil(() -> handled.contains("hello"), 3000);
            assertEquals(0, worker.getQueue().reservedSize());
        } finally {
            worker.close();
        }
    }

    @Test
    public void workerMovesFailedMessagesToDeadWhenRetriesExhausted() throws Exception {
        RedisQueueWorker<String> worker = RedisQueueKit.worker("worker-fail", StringRedisCodec.INSTANCE,
                        new RedisQueueOptions().setVisibilityTimeoutMillis(1000).setMaxRetries(1))
                .consumerId("worker-fail-c")
                .pollTimeoutMillis(50)
                .idleSleepMillis(10)
                .handler(value -> {
                    throw new IllegalStateException("boom");
                });

        worker.start();
        try {
            worker.getQueue().offer("bad", "fail");
            waitUntil(() -> worker.getQueue().deadSize() == 1, 3000);
        } finally {
            worker.close();
        }
    }

    private void waitUntil(Check check, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("condition not met within " + timeoutMillis + " ms");
    }

    private interface Check {
        boolean ok();
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
}
