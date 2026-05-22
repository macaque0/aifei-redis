package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.RedisCodec;

import java.lang.reflect.Field;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RedisQueueListenerContainerTest {

    @Test
    public void duplicateRegistrationIsIgnored() throws Exception {
        RedisQueueListenerContainer container = new RedisQueueListenerContainer(new StubQueueFactory());
        Listener listener = new Listener();

        container.register(listener);
        container.register(listener);

        assertEquals(1, runnerCount(container));
    }

    @Test
    public void pollingRunnerContinuesAfterBusinessError() throws Exception {
        PollingQueue<String> queue = new PollingQueue<>();
        queue.messages.add(new RedisMessage<String>().setId("1").setQueue("jobs").setBody("a"));
        queue.messages.add(new RedisMessage<String>().setId("2").setQueue("jobs").setBody("b"));
        RedisQueueListenerContainer container = new RedisQueueListenerContainer(new StubQueueFactory(queue));
        ErrorListener listener = new ErrorListener();

        PrintStream oldErr = System.err;
        System.setErr(new PrintStream(new ByteArrayOutputStream()));
        try {
            container.register(listener);
            container.start();
            waitUntil(() -> listener.count.get() == 2, 1000);
        } finally {
            try {
                container.close();
            } finally {
                System.setErr(oldErr);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static int runnerCount(RedisQueueListenerContainer container) throws Exception {
        Field field = RedisQueueListenerContainer.class.getDeclaredField("runners");
        field.setAccessible(true);
        return ((List<Object>) field.get(container)).size();
    }

    public static class Listener {

        @RedisQueueListener("jobs")
        public void handle(String body) {
        }
    }

    public static class ErrorListener {

        final AtomicInteger count = new AtomicInteger();

        @RedisQueueListener(value = "jobs", pollTimeoutMillis = 10, idleSleepMillis = 1)
        public void handle(String body) {
            count.incrementAndGet();
            throw new AssertionError("planned business error");
        }
    }

    private static class StubQueueFactory implements RedisQueueFactory {

        private final RedisQueue<?> queue;

        StubQueueFactory() {
            this(new StubQueue<>());
        }

        StubQueueFactory(RedisQueue<?> queue) {
            this.queue = queue;
        }

        @Override
        public <T> RedisQueue<T> queue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> RedisQueue<T> queue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
            return (RedisQueue<T>) queue;
        }

        @Override
        public <T> RedisDelayQueue<T> delayQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisDelayQueue<T> delayQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisReliableQueue<T> reliableQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisReliableQueue<T> reliableQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisStreamQueue<T> streamQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisStreamQueue<T> streamQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisPriorityQueue<T> priorityQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisPriorityQueue<T> priorityQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisQueueWorker<T> worker(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisQueueWorker<T> worker(String name, RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static class PollingQueue<T> extends StubQueue<T> {

        final Queue<RedisMessage<T>> messages = new ConcurrentLinkedQueue<>();

        @Override
        public RedisMessage<T> poll() {
            return messages.poll();
        }

        @Override
        public RedisMessage<T> poll(long timeoutMillis) {
            return poll();
        }
    }

    private static class StubQueue<T> implements RedisQueue<T> {

        @Override
        public String offer(T body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean offer(String messageId, T body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void offerBatch(List<T> bodies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RedisMessage<T> poll() {
            return null;
        }

        @Override
        public RedisMessage<T> poll(long timeoutMillis) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void pause() {
        }

        @Override
        public void resume() {
        }

        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public RedisQueueStats stats() {
            return new RedisQueueStats();
        }
    }

    private static void waitUntil(Check check, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
        throw new AssertionError("condition not met within " + timeoutMillis + " ms");
    }

    private interface Check {
        boolean ok();
    }
}
