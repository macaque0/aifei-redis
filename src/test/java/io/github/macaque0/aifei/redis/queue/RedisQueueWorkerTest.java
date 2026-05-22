package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.StringRedisCodec;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class RedisQueueWorkerTest {

    @Test
    public void builderAcceptsValidOptions() {
        RedisQueueWorker<String> worker = new RedisQueueWorker<>(
                new StubQueueFactory(),
                "jobs",
                StringRedisCodec.INSTANCE,
                new RedisQueueOptions());

        worker.consumerId("c1")
                .concurrency(2)
                .pollTimeoutMillis(10)
                .idleSleepMillis(1)
                .visibilityTimeoutMillis(100)
                .maxRetries(3)
                .retryDelayPolicy(RetryDelayPolicy.fixed(5))
                .listener(new RedisQueueWorkerListener<String>() {
                })
                .handler(value -> {
                });

        assertFalse(worker.isRunning());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInvalidConcurrency() {
        new RedisQueueWorker<>(new StubQueueFactory(), "jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions())
                .concurrency(0);
    }

    @Test(expected = IllegalStateException.class)
    public void queueUnavailableBeforeStart() {
        new RedisQueueWorker<>(new StubQueueFactory(), "jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions())
                .getQueue();
    }

    private static class StubQueueFactory implements RedisQueueFactory {

        @Override
        public <T> RedisQueue<T> queue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisQueue<T> queue(String name, io.github.macaque0.aifei.redis.codec.RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisDelayQueue<T> delayQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisDelayQueue<T> delayQueue(String name, io.github.macaque0.aifei.redis.codec.RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisReliableQueue<T> reliableQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisReliableQueue<T> reliableQueue(String name, io.github.macaque0.aifei.redis.codec.RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisStreamQueue<T> streamQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisStreamQueue<T> streamQueue(String name, io.github.macaque0.aifei.redis.codec.RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisPriorityQueue<T> priorityQueue(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisPriorityQueue<T> priorityQueue(String name, io.github.macaque0.aifei.redis.codec.RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisQueueWorker<T> worker(String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> RedisQueueWorker<T> worker(String name, io.github.macaque0.aifei.redis.codec.RedisCodec<T> codec, RedisQueueOptions options) {
            throw new UnsupportedOperationException();
        }
    }
}
