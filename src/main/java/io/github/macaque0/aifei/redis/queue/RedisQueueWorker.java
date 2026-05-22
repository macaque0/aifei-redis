package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.RedisCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisQueueWorker<T> implements AutoCloseable {

    private final RedisQueueFactory queueFactory;
    private final String queueName;
    private final RedisCodec<T> codec;
    private final RedisQueueOptions options;
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private String consumerId = "worker-" + Integer.toHexString(System.identityHashCode(this));
    private int concurrency = 1;
    private long pollTimeoutMillis = 1000;
    private long idleSleepMillis = 50;
    private RedisMessageHandler<T> handler;
    private RedisQueueWorkerListener<T> listener = new RedisQueueWorkerListener<T>() {
    };
    private RedisReliableQueue<T> queue;

    public RedisQueueWorker(RedisQueueFactory queueFactory, String queueName, RedisCodec<T> codec, RedisQueueOptions options) {
        if (queueFactory == null) {
            throw new IllegalArgumentException("queueFactory can not be null");
        }
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("queueName can not be blank");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec can not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options can not be null");
        }
        this.queueFactory = queueFactory;
        this.queueName = queueName;
        this.codec = codec;
        this.options = options;
    }

    public RedisQueueWorker<T> consumerId(String consumerId) {
        if (consumerId == null || consumerId.trim().isEmpty()) {
            throw new IllegalArgumentException("consumerId can not be blank");
        }
        this.consumerId = consumerId.trim();
        return this;
    }

    public RedisQueueWorker<T> concurrency(int concurrency) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be positive");
        }
        this.concurrency = concurrency;
        return this;
    }

    public RedisQueueWorker<T> pollTimeoutMillis(long pollTimeoutMillis) {
        if (pollTimeoutMillis < 0) {
            throw new IllegalArgumentException("pollTimeoutMillis can not be negative");
        }
        this.pollTimeoutMillis = pollTimeoutMillis;
        return this;
    }

    public RedisQueueWorker<T> idleSleepMillis(long idleSleepMillis) {
        if (idleSleepMillis < 0) {
            throw new IllegalArgumentException("idleSleepMillis can not be negative");
        }
        this.idleSleepMillis = idleSleepMillis;
        return this;
    }

    public RedisQueueWorker<T> visibilityTimeoutMillis(long visibilityTimeoutMillis) {
        options.setVisibilityTimeoutMillis(visibilityTimeoutMillis);
        return this;
    }

    public RedisQueueWorker<T> maxRetries(int maxRetries) {
        options.setMaxRetries(maxRetries);
        return this;
    }

    public RedisQueueWorker<T> retryDelayPolicy(RetryDelayPolicy retryDelayPolicy) {
        options.setRetryDelayPolicy(retryDelayPolicy);
        return this;
    }

    public RedisQueueWorker<T> handler(RedisQueueHandler<T> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler can not be null");
        }
        this.handler = message -> handler.handle(message.getBody());
        return this;
    }

    public RedisQueueWorker<T> messageHandler(RedisMessageHandler<T> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler can not be null");
        }
        this.handler = handler;
        return this;
    }

    public RedisQueueWorker<T> listener(RedisQueueWorkerListener<T> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener can not be null");
        }
        this.listener = listener;
        return this;
    }

    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        if (handler == null) {
            running.set(false);
            throw new IllegalStateException("RedisQueueWorker handler has not been set");
        }
        queue = queueFactory.reliableQueue(queueName, codec, options);
        for (int i = 0; i < concurrency; i++) {
            Thread thread = new Thread(() -> runLoop(Thread.currentThread().getName()), "aifei-redis-worker-" + queueName + "-" + i);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        listener.onStart(this);
    }

    public boolean isRunning() {
        return running.get();
    }

    public RedisReliableQueue<T> getQueue() {
        RedisReliableQueue<T> ret = queue;
        if (ret == null) {
            throw new IllegalStateException("RedisQueueWorker has not been started");
        }
        return ret;
    }

    @Override
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (Thread thread : threads) {
            try {
                thread.join(Math.max(100, pollTimeoutMillis + idleSleepMillis));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        threads.clear();
        listener.onStop(this);
    }

    private void runLoop(String threadName) {
        String actualConsumerId = consumerId + ":" + threadName;
        while (running.get()) {
            try {
                RedisMessage<T> message = queue.reserve(actualConsumerId, pollTimeoutMillis);
                if (message == null) {
                    sleep(idleSleepMillis);
                    continue;
                }
                handle(message);
            } catch (Throwable e) {
                listener.onError(e);
                sleep(idleSleepMillis);
            }
        }
    }

    private void handle(RedisMessage<T> message) {
        try {
            handler.handle(message);
            queue.ack(message.getId());
            listener.onSuccess(message);
        } catch (Throwable e) {
            if (message.getAttempts() >= options.getMaxRetries()) {
                queue.dead(message.getId(), e.getClass().getName() + ":" + safe(e.getMessage()));
                listener.onDead(message, e);
                return;
            }
            long delayMillis = options.getRetryDelayPolicy().nextDelayMillis(message.getAttempts(), message);
            queue.retryLater(message.getId(), Math.max(0, delayMillis));
            listener.onRetry(message, e, Math.max(0, delayMillis));
        }
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value.replace("|", "_");
    }
}
