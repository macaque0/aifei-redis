package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.RedisCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 可靠队列的后台消费者。
 *
 * <p>Worker 封装 reserve -> 执行业务 -> ack/retry/dead 的循环，适合生产任务直接使用。
 * 业务 handler 必须保持幂等，因为可靠队列提供的是至少一次投递。</p>
 */
public class RedisQueueWorker<T> implements AutoCloseable {

    private final RedisQueueFactory queueFactory;
    private final String queueName;
    private final RedisCodec<T> codec;
    private final RedisQueueOptions options;
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private String consumerId = "worker-" + Integer.toHexString(System.identityHashCode(this));
    private int concurrency = 1;
    private int batchSize = 1;
    private long pollTimeoutMillis = 1000;
    private long idleSleepMillis = 50;
    private RedisMessageHandler<T> handler;
    // 旧的 worker 级回调保留给业务定制；统一审计/指标建议使用 RedisQueueEventKit。
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

    public RedisQueueWorker<T> batchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
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
        // Worker 始终使用可靠队列，确保业务执行失败时能 retry 或进入死信。
        queue = queueFactory.reliableQueue(queueName, codec, options);
        for (int i = 0; i < concurrency; i++) {
            Thread thread = new Thread(() -> runLoop(Thread.currentThread().getName()), "aifei-redis-worker-" + queueName + "-" + i);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        notifyStart();
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
        notifyStop();
    }

    private void runLoop(String threadName) {
        // consumerId 加线程名，方便在 Redis reserved 元数据和日志中定位具体消费线程。
        String actualConsumerId = consumerId + ":" + threadName;
        while (running.get()) {
            try {
                List<RedisMessage<T>> messages = queue.reserveBatch(actualConsumerId, batchSize, pollTimeoutMillis);
                if (messages.isEmpty()) {
                    sleep(idleSleepMillis);
                    continue;
                }
                handle(messages, actualConsumerId);
            } catch (Throwable e) {
                RedisQueueEventKit.consumerError(queueName, actualConsumerId, e);
                notifyError(e);
                sleep(idleSleepMillis);
            }
        }
    }

    private void handle(List<RedisMessage<T>> messages, String actualConsumerId) {
        List<HandledSuccess<T>> successes = new ArrayList<>();
        for (RedisMessage<T> message : messages) {
            HandledSuccess<T> success = handleWithoutAck(message, actualConsumerId);
            if (success != null) {
                successes.add(success);
            }
        }
        if (successes.isEmpty()) {
            return;
        }
        queue.ackBatch(successIds(successes));
        for (HandledSuccess<T> success : successes) {
            RedisQueueEventKit.consumeSuccess(queueName, success.message, actualConsumerId, success.elapsedMillis);
            notifySuccess(success.message);
        }
    }

    private HandledSuccess<T> handleWithoutAck(RedisMessage<T> message, String actualConsumerId) {
        long started = System.currentTimeMillis();
        RedisQueueEventKit.consumeStart(queueName, message, actualConsumerId);
        try {
            handler.handle(message);
            // 只有业务方法正常返回才加入批量 ack；抛异常会进入 retry/dead 分支。
            return new HandledSuccess<>(message, elapsedSince(started));
        } catch (Throwable e) {
            if (message.getAttempts() >= options.getMaxRetries()) {
                // 达到最大尝试次数后进入死信，保留错误摘要便于后续排查或人工回放。
                queue.dead(message.getId(), e.getClass().getName() + ":" + safe(e.getMessage()));
                RedisQueueEventKit.dead(queueName, message, actualConsumerId, e, elapsedSince(started));
                notifyDead(message, e);
                return null;
            }
            long delayMillis = options.getRetryDelayPolicy().nextDelayMillis(message.getAttempts(), message);
            // 未达到最大重试次数时按策略延迟重试，避免失败消息立即反复冲击下游。
            queue.retryLater(message.getId(), Math.max(0, delayMillis));
            RedisQueueEventKit.retry(queueName, message, actualConsumerId, e, Math.max(0, delayMillis), elapsedSince(started));
            notifyRetry(message, e, Math.max(0, delayMillis));
            return null;
        }
    }

    // 业务回调异常只记录为消费线程异常，不反向影响消息 ack/retry 结果。
    private void notifyStart() {
        try {
            listener.onStart(this);
        } catch (Throwable e) {
            RedisQueueEventKit.consumerError(queueName, consumerId, e);
        }
    }

    private void notifyStop() {
        try {
            listener.onStop(this);
        } catch (Throwable e) {
            RedisQueueEventKit.consumerError(queueName, consumerId, e);
        }
    }

    private void notifySuccess(RedisMessage<T> message) {
        try {
            listener.onSuccess(message);
        } catch (Throwable e) {
            RedisQueueEventKit.consumerError(queueName, consumerId, e);
        }
    }

    private void notifyRetry(RedisMessage<T> message, Throwable error, long delayMillis) {
        try {
            listener.onRetry(message, error, delayMillis);
        } catch (Throwable e) {
            RedisQueueEventKit.consumerError(queueName, consumerId, e);
        }
    }

    private void notifyDead(RedisMessage<T> message, Throwable error) {
        try {
            listener.onDead(message, error);
        } catch (Throwable e) {
            RedisQueueEventKit.consumerError(queueName, consumerId, e);
        }
    }

    private void notifyError(Throwable error) {
        try {
            listener.onError(error);
        } catch (Throwable e) {
            RedisQueueEventKit.consumerError(queueName, consumerId, e);
        }
    }

    private static long elapsedSince(long started) {
        return Math.max(0, System.currentTimeMillis() - started);
    }

    private static <T> String[] successIds(List<HandledSuccess<T>> successes) {
        String[] ids = new String[successes.size()];
        for (int i = 0; i < successes.size(); i++) {
            ids[i] = successes.get(i).message.getId();
        }
        return ids;
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

    private static class HandledSuccess<T> {

        private final RedisMessage<T> message;
        private final long elapsedMillis;

        private HandledSuccess(RedisMessage<T> message, long elapsedMillis) {
            this.message = message;
            this.elapsedMillis = elapsedMillis;
        }
    }
}
