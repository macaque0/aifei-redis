package io.github.macaque0.aifei.redis.queue;

public interface RedisDelayQueue<T> {

    String offer(T body, long delayMillis);

    boolean offer(String messageId, T body, long delayMillis);

    String offerAt(T body, long timestampMillis);

    boolean offerAt(String messageId, T body, long timestampMillis);

    RedisMessage<T> poll();

    RedisMessage<T> poll(long timeoutMillis);

    boolean cancel(String messageId);

    long delayedSize();

    long readySize();

    void pause();

    void resume();

    boolean isPaused();

    RedisQueueStats stats();
}
