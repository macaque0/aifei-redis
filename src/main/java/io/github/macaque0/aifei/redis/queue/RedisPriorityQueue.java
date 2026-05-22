package io.github.macaque0.aifei.redis.queue;

import java.util.List;

public interface RedisPriorityQueue<T> {

    String offer(T body, int priority);

    boolean offer(String messageId, T body, int priority);

    RedisMessage<T> reserve(String consumerId);

    RedisMessage<T> reserve(String consumerId, long timeoutMillis);

    List<RedisMessage<T>> reserveBatch(String consumerId, int count, long timeoutMillis);

    void ack(String messageId);

    void nack(String messageId);

    void retryLater(String messageId, long delayMillis);

    void dead(String messageId, String reason);

    long readySize();

    long reservedSize();

    long deadSize();

    void pause();

    void resume();

    boolean isPaused();

    RedisQueueStats stats();
}
