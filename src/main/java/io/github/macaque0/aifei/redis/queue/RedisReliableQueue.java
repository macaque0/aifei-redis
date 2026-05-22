package io.github.macaque0.aifei.redis.queue;

import java.util.List;

public interface RedisReliableQueue<T> {

    String offer(T body);

    boolean offer(String messageId, T body);

    String offer(T body, long delayMillis);

    boolean offer(String messageId, T body, long delayMillis);

    void offerBatch(List<T> bodies);

    RedisMessage<T> reserve(String consumerId);

    RedisMessage<T> reserve(String consumerId, long timeoutMillis);

    List<RedisMessage<T>> reserveBatch(String consumerId, int count, long timeoutMillis);

    void ack(String messageId);

    void ackBatch(String... messageIds);

    void nack(String messageId);

    void retryLater(String messageId, long delayMillis);

    void dead(String messageId, String reason);

    boolean replayDead(String messageId);

    int replayDeadBatch(int count);

    long readySize();

    long delayedSize();

    long reservedSize();

    long deadSize();

    void pause();

    void resume();

    boolean isPaused();

    RedisQueueStats stats();
}
