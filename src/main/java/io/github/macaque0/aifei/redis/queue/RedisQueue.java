package io.github.macaque0.aifei.redis.queue;

import java.util.List;

public interface RedisQueue<T> {

    String offer(T body);

    boolean offer(String messageId, T body);

    void offerBatch(List<T> bodies);

    RedisMessage<T> poll();

    RedisMessage<T> poll(long timeoutMillis);

    long size();

    void pause();

    void resume();

    boolean isPaused();

    RedisQueueStats stats();
}
