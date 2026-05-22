package io.github.macaque0.aifei.redis.queue;

import java.util.List;

public interface RedisStreamQueue<T> {

    String add(T body);

    String add(String messageId, T body);

    void createGroup(String group);

    List<RedisMessage<T>> readGroup(String group, String consumer, int count, long blockMillis);

    void ack(String group, String... messageIds);

    List<RedisMessage<T>> pending(String group, String consumer, int count);

    List<RedisMessage<T>> claimIdle(String group, String consumer, long minIdleMillis, int count);

    void pause();

    void resume();

    boolean isPaused();

    RedisQueueStats stats();
}
