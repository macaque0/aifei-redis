package io.github.macaque0.aifei.redis.queue;

public interface RedisQueueHandler<T> {

    void handle(T body) throws Exception;
}
