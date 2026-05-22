package io.github.macaque0.aifei.redis.queue;

public interface RedisMessageHandler<T> {

    void handle(RedisMessage<T> message) throws Exception;
}
