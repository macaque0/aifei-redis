package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.RedisCodec;

public interface RedisQueueFactory {

    <T> RedisQueue<T> queue(String name, Class<T> type);

    <T> RedisQueue<T> queue(String name, RedisCodec<T> codec, RedisQueueOptions options);

    <T> RedisDelayQueue<T> delayQueue(String name, Class<T> type);

    <T> RedisDelayQueue<T> delayQueue(String name, RedisCodec<T> codec, RedisQueueOptions options);

    <T> RedisReliableQueue<T> reliableQueue(String name, Class<T> type);

    <T> RedisReliableQueue<T> reliableQueue(String name, RedisCodec<T> codec, RedisQueueOptions options);

    <T> RedisStreamQueue<T> streamQueue(String name, Class<T> type);

    <T> RedisStreamQueue<T> streamQueue(String name, RedisCodec<T> codec, RedisQueueOptions options);

    <T> RedisPriorityQueue<T> priorityQueue(String name, Class<T> type);

    <T> RedisPriorityQueue<T> priorityQueue(String name, RedisCodec<T> codec, RedisQueueOptions options);

    <T> RedisQueueWorker<T> worker(String name, Class<T> type);

    <T> RedisQueueWorker<T> worker(String name, RedisCodec<T> codec, RedisQueueOptions options);
}
