package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.RedisConfig;
import io.github.macaque0.aifei.redis.codec.JsonRedisCodec;
import io.github.macaque0.aifei.redis.codec.RedisCodec;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class DefaultRedisQueueFactory implements RedisQueueFactory {

    private final Redis redis;
    private final RedisConfig config;
    private final List<RedisQueueMaintenance> maintenances = new CopyOnWriteArrayList<>();

    public DefaultRedisQueueFactory(Redis redis, RedisConfig config) {
        if (redis == null) {
            throw new IllegalArgumentException("redis can not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        this.redis = redis;
        this.config = config;
    }

    @Override
    public <T> RedisQueue<T> queue(String name, Class<T> type) {
        return queue(name, new JsonRedisCodec<>(type), defaultOptions());
    }

    @Override
    public <T> RedisQueue<T> queue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return new DefaultRedisQueue<>(redis, keys(name), codec, useOptions(options));
    }

    @Override
    public <T> RedisDelayQueue<T> delayQueue(String name, Class<T> type) {
        return delayQueue(name, new JsonRedisCodec<>(type), defaultOptions());
    }

    @Override
    public <T> RedisDelayQueue<T> delayQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        DefaultRedisDelayQueue<T> queue = new DefaultRedisDelayQueue<>(redis, keys(name), codec, useOptions(options));
        maintenances.add(queue);
        return queue;
    }

    @Override
    public <T> RedisReliableQueue<T> reliableQueue(String name, Class<T> type) {
        return reliableQueue(name, new JsonRedisCodec<>(type), defaultOptions());
    }

    @Override
    public <T> RedisReliableQueue<T> reliableQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        DefaultRedisReliableQueue<T> queue = new DefaultRedisReliableQueue<>(redis, keys(name), codec, useOptions(options));
        maintenances.add(queue);
        return queue;
    }

    @Override
    public <T> RedisStreamQueue<T> streamQueue(String name, Class<T> type) {
        return streamQueue(name, new JsonRedisCodec<>(type), defaultOptions());
    }

    @Override
    public <T> RedisStreamQueue<T> streamQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return new DefaultRedisStreamQueue<>(redis, keys(name), codec, useOptions(options));
    }

    @Override
    public <T> RedisPriorityQueue<T> priorityQueue(String name, Class<T> type) {
        return priorityQueue(name, new JsonRedisCodec<>(type), defaultOptions());
    }

    @Override
    public <T> RedisPriorityQueue<T> priorityQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        DefaultRedisPriorityQueue<T> queue = new DefaultRedisPriorityQueue<>(redis, keys(name), codec, useOptions(options));
        maintenances.add(queue);
        return queue;
    }

    @Override
    public <T> RedisQueueWorker<T> worker(String name, Class<T> type) {
        return worker(name, new JsonRedisCodec<>(type), defaultOptions());
    }

    @Override
    public <T> RedisQueueWorker<T> worker(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return new RedisQueueWorker<>(this, name, codec, useOptions(options));
    }

    List<RedisQueueMaintenance> maintenances() {
        return maintenances;
    }

    public RedisQueueOptions defaultOptions() {
        return new RedisQueueOptions()
                .setVisibilityTimeoutMillis(config.getQueueDefaultVisibilityTimeoutMillis())
                .setMaxRetries(config.getQueueDefaultMaxRetries())
                .setMaintainBatchSize(config.getQueueMaintainBatchSize());
    }

    private QueueKeySet keys(String name) {
        return new QueueKeySet(config.getKeyPrefix(), name, config.getQueueDeadLetterSuffix());
    }

    private RedisQueueOptions useOptions(RedisQueueOptions options) {
        return options == null ? defaultOptions() : options;
    }
}
