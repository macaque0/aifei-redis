package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.RedisConfig;
import io.github.macaque0.aifei.redis.codec.ByteArrayRedisCodec;
import io.github.macaque0.aifei.redis.codec.JsonRedisCodec;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import io.github.macaque0.aifei.redis.codec.StringRedisCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 队列对象工厂，统一负责 key 命名、默认配置和 codec 选择。
 */
public class DefaultRedisQueueFactory implements RedisQueueFactory {

    private final Redis redis;
    private final RedisConfig config;
    private final ConcurrentMap<String, RedisQueueMaintenance> maintenances = new ConcurrentHashMap<>();

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
        return queue(name, codec(type), defaultOptions());
    }

    @Override
    public <T> RedisQueue<T> queue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return new DefaultRedisQueue<>(redis, keys(name), codec, useOptions(options));
    }

    @Override
    public <T> RedisDelayQueue<T> delayQueue(String name, Class<T> type) {
        return delayQueue(name, codec(type), defaultOptions());
    }

    @Override
    public <T> RedisDelayQueue<T> delayQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        QueueKeySet keySet = keys(name);
        DefaultRedisDelayQueue<T> queue = new DefaultRedisDelayQueue<>(redis, keySet, codec, useOptions(options));
        // 延迟队列需要维护线程把到期消息提升到 ready 队列。
        registerMaintenance("delay", keySet, queue);
        return queue;
    }

    @Override
    public <T> RedisReliableQueue<T> reliableQueue(String name, Class<T> type) {
        return reliableQueue(name, codec(type), defaultOptions());
    }

    @Override
    public <T> RedisReliableQueue<T> reliableQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        QueueKeySet keySet = keys(name);
        DefaultRedisReliableQueue<T> queue = new DefaultRedisReliableQueue<>(redis, keySet, codec, useOptions(options));
        // 可靠队列需要维护线程处理可见性超时、重试和死信。
        registerMaintenance("reliable", keySet, queue);
        return queue;
    }

    @Override
    public <T> RedisStreamQueue<T> streamQueue(String name, Class<T> type) {
        return streamQueue(name, codec(type), defaultOptions());
    }

    @Override
    public <T> RedisStreamQueue<T> streamQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return new DefaultRedisStreamQueue<>(redis, keys(name), codec, useOptions(options));
    }

    @Override
    public <T> RedisPriorityQueue<T> priorityQueue(String name, Class<T> type) {
        return priorityQueue(name, codec(type), defaultOptions());
    }

    @Override
    public <T> RedisPriorityQueue<T> priorityQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        QueueKeySet keySet = keys(name);
        DefaultRedisPriorityQueue<T> queue = new DefaultRedisPriorityQueue<>(redis, keySet, codec, useOptions(options));
        // 优先级队列也具备可靠语义，因此同样参与后台维护。
        registerMaintenance("priority", keySet, queue);
        return queue;
    }

    @Override
    public <T> RedisQueueWorker<T> worker(String name, Class<T> type) {
        return worker(name, codec(type), defaultOptions());
    }

    @Override
    public <T> RedisQueueWorker<T> worker(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return new RedisQueueWorker<>(this, name, codec, useOptions(options));
    }

    List<RedisQueueMaintenance> maintenances() {
        return new ArrayList<>(maintenances.values());
    }

    public RedisQueueOptions defaultOptions() {
        // 每次创建新 options，避免调用方修改某个队列配置时影响其他队列。
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

    private void registerMaintenance(String type, QueueKeySet keySet, RedisQueueMaintenance queue) {
        // 同名队列反复获取时只保留一个维护对象；若传入新 options，以最新创建的队列配置为准。
        maintenances.put(type + ":" + keySet.name, queue);
    }

    @SuppressWarnings("unchecked")
    private static <T> RedisCodec<T> codec(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("type can not be null");
        }
        // 常用类型走专用 codec，避免 String 被 JSON 额外包一层引号。
        if (type == String.class) {
            return (RedisCodec<T>) StringRedisCodec.INSTANCE;
        }
        if (type == byte[].class) {
            return (RedisCodec<T>) ByteArrayRedisCodec.INSTANCE;
        }
        return new JsonRedisCodec<>(type);
    }
}
