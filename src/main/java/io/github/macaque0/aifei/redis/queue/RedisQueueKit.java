package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.RedisCodec;

public class RedisQueueKit {

    private static volatile RedisQueueFactory queueFactory;

    public static void init(RedisQueueFactory queueFactory) {
        if (queueFactory == null) {
            throw new IllegalArgumentException("queueFactory can not be null");
        }
        RedisQueueKit.queueFactory = queueFactory;
    }

    public static RedisQueueFactory getQueueFactory() {
        RedisQueueFactory ret = queueFactory;
        if (ret == null) {
            throw new IllegalStateException("RedisQueueKit has not been initialized. Add RedisPlugin first.");
        }
        return ret;
    }

    public static <T> RedisQueue<T> queue(String name, Class<T> type) {
        return getQueueFactory().queue(name, type);
    }

    public static <T> RedisQueue<T> queue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return getQueueFactory().queue(name, codec, options);
    }

    public static <T> RedisDelayQueue<T> delayQueue(String name, Class<T> type) {
        return getQueueFactory().delayQueue(name, type);
    }

    public static <T> RedisDelayQueue<T> delayQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return getQueueFactory().delayQueue(name, codec, options);
    }

    public static <T> RedisReliableQueue<T> reliableQueue(String name, Class<T> type) {
        return getQueueFactory().reliableQueue(name, type);
    }

    public static <T> RedisReliableQueue<T> reliableQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return getQueueFactory().reliableQueue(name, codec, options);
    }

    public static <T> RedisStreamQueue<T> streamQueue(String name, Class<T> type) {
        return getQueueFactory().streamQueue(name, type);
    }

    public static <T> RedisStreamQueue<T> streamQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return getQueueFactory().streamQueue(name, codec, options);
    }

    public static <T> RedisPriorityQueue<T> priorityQueue(String name, Class<T> type) {
        return getQueueFactory().priorityQueue(name, type);
    }

    public static <T> RedisPriorityQueue<T> priorityQueue(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return getQueueFactory().priorityQueue(name, codec, options);
    }

    public static <T> RedisQueueWorker<T> worker(String name, Class<T> type) {
        return getQueueFactory().worker(name, type);
    }

    public static <T> RedisQueueWorker<T> worker(String name, RedisCodec<T> codec, RedisQueueOptions options) {
        return getQueueFactory().worker(name, codec, options);
    }

    public static void clearInit() {
        queueFactory = null;
    }

    public static void clearInit(RedisQueueFactory expected) {
        if (queueFactory == expected) {
            queueFactory = null;
        }
    }
}
