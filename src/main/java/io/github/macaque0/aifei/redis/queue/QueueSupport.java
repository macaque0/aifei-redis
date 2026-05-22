package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.RedisException;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

abstract class QueueSupport<T> {

    protected final Redis redis;
    protected final QueueKeySet keys;
    protected final RedisCodec<T> codec;
    protected final RedisQueueOptions options;

    QueueSupport(Redis redis, QueueKeySet keys, RedisCodec<T> codec, RedisQueueOptions options) {
        if (redis == null) {
            throw new IllegalArgumentException("redis can not be null");
        }
        if (keys == null) {
            throw new IllegalArgumentException("keys can not be null");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec can not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options can not be null");
        }
        this.redis = redis;
        this.keys = keys;
        this.codec = codec;
        this.options = options;
    }

    protected String newMessageId() {
        return System.currentTimeMillis() + "-" + UUID.randomUUID().toString().replace("-", "");
    }

    protected String requireMessageId(String messageId) {
        if (messageId == null || messageId.trim().isEmpty()) {
            throw new IllegalArgumentException("messageId can not be blank");
        }
        return messageId.trim();
    }

    protected String encode(T body) {
        if (body == null) {
            throw new IllegalArgumentException("body can not be null");
        }
        String encoded = codec.encode(body);
        if (encoded == null) {
            throw new IllegalArgumentException("encoded body can not be null");
        }
        return encoded;
    }

    protected T decode(String encoded) {
        try {
            return codec.decode(encoded);
        } catch (RuntimeException e) {
            throw new RedisException("Queue message decode failed: " + keys.name, e);
        }
    }

    protected String meta(long createdAtMillis, long availableAtMillis, long reservedUntilMillis, int attempts, String consumerId) {
        return createdAtMillis + "|" + availableAtMillis + "|" + reservedUntilMillis + "|" + attempts + "|" + safe(consumerId);
    }

    protected RedisMessage<T> message(String id, String encoded, String meta, int attempts) {
        Meta parsed = Meta.parse(meta);
        return new RedisMessage<T>()
                .setId(id)
                .setQueue(keys.name)
                .setBody(decode(encoded))
                .setCreatedAtMillis(parsed.createdAtMillis)
                .setAvailableAtMillis(parsed.availableAtMillis)
                .setReservedUntilMillis(parsed.reservedUntilMillis)
                .setAttempts(attempts > 0 ? attempts : parsed.attempts);
    }

    protected RedisMessage<T> readMessage(Jedis jedis, String id) {
        String body = jedis.hget(keys.payload, id);
        if (body == null) {
            return null;
        }
        String meta = jedis.hget(keys.meta, id);
        String attempts = jedis.hget(keys.attempts, id);
        return message(id, body, meta, parseInt(attempts, 0));
    }

    protected boolean expired(String meta, long now) {
        if (options.getMessageTtlMillis() <= 0) {
            return false;
        }
        Meta parsed = Meta.parse(meta);
        return parsed.createdAtMillis > 0 && now - parsed.createdAtMillis >= options.getMessageTtlMillis();
    }

    protected void enforceCapacity(Jedis jedis) {
        long maxLength = options.getMaxLength();
        if (maxLength <= 0) {
            return;
        }
        long length = jedis.llen(keys.ready);
        if (length < maxLength) {
            return;
        }
        if (options.getFullQueuePolicy() == FullQueuePolicy.DROP_NEWEST || options.getFullQueuePolicy() == FullQueuePolicy.REJECT) {
            throw new RedisException("Queue is full: " + keys.name);
        }
        while (length >= maxLength) {
            String old = jedis.lpop(keys.ready);
            if (old == null) {
                break;
            }
            deletePayload(jedis, old);
            length--;
        }
    }

    protected void deletePayload(Jedis jedis, String id) {
        jedis.hdel(keys.payload, id);
        jedis.hdel(keys.meta, id);
        jedis.hdel(keys.attempts, id);
        jedis.hdel(keys.priorityValue, id);
        jedis.zrem(keys.reserved, id);
        jedis.zrem(keys.delay, id);
        jedis.zrem(keys.dead, id);
        jedis.zrem(keys.priority, id);
    }

    protected void pauseQueue() {
        redis.execute(jedis -> jedis.hset(keys.control, "paused", "1"));
    }

    protected void resumeQueue() {
        redis.execute(jedis -> jedis.hdel(keys.control, "paused"));
    }

    protected boolean isPaused() {
        return redis.execute(jedis -> isPaused(jedis));
    }

    protected boolean isPaused(Jedis jedis) {
        return "1".equals(jedis.hget(keys.control, "paused"));
    }

    protected RedisQueueStats stats() {
        return redis.execute(jedis -> stats(jedis));
    }

    protected RedisQueueStats stats(Jedis jedis) {
        return new RedisQueueStats()
                .setQueue(keys.name)
                .setReadySize(jedis.exists(keys.ready) ? jedis.llen(keys.ready) : 0)
                .setDelayedSize(jedis.exists(keys.delay) ? jedis.zcard(keys.delay) : 0)
                .setReservedSize(jedis.exists(keys.reserved) ? jedis.zcard(keys.reserved) : 0)
                .setDeadSize(jedis.exists(keys.dead) ? jedis.zcard(keys.dead) : 0)
                .setPrioritySize(jedis.exists(keys.priority) ? jedis.zcard(keys.priority) : 0)
                .setStreamLength(jedis.exists(keys.stream) ? jedis.xlen(keys.stream) : 0)
                .setPaused(isPaused(jedis));
    }

    protected List<RedisMessage<T>> listMessages(Jedis jedis, List<String> ids) {
        List<RedisMessage<T>> messages = new ArrayList<>();
        if (ids == null) {
            return messages;
        }
        for (String id : ids) {
            RedisMessage<T> message = readMessage(jedis, id);
            if (message != null) {
                messages.add(message);
            }
        }
        return messages;
    }

    private static String safe(String value) {
        return value == null ? "" : value.replace("|", "_");
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return value == null ? defaultValue : Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    static class Meta {
        long createdAtMillis;
        long availableAtMillis;
        long reservedUntilMillis;
        int attempts;

        static Meta parse(String value) {
            Meta meta = new Meta();
            if (value == null || value.trim().isEmpty()) {
                return meta;
            }
            String[] parts = value.split("\\|", -1);
            meta.createdAtMillis = parseLong(parts, 0);
            meta.availableAtMillis = parseLong(parts, 1);
            meta.reservedUntilMillis = parseLong(parts, 2);
            meta.attempts = (int) parseLong(parts, 3);
            return meta;
        }

        private static long parseLong(String[] parts, int index) {
            if (index >= parts.length) {
                return 0;
            }
            try {
                return Long.parseLong(parts[index]);
            } catch (Exception e) {
                return 0;
            }
        }
    }
}
