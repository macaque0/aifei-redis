package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import io.github.macaque0.aifei.redis.script.RedisScripts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class DefaultRedisPriorityQueue<T> extends QueueSupport<T> implements RedisPriorityQueue<T>, RedisQueueMaintenance {

    private static final long PRIORITY_FACTOR = 1000000000000L;

    DefaultRedisPriorityQueue(Redis redis, QueueKeySet keys, RedisCodec<T> codec, RedisQueueOptions options) {
        super(redis, keys, codec, options);
    }

    @Override
    public String offer(T body, int priority) {
        String id = newMessageId();
        offer(id, body, priority);
        return id;
    }

    @Override
    public boolean offer(String messageId, T body, int priority) {
        final String id = requireMessageId(messageId);
        final String encoded = encode(body);
        final long now = System.currentTimeMillis();
        final String meta = meta(now, now, 0, 0, null);
        final double score = priorityScore(priority, now);
        return redis.execute(jedis -> {
            if (options.getMaxLength() > 0 && jedis.zcard(keys.priority) >= options.getMaxLength()) {
                if (options.getFullQueuePolicy() == FullQueuePolicy.DROP_NEWEST) {
                    return false;
                }
                if (options.getFullQueuePolicy() == FullQueuePolicy.REJECT) {
                    throw new io.github.macaque0.aifei.redis.RedisException("Queue is full: " + keys.name);
                }
                Set<String> oldest = jedis.zrevrange(keys.priority, 0, 0);
                for (String old : oldest) {
                    deletePayload(jedis, old);
                }
            }
            if (jedis.hexists(keys.payload, id)) {
                return false;
            }
            jedis.hset(keys.payload, id, encoded);
            jedis.hset(keys.meta, id, meta);
            jedis.hset(keys.priorityValue, id, String.valueOf(priority));
            jedis.zadd(keys.priority, score, id);
            return true;
        });
    }

    @Override
    public RedisMessage<T> reserve(String consumerId) {
        return reserve(consumerId, 0);
    }

    @Override
    public RedisMessage<T> reserve(String consumerId, long timeoutMillis) {
        List<RedisMessage<T>> messages = reserveBatch(consumerId, 1, timeoutMillis);
        return messages.isEmpty() ? null : messages.get(0);
    }

    @Override
    public List<RedisMessage<T>> reserveBatch(String consumerId, int count, long timeoutMillis) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis can not be negative");
        }
        String cid = requireConsumerId(consumerId);
        long deadline = System.currentTimeMillis() + timeoutMillis;
        List<RedisMessage<T>> messages = new ArrayList<>();
        do {
            maintain();
            while (messages.size() < count) {
                RedisMessage<T> message = reserveOne(cid);
                if (message == null) {
                    break;
                }
                messages.add(message);
            }
            if (!messages.isEmpty() || timeoutMillis == 0) {
                return messages;
            }
            sleep(50);
        } while (System.currentTimeMillis() < deadline);
        return messages;
    }

    @Override
    public void ack(String messageId) {
        final String id = requireMessageId(messageId);
        redis.execute(jedis -> {
            deletePayload(jedis, id);
            return null;
        });
    }

    @Override
    public void nack(String messageId) {
        final String id = requireMessageId(messageId);
        redis.execute(jedis -> {
            retryNow(jedis, id);
            return null;
        });
    }

    @Override
    public void retryLater(String messageId, long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis can not be negative");
        }
        final String id = requireMessageId(messageId);
        final long availableAt = System.currentTimeMillis() + delayMillis;
        redis.execute(jedis -> {
            if (jedis.hexists(keys.payload, id)) {
                jedis.zrem(keys.reserved, id);
                jedis.zadd(keys.delay, availableAt, id);
            }
            return null;
        });
    }

    @Override
    public void dead(String messageId, String reason) {
        final String id = requireMessageId(messageId);
        final long now = System.currentTimeMillis();
        redis.execute(jedis -> {
            jedis.zrem(keys.priority, id);
            jedis.eval(RedisScripts.DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(now), "dead:" + safe(reason)));
            return null;
        });
    }

    @Override
    public long readySize() {
        return redis.execute(jedis -> jedis.zcard(keys.priority));
    }

    @Override
    public long reservedSize() {
        return redis.execute(jedis -> jedis.zcard(keys.reserved));
    }

    @Override
    public long deadSize() {
        return redis.execute(jedis -> jedis.zcard(keys.dead));
    }

    @Override
    public void pause() {
        pauseQueue();
    }

    @Override
    public void resume() {
        resumeQueue();
    }

    @Override
    public boolean isPaused() {
        return super.isPaused();
    }

    @Override
    public RedisQueueStats stats() {
        return super.stats();
    }

    @Override
    public void maintain() {
        final long now = System.currentTimeMillis();
        redis.execute(jedis -> {
            Set<String> delayed = jedis.zrangeByScore(keys.delay, 0, now, 0, options.getMaintainBatchSize());
            for (String id : delayed) {
                if (jedis.zrem(keys.delay, id) == 1) {
                    addReady(jedis, id, now);
                }
            }

            Set<String> expired = jedis.zrangeByScore(keys.reserved, 0, now, 0, options.getMaintainBatchSize());
            for (String id : expired) {
                String attemptsValue = jedis.hget(keys.attempts, id);
                int attempts = attemptsValue == null ? 0 : Integer.parseInt(attemptsValue);
                String meta = jedis.hget(keys.meta, id);
                if (expired(meta, now) || attempts >= options.getMaxRetries()) {
                    jedis.eval(RedisScripts.DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(now), expired(meta, now) ? "expired" : "maxRetries"));
                } else {
                    long delay = options.getRetryDelayPolicy().nextDelayMillis(attempts, readMessage(jedis, id));
                    if (delay <= 0) {
                        retryNow(jedis, id);
                    } else {
                        jedis.zrem(keys.reserved, id);
                        jedis.zadd(keys.delay, now + delay, id);
                    }
                }
            }
            return null;
        });
    }

    private RedisMessage<T> reserveOne(String consumerId) {
        return redis.execute(jedis -> {
            if (isPaused(jedis)) {
                return null;
            }
            for (int i = 0; i < options.getMaintainBatchSize(); i++) {
                Set<String> ids = jedis.zrange(keys.priority, 0, 0);
                if (ids.isEmpty()) {
                    return null;
                }
                String id = ids.iterator().next();
                if (jedis.zrem(keys.priority, id) != 1) {
                    continue;
                }
                String body = jedis.hget(keys.payload, id);
                if (body == null) {
                    deletePayload(jedis, id);
                    continue;
                }
                String oldMeta = jedis.hget(keys.meta, id);
                long now = System.currentTimeMillis();
                if (expired(oldMeta, now)) {
                    jedis.zadd(keys.dead, now, id);
                    jedis.hset(keys.meta, id, oldMeta == null ? "0|0|0|0|expired" : oldMeta + "|expired");
                    continue;
                }
                int attempts = Math.toIntExact(jedis.hincrBy(keys.attempts, id, 1));
                long reservedUntil = now + options.getVisibilityTimeoutMillis();
                String meta = preserveMeta(oldMeta, reservedUntil, attempts, consumerId);
                jedis.hset(keys.meta, id, meta);
                jedis.zadd(keys.reserved, reservedUntil, id);
                return message(id, body, meta, attempts);
            }
            return null;
        });
    }

    private void retryNow(redis.clients.jedis.Jedis jedis, String id) {
        if (jedis.hexists(keys.payload, id)) {
            jedis.zrem(keys.reserved, id);
            addReady(jedis, id, System.currentTimeMillis());
        }
    }

    private void addReady(redis.clients.jedis.Jedis jedis, String id, long now) {
        String priorityValue = jedis.hget(keys.priorityValue, id);
        int priority = priorityValue == null ? 0 : Integer.parseInt(priorityValue);
        jedis.zadd(keys.priority, priorityScore(priority, now), id);
    }

    private String preserveMeta(String oldMeta, long reservedUntil, int attempts, String consumerId) {
        Meta meta = Meta.parse(oldMeta);
        return meta(meta.createdAtMillis, meta.availableAtMillis, reservedUntil, attempts, consumerId);
    }

    private static double priorityScore(int priority, long now) {
        long sequence = now % PRIORITY_FACTOR;
        return (double) (-1L * priority * PRIORITY_FACTOR + sequence);
    }

    private static String requireConsumerId(String consumerId) {
        if (consumerId == null || consumerId.trim().isEmpty()) {
            throw new IllegalArgumentException("consumerId can not be blank");
        }
        return consumerId.trim();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value.replace("|", "_");
    }
}
