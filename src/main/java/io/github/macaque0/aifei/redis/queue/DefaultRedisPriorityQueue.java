package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.RedisException;
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
            Object ret = jedis.eval(RedisScripts.PRIORITY_OFFER, priorityKeys(),
                    Arrays.asList(id, encoded, meta, String.valueOf(score), String.valueOf(priority),
                            String.valueOf(options.getMaxLength()), options.getFullQueuePolicy().name()));
            long value = asLong(ret);
            if (value < 0) {
                throw new RedisException("Queue is full: " + keys.name);
            }
            return value == 1L;
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
        redis.execute(jedis -> jedis.eval(RedisScripts.PRIORITY_ACK, priorityKeys(), Arrays.asList(id)));
    }

    @Override
    public void nack(String messageId) {
        final String id = requireMessageId(messageId);
        redis.execute(jedis -> {
            retryNow(jedis, id, System.currentTimeMillis());
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
        redis.execute(jedis -> jedis.eval(RedisScripts.PRIORITY_RETRY_LATER, priorityKeys(),
                Arrays.asList(id, String.valueOf(availableAt))));
    }

    @Override
    public void dead(String messageId, String reason) {
        final String id = requireMessageId(messageId);
        final long now = System.currentTimeMillis();
        redis.execute(jedis -> jedis.eval(RedisScripts.PRIORITY_DEAD, priorityKeys(),
                Arrays.asList(id, String.valueOf(now), "dead:" + safe(reason))));
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
                jedis.eval(RedisScripts.PRIORITY_PROMOTE_DELAY, priorityKeys(),
                        Arrays.asList(id, String.valueOf(now), String.valueOf(PRIORITY_FACTOR)));
            }

            Set<String> expired = jedis.zrangeByScore(keys.reserved, 0, now, 0, options.getMaintainBatchSize());
            for (String id : expired) {
                String attemptsValue = jedis.hget(keys.attempts, id);
                int attempts = parseInt(attemptsValue, 0);
                String meta = jedis.hget(keys.meta, id);
                if (expired(meta, now) || attempts >= options.getMaxRetries()) {
                    jedis.eval(RedisScripts.PRIORITY_DEAD, priorityKeys(),
                            Arrays.asList(id, String.valueOf(now), expired(meta, now) ? "expired" : "maxRetries"));
                } else {
                    long delay = options.getRetryDelayPolicy().nextDelayMillis(attempts, readMessage(jedis, id));
                    if (delay <= 0) {
                        retryNow(jedis, id, now);
                    } else {
                        jedis.eval(RedisScripts.PRIORITY_RETRY_LATER, priorityKeys(),
                                Arrays.asList(id, String.valueOf(now + delay)));
                    }
                }
            }
            return null;
        });
    }

    private RedisMessage<T> reserveOne(String consumerId) {
        Object ret = redis.execute(jedis -> {
            if (isPaused(jedis)) {
                return null;
            }
            long now = System.currentTimeMillis();
            long reservedUntil = now + options.getVisibilityTimeoutMillis();
            return jedis.eval(RedisScripts.PRIORITY_RESERVE, priorityKeys(),
                    Arrays.asList(String.valueOf(reservedUntil), consumerId,
                            String.valueOf(options.getMaintainBatchSize()), String.valueOf(now),
                            String.valueOf(options.getMessageTtlMillis())));
        });
        if (ret == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) ret;
        String id = String.valueOf(values.get(0));
        String body = String.valueOf(values.get(1));
        int attempts = ((Number) values.get(2)).intValue();
        String meta = String.valueOf(values.get(3));
        return message(id, body, meta, attempts);
    }

    private void retryNow(redis.clients.jedis.Jedis jedis, String id, long now) {
        jedis.eval(RedisScripts.PRIORITY_RETRY_NOW, priorityKeys(),
                Arrays.asList(id, String.valueOf(now), String.valueOf(PRIORITY_FACTOR)));
    }

    private List<String> priorityKeys() {
        return Arrays.asList(keys.priority, keys.reserved, keys.payload, keys.meta,
                keys.attempts, keys.delay, keys.dead, keys.priorityValue);
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

    private static long asLong(Object value) {
        return ((Number) value).longValue();
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return value == null ? defaultValue : Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
