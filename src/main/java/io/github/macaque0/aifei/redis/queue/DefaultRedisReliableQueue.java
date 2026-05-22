package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.RedisException;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import io.github.macaque0.aifei.redis.script.RedisScripts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class DefaultRedisReliableQueue<T> extends QueueSupport<T> implements RedisReliableQueue<T>, RedisQueueMaintenance {

    DefaultRedisReliableQueue(Redis redis, QueueKeySet keys, RedisCodec<T> codec, RedisQueueOptions options) {
        super(redis, keys, codec, options);
    }

    @Override
    public String offer(T body) {
        String id = newMessageId();
        offer(id, body);
        return id;
    }

    @Override
    public boolean offer(String messageId, T body) {
        return offerNow(messageId, body);
    }

    @Override
    public String offer(T body, long delayMillis) {
        String id = newMessageId();
        offer(id, body, delayMillis);
        return id;
    }

    @Override
    public boolean offer(String messageId, T body, long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis can not be negative");
        }
        if (delayMillis == 0) {
            return offerNow(messageId, body);
        }
        final String id = requireMessageId(messageId);
        final String encoded = encode(body);
        final long now = System.currentTimeMillis();
        final long availableAt = now + delayMillis;
        final String meta = meta(now, availableAt, 0, 0, null);
        return redis.execute(jedis -> {
            Object ret = jedis.eval(RedisScripts.OFFER_DELAY,
                    Arrays.asList(keys.delay, keys.ready, keys.payload, keys.meta),
                    Arrays.asList(id, encoded, String.valueOf(availableAt), meta));
            return ((Number) ret).longValue() == 1L;
        });
    }

    @Override
    public void offerBatch(List<T> bodies) {
        if (bodies == null) {
            throw new IllegalArgumentException("bodies can not be null");
        }
        if (bodies.isEmpty()) {
            return;
        }
        List<String> args = batchOfferArgs(bodies);
        redis.execute(jedis -> {
            Object ret = jedis.eval(RedisScripts.OFFER_READY_BATCH, keys.commonKeys(), args);
            if (((Number) ret).longValue() < 0) {
                throw new RedisException("Queue is full: " + keys.name);
            }
            return null;
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
            messages.addAll(reserveAvailableBatch(cid, count - messages.size()));
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
        redis.execute(jedis -> jedis.eval(RedisScripts.ACK, keys.commonKeys(), Arrays.asList(id)));
    }

    @Override
    public void ackBatch(String... messageIds) {
        if (messageIds == null) {
            return;
        }
        if (messageIds.length == 0) {
            return;
        }
        List<String> ids = new ArrayList<>(messageIds.length);
        for (String messageId : messageIds) {
            ids.add(requireMessageId(messageId));
        }
        redis.execute(jedis -> jedis.eval(RedisScripts.ACK_BATCH, keys.commonKeys(), ids));
    }

    @Override
    public void nack(String messageId) {
        final String id = requireMessageId(messageId);
        redis.execute(jedis -> jedis.eval(RedisScripts.RETRY_NOW, keys.commonKeys(), Arrays.asList(id)));
    }

    @Override
    public void retryLater(String messageId, long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis can not be negative");
        }
        final String id = requireMessageId(messageId);
        final long availableAt = System.currentTimeMillis() + delayMillis;
        redis.execute(jedis -> jedis.eval(RedisScripts.RETRY_LATER, keys.commonKeys(), Arrays.asList(id, String.valueOf(availableAt))));
    }

    @Override
    public void dead(String messageId, String reason) {
        final String id = requireMessageId(messageId);
        final long now = System.currentTimeMillis();
        redis.execute(jedis -> jedis.eval(RedisScripts.DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(now), "dead:" + safe(reason))));
    }

    @Override
    public boolean replayDead(String messageId) {
        final String id = requireMessageId(messageId);
        final long now = System.currentTimeMillis();
        return redis.execute(jedis -> {
            Object ret = jedis.eval(RedisScripts.REPLAY_DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(now)));
            return ((Number) ret).longValue() == 1L;
        });
    }

    @Override
    public int replayDeadBatch(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        return redis.execute(jedis -> {
            Set<String> ids = jedis.zrange(keys.dead, 0, count - 1);
            int replayed = 0;
            for (String id : ids) {
                Object ret = jedis.eval(RedisScripts.REPLAY_DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(System.currentTimeMillis())));
                if (((Number) ret).longValue() == 1L) {
                    replayed++;
                }
            }
            return replayed;
        });
    }

    @Override
    public long readySize() {
        return redis.execute(jedis -> jedis.llen(keys.ready));
    }

    @Override
    public long delayedSize() {
        return redis.execute(jedis -> jedis.zcard(keys.delay));
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
            promoteDelay(jedis, now);
            restoreExpiredReserved(jedis, now);
            return null;
        });
    }

    private boolean offerNow(String messageId, T body) {
        final String id = requireMessageId(messageId);
        final String encoded = encode(body);
        final long now = System.currentTimeMillis();
        final String meta = meta(now, now, 0, 0, null);
        return redis.execute(jedis -> {
            Object ret = jedis.eval(RedisScripts.OFFER_READY_LIMITED,
                    keys.commonKeys(),
                    Arrays.asList(id, encoded, meta, String.valueOf(options.getMaxLength()),
                            options.getFullQueuePolicy().name()));
            long value = ((Number) ret).longValue();
            if (value < 0) {
                throw new RedisException("Queue is full: " + keys.name);
            }
            return value == 1L;
        });
    }

    private List<String> batchOfferArgs(List<T> bodies) {
        List<String> args = new ArrayList<>(2 + bodies.size() * 3);
        args.add(String.valueOf(options.getMaxLength()));
        args.add(options.getFullQueuePolicy().name());
        long now = System.currentTimeMillis();
        for (T body : bodies) {
            String id = newMessageId();
            args.add(id);
            args.add(encode(body));
            args.add(meta(now, now, 0, 0, null));
        }
        return args;
    }

    @SuppressWarnings("unchecked")
    private RedisMessage<T> reserveOne(String consumerId) {
        long now = System.currentTimeMillis();
        long reservedUntil = now + options.getVisibilityTimeoutMillis();
        Object ret = redis.execute(jedis -> {
            if (isPaused(jedis)) {
                return null;
            }
            return jedis.eval(RedisScripts.RESERVE,
                    Arrays.asList(keys.ready, keys.reserved, keys.payload, keys.meta, keys.attempts, keys.dead),
                    Arrays.asList(String.valueOf(reservedUntil), consumerId, String.valueOf(options.getMaintainBatchSize()),
                            String.valueOf(now), String.valueOf(options.getMessageTtlMillis())));
        });
        if (ret == null) {
            return null;
        }
        List<Object> values = (List<Object>) ret;
        String id = String.valueOf(values.get(0));
        String body = String.valueOf(values.get(1));
        int attempts = ((Number) values.get(2)).intValue();
        String meta = values.size() > 3 ? String.valueOf(values.get(3)) : meta(now, now, reservedUntil, attempts, consumerId);
        return message(id, body, meta, attempts);
    }

    @SuppressWarnings("unchecked")
    private List<RedisMessage<T>> reserveAvailableBatch(String consumerId, int count) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        long now = System.currentTimeMillis();
        long reservedUntil = now + options.getVisibilityTimeoutMillis();
        int maxScan = Math.max(options.getMaintainBatchSize(), count);
        Object ret = redis.execute(jedis -> {
            if (isPaused(jedis)) {
                return null;
            }
            return jedis.eval(RedisScripts.RESERVE_BATCH,
                    Arrays.asList(keys.ready, keys.reserved, keys.payload, keys.meta, keys.attempts, keys.dead),
                    Arrays.asList(String.valueOf(reservedUntil), consumerId, String.valueOf(maxScan),
                            String.valueOf(now), String.valueOf(options.getMessageTtlMillis()), String.valueOf(count)));
        });
        List<RedisMessage<T>> messages = new ArrayList<>();
        if (ret == null) {
            return messages;
        }
        List<Object> values = (List<Object>) ret;
        for (int i = 0; i + 3 < values.size(); i += 4) {
            String id = String.valueOf(values.get(i));
            String body = String.valueOf(values.get(i + 1));
            int attempts = ((Number) values.get(i + 2)).intValue();
            String meta = String.valueOf(values.get(i + 3));
            messages.add(message(id, body, meta, attempts));
        }
        return messages;
    }

    private void promoteDelay(redis.clients.jedis.Jedis jedis, long now) {
        Set<String> ids = jedis.zrangeByScore(keys.delay, 0, now, 0, options.getMaintainBatchSize());
        for (String id : ids) {
            String meta = jedis.hget(keys.meta, id);
            if (expired(meta, now)) {
                jedis.eval(RedisScripts.DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(now), "expired"));
            } else {
                jedis.eval(RedisScripts.PROMOTE_DELAY, Arrays.asList(keys.delay, keys.ready), Arrays.asList(id));
            }
        }
    }

    private void restoreExpiredReserved(redis.clients.jedis.Jedis jedis, long now) {
        Set<String> ids = jedis.zrangeByScore(keys.reserved, 0, now, 0, options.getMaintainBatchSize());
        for (String id : ids) {
            String attemptsValue = jedis.hget(keys.attempts, id);
            int attempts = attemptsValue == null ? 0 : Integer.parseInt(attemptsValue);
            String meta = jedis.hget(keys.meta, id);
            if (expired(meta, now) || attempts >= options.getMaxRetries()) {
                jedis.eval(RedisScripts.DEAD, keys.commonKeys(), Arrays.asList(id, String.valueOf(now), expired(meta, now) ? "expired" : "maxRetries"));
            } else {
                long delay = options.getRetryDelayPolicy().nextDelayMillis(attempts, readMessage(jedis, id));
                if (delay <= 0) {
                    jedis.eval(RedisScripts.RETRY_NOW, keys.commonKeys(), Arrays.asList(id));
                } else {
                    jedis.eval(RedisScripts.RETRY_LATER, keys.commonKeys(), Arrays.asList(id, String.valueOf(now + delay)));
                }
            }
        }
    }

    private static String requireConsumerId(String consumerId) {
        if (consumerId == null || consumerId.trim().isEmpty()) {
            throw new IllegalArgumentException("consumerId can not be blank");
        }
        return consumerId.trim();
    }

    private static String safe(String value) {
        return value == null ? "" : value.replace("|", "_");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
