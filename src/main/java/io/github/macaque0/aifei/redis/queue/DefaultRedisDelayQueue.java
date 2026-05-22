package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import io.github.macaque0.aifei.redis.script.RedisScripts;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

class DefaultRedisDelayQueue<T> extends QueueSupport<T> implements RedisDelayQueue<T>, RedisQueueMaintenance {

    DefaultRedisDelayQueue(Redis redis, QueueKeySet keys, RedisCodec<T> codec, RedisQueueOptions options) {
        super(redis, keys, codec, options);
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
        return offerAt(messageId, body, System.currentTimeMillis() + delayMillis);
    }

    @Override
    public String offerAt(T body, long timestampMillis) {
        String id = newMessageId();
        offerAt(id, body, timestampMillis);
        return id;
    }

    @Override
    public boolean offerAt(String messageId, T body, long timestampMillis) {
        final String id = requireMessageId(messageId);
        final String encoded = encode(body);
        final long now = System.currentTimeMillis();
        final String meta = meta(now, timestampMillis, 0, 0, null);
        return redis.execute(jedis -> {
            Object ret = jedis.eval(RedisScripts.OFFER_DELAY,
                    Arrays.asList(keys.delay, keys.ready, keys.payload, keys.meta),
                    Arrays.asList(id, encoded, String.valueOf(timestampMillis), meta));
            return ((Number) ret).longValue() == 1L;
        });
    }

    @Override
    public RedisMessage<T> poll() {
        maintain();
        return redis.execute(jedis -> {
            if (isPaused(jedis)) {
                return null;
            }
            Object ret = jedis.eval(RedisScripts.POLL_READY,
                    Arrays.asList(keys.ready, keys.payload, keys.meta),
                    Arrays.asList());
            RedisMessage<T> message = parsePoll(ret);
            if (message != null && options.getMessageTtlMillis() > 0 &&
                    System.currentTimeMillis() - message.getCreatedAtMillis() >= options.getMessageTtlMillis()) {
                return null;
            }
            return message;
        });
    }

    @Override
    public RedisMessage<T> poll(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis can not be negative");
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        RedisMessage<T> message;
        do {
            message = poll();
            if (message != null || timeoutMillis == 0) {
                return message;
            }
            sleep(50);
        } while (System.currentTimeMillis() < deadline);
        return null;
    }

    @Override
    public boolean cancel(String messageId) {
        final String id = requireMessageId(messageId);
        return redis.execute(jedis -> {
            Long removed = jedis.zrem(keys.delay, id);
            if (removed != null && removed > 0) {
                deletePayload(jedis, id);
                return true;
            }
            return false;
        });
    }

    @Override
    public long delayedSize() {
        return redis.execute(jedis -> jedis.zcard(keys.delay));
    }

    @Override
    public long readySize() {
        return redis.execute(jedis -> jedis.llen(keys.ready));
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
            Set<String> ids = jedis.zrangeByScore(keys.delay, 0, now, 0, options.getMaintainBatchSize());
            for (String id : ids) {
                jedis.eval(RedisScripts.PROMOTE_DELAY,
                        Arrays.asList(keys.delay, keys.ready),
                        Arrays.asList(id));
            }
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    private RedisMessage<T> parsePoll(Object ret) {
        if (ret == null) {
            return null;
        }
        List<Object> values = (List<Object>) ret;
        if (values.size() < 2) {
            return null;
        }
        String id = String.valueOf(values.get(0));
        String body = String.valueOf(values.get(1));
        String meta = values.size() > 2 ? String.valueOf(values.get(2)) : null;
        return message(id, body, meta, 0);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
