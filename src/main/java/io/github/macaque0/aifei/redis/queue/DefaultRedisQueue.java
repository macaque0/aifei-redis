package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import io.github.macaque0.aifei.redis.script.RedisScripts;

import java.util.Arrays;
import java.util.List;

class DefaultRedisQueue<T> extends QueueSupport<T> implements RedisQueue<T> {

    DefaultRedisQueue(Redis redis, QueueKeySet keys, RedisCodec<T> codec, RedisQueueOptions options) {
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
        final String id = requireMessageId(messageId);
        final String encoded = encode(body);
        final long now = System.currentTimeMillis();
        final String meta = meta(now, now, 0, 0, null);
        return redis.execute(jedis -> {
            if (options.getMaxLength() > 0 && jedis.llen(keys.ready) >= options.getMaxLength()) {
                if (options.getFullQueuePolicy() == FullQueuePolicy.DROP_NEWEST) {
                    return false;
                }
                enforceCapacity(jedis);
            }
            Object ret = jedis.eval(RedisScripts.OFFER_READY,
                    Arrays.asList(keys.ready, keys.payload, keys.meta),
                    Arrays.asList(id, encoded, meta));
            return asLong(ret) == 1L;
        });
    }

    @Override
    public void offerBatch(List<T> bodies) {
        if (bodies == null) {
            throw new IllegalArgumentException("bodies can not be null");
        }
        for (T body : bodies) {
            offer(body);
        }
    }

    @Override
    public RedisMessage<T> poll() {
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
    public long size() {
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

    private static long asLong(Object value) {
        return ((Number) value).longValue();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
