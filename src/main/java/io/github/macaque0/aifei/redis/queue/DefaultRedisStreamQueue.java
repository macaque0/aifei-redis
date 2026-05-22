package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.StreamPendingEntry;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DefaultRedisStreamQueue<T> extends QueueSupport<T> implements RedisStreamQueue<T> {

    DefaultRedisStreamQueue(Redis redis, QueueKeySet keys, RedisCodec<T> codec, RedisQueueOptions options) {
        super(redis, keys, codec, options);
    }

    @Override
    public String add(T body) {
        return add(null, body);
    }

    @Override
    public String add(String messageId, T body) {
        final String encoded = encode(body);
        final String businessId = messageId == null || messageId.trim().isEmpty() ? newMessageId() : messageId.trim();
        return redis.execute(jedis -> {
            Map<String, String> fields = new HashMap<>();
            fields.put("id", businessId);
            fields.put("body", encoded);
            fields.put("createdAt", String.valueOf(System.currentTimeMillis()));
            return jedis.xadd(keys.stream, StreamEntryID.NEW_ENTRY, fields).toString();
        });
    }

    @Override
    public void createGroup(String group) {
        final String g = requireGroup(group);
        redis.execute(jedis -> {
            try {
                jedis.xgroupCreate(keys.stream, g, new StreamEntryID("0-0"), true);
            } catch (JedisDataException e) {
                if (!e.getMessage().contains("BUSYGROUP")) {
                    throw e;
                }
            }
            return null;
        });
    }

    @Override
    public List<RedisMessage<T>> readGroup(String group, String consumer, int count, long blockMillis) {
        final String g = requireGroup(group);
        final String c = requireConsumer(consumer);
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        if (blockMillis < 0) {
            throw new IllegalArgumentException("blockMillis can not be negative");
        }
        return redis.execute(jedis -> {
            if (isPaused(jedis)) {
                return new ArrayList<>();
            }
            XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(count);
            if (blockMillis > 0) {
                params.block((int) Math.min(blockMillis, Integer.MAX_VALUE));
            }
            Map<String, StreamEntryID> streams = new HashMap<>();
            streams.put(keys.stream, StreamEntryID.UNRECEIVED_ENTRY);
            List<Map.Entry<String, List<StreamEntry>>> ret = jedis.xreadGroup(g, c, params, streams);
            return parseEntries(ret);
        });
    }

    @Override
    public void ack(String group, String... messageIds) {
        final String g = requireGroup(group);
        if (messageIds == null || messageIds.length == 0) {
            return;
        }
        final StreamEntryID[] ids = toStreamIds(messageIds);
        redis.execute(jedis -> jedis.xack(keys.stream, g, ids));
    }

    @Override
    public List<RedisMessage<T>> pending(String group, String consumer, int count) {
        final String g = requireGroup(group);
        final String c = requireConsumer(consumer);
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        return redis.execute(jedis -> {
            XPendingParams params = XPendingParams.xPendingParams()
                    .start(new StreamEntryID("0-0"))
                    .end(new StreamEntryID(Long.MAX_VALUE, Long.MAX_VALUE))
                    .count(count)
                    .consumer(c);
            List<StreamPendingEntry> pending = jedis.xpending(keys.stream, g, params);
            List<RedisMessage<T>> messages = new ArrayList<>();
            for (StreamPendingEntry entry : pending) {
                List<StreamEntry> streamEntries = jedis.xrange(keys.stream, entry.getID(), entry.getID(), 1);
                if (!streamEntries.isEmpty()) {
                    RedisMessage<T> message = parseEntry(streamEntries.get(0));
                    message.setAttempts((int) entry.getDeliveredTimes());
                    messages.add(message);
                }
            }
            return messages;
        });
    }

    @Override
    public List<RedisMessage<T>> claimIdle(String group, String consumer, long minIdleMillis, int count) {
        final String g = requireGroup(group);
        final String c = requireConsumer(consumer);
        if (minIdleMillis < 0) {
            throw new IllegalArgumentException("minIdleMillis can not be negative");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        return redis.execute(jedis -> {
            XPendingParams params = XPendingParams.xPendingParams()
                    .idle(minIdleMillis)
                    .start(new StreamEntryID("0-0"))
                    .end(new StreamEntryID(Long.MAX_VALUE, Long.MAX_VALUE))
                    .count(count);
            List<StreamPendingEntry> pending = jedis.xpending(keys.stream, g, params);
            List<StreamEntryID> claimIds = new ArrayList<>();
            for (StreamPendingEntry entry : pending) {
                if (entry.getIdleTime() >= minIdleMillis) {
                    claimIds.add(entry.getID());
                }
            }
            if (claimIds.isEmpty()) {
                return new ArrayList<>();
            }
            StreamEntryID[] ids = claimIds.toArray(new StreamEntryID[0]);
            List<StreamEntry> entries = jedis.xclaim(keys.stream, g, c, minIdleMillis, 0, 0, false, ids);
            return parseStreamEntries(entries);
        });
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

    private List<RedisMessage<T>> parseEntries(List<Map.Entry<String, List<StreamEntry>>> ret) {
        List<RedisMessage<T>> messages = new ArrayList<>();
        if (ret == null) {
            return messages;
        }
        for (Map.Entry<String, List<StreamEntry>> stream : ret) {
            messages.addAll(parseStreamEntries(stream.getValue()));
        }
        return messages;
    }

    private List<RedisMessage<T>> parseStreamEntries(List<StreamEntry> entries) {
        List<RedisMessage<T>> messages = new ArrayList<>();
        if (entries == null) {
            return messages;
        }
        for (StreamEntry entry : entries) {
            messages.add(parseEntry(entry));
        }
        return messages;
    }

    private RedisMessage<T> parseEntry(StreamEntry entry) {
        Map<String, String> fields = entry.getFields();
        String body = fields.get("body");
        long createdAt = parseLong(fields.get("createdAt"));
        return new RedisMessage<T>()
                .setId(entry.getID().toString())
                .setQueue(keys.name)
                .setBody(decode(body))
                .setCreatedAtMillis(createdAt)
                .setAvailableAtMillis(createdAt)
                .addHeader("businessId", fields.get("id"));
    }

    private static StreamEntryID[] toStreamIds(String[] messageIds) {
        StreamEntryID[] ret = new StreamEntryID[messageIds.length];
        for (int i = 0; i < messageIds.length; i++) {
            ret[i] = new StreamEntryID(messageIds[i]);
        }
        return ret;
    }

    private static String requireGroup(String group) {
        if (group == null || group.trim().isEmpty()) {
            throw new IllegalArgumentException("group can not be blank");
        }
        return group.trim();
    }

    private static String requireConsumer(String consumer) {
        if (consumer == null || consumer.trim().isEmpty()) {
            throw new IllegalArgumentException("consumer can not be blank");
        }
        return consumer.trim();
    }

    private static long parseLong(String value) {
        try {
            return value == null ? 0 : Long.parseLong(value);
        } catch (Exception e) {
            return 0;
        }
    }
}
