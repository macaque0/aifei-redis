package io.github.macaque0.aifei.redis.queue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RedisMessage<T> {

    private String id;
    private String queue;
    private T body;
    private Map<String, String> headers = new HashMap<>();
    private long createdAtMillis;
    private long availableAtMillis;
    private long reservedUntilMillis;
    private int attempts;

    public boolean isRedelivered() {
        return attempts > 1;
    }

    public String getId() {
        return id;
    }

    public RedisMessage<T> setId(String id) {
        this.id = id;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public RedisMessage<T> setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public T getBody() {
        return body;
    }

    public RedisMessage<T> setBody(T body) {
        this.body = body;
        return this;
    }

    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }

    public RedisMessage<T> setHeaders(Map<String, String> headers) {
        this.headers = headers == null ? new HashMap<>() : new HashMap<>(headers);
        return this;
    }

    public RedisMessage<T> addHeader(String key, String value) {
        if (key != null && value != null) {
            this.headers.put(key, value);
        }
        return this;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public RedisMessage<T> setCreatedAtMillis(long createdAtMillis) {
        this.createdAtMillis = createdAtMillis;
        return this;
    }

    public long getAvailableAtMillis() {
        return availableAtMillis;
    }

    public RedisMessage<T> setAvailableAtMillis(long availableAtMillis) {
        this.availableAtMillis = availableAtMillis;
        return this;
    }

    public long getReservedUntilMillis() {
        return reservedUntilMillis;
    }

    public RedisMessage<T> setReservedUntilMillis(long reservedUntilMillis) {
        this.reservedUntilMillis = reservedUntilMillis;
        return this;
    }

    public int getAttempts() {
        return attempts;
    }

    public RedisMessage<T> setAttempts(int attempts) {
        this.attempts = attempts;
        return this;
    }
}
