package io.github.macaque0.aifei.redis.queue;

public class RedisQueueOptions {

    private long visibilityTimeoutMillis = 30000;
    private int maxRetries = 16;
    private int maintainBatchSize = 100;
    private long messageTtlMillis = 0;
    private long maxLength = 0;
    private FullQueuePolicy fullQueuePolicy = FullQueuePolicy.REJECT;
    private RetryDelayPolicy retryDelayPolicy = RetryDelayPolicy.fixed(1000);

    public long getVisibilityTimeoutMillis() {
        return visibilityTimeoutMillis;
    }

    public RedisQueueOptions setVisibilityTimeoutMillis(long visibilityTimeoutMillis) {
        if (visibilityTimeoutMillis <= 0) {
            throw new IllegalArgumentException("visibilityTimeoutMillis must be positive");
        }
        this.visibilityTimeoutMillis = visibilityTimeoutMillis;
        return this;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public RedisQueueOptions setMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries can not be negative");
        }
        this.maxRetries = maxRetries;
        return this;
    }

    public int getMaintainBatchSize() {
        return maintainBatchSize;
    }

    public RedisQueueOptions setMaintainBatchSize(int maintainBatchSize) {
        if (maintainBatchSize <= 0) {
            throw new IllegalArgumentException("maintainBatchSize must be positive");
        }
        this.maintainBatchSize = maintainBatchSize;
        return this;
    }

    public long getMessageTtlMillis() {
        return messageTtlMillis;
    }

    public RedisQueueOptions setMessageTtlMillis(long messageTtlMillis) {
        if (messageTtlMillis < 0) {
            throw new IllegalArgumentException("messageTtlMillis can not be negative");
        }
        this.messageTtlMillis = messageTtlMillis;
        return this;
    }

    public long getMaxLength() {
        return maxLength;
    }

    public RedisQueueOptions setMaxLength(long maxLength) {
        if (maxLength < 0) {
            throw new IllegalArgumentException("maxLength can not be negative");
        }
        this.maxLength = maxLength;
        return this;
    }

    public FullQueuePolicy getFullQueuePolicy() {
        return fullQueuePolicy;
    }

    public RedisQueueOptions setFullQueuePolicy(FullQueuePolicy fullQueuePolicy) {
        if (fullQueuePolicy == null) {
            throw new IllegalArgumentException("fullQueuePolicy can not be null");
        }
        this.fullQueuePolicy = fullQueuePolicy;
        return this;
    }

    public RetryDelayPolicy getRetryDelayPolicy() {
        return retryDelayPolicy;
    }

    public RedisQueueOptions setRetryDelayPolicy(RetryDelayPolicy retryDelayPolicy) {
        if (retryDelayPolicy == null) {
            throw new IllegalArgumentException("retryDelayPolicy can not be null");
        }
        this.retryDelayPolicy = retryDelayPolicy;
        return this;
    }
}
