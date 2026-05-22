package io.github.macaque0.aifei.redis.queue;

public class RedisQueueStats {

    private String queue;
    private long readySize;
    private long delayedSize;
    private long reservedSize;
    private long deadSize;
    private long prioritySize;
    private long streamLength;
    private boolean paused;

    public String getQueue() {
        return queue;
    }

    public RedisQueueStats setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public long getReadySize() {
        return readySize;
    }

    public RedisQueueStats setReadySize(long readySize) {
        this.readySize = readySize;
        return this;
    }

    public long getDelayedSize() {
        return delayedSize;
    }

    public RedisQueueStats setDelayedSize(long delayedSize) {
        this.delayedSize = delayedSize;
        return this;
    }

    public long getReservedSize() {
        return reservedSize;
    }

    public RedisQueueStats setReservedSize(long reservedSize) {
        this.reservedSize = reservedSize;
        return this;
    }

    public long getDeadSize() {
        return deadSize;
    }

    public RedisQueueStats setDeadSize(long deadSize) {
        this.deadSize = deadSize;
        return this;
    }

    public long getPrioritySize() {
        return prioritySize;
    }

    public RedisQueueStats setPrioritySize(long prioritySize) {
        this.prioritySize = prioritySize;
        return this;
    }

    public long getStreamLength() {
        return streamLength;
    }

    public RedisQueueStats setStreamLength(long streamLength) {
        this.streamLength = streamLength;
        return this;
    }

    public boolean isPaused() {
        return paused;
    }

    public RedisQueueStats setPaused(boolean paused) {
        this.paused = paused;
        return this;
    }
}
