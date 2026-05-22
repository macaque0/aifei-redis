package io.github.macaque0.aifei.redis.queue;

public enum FullQueuePolicy {
    REJECT,
    DROP_OLDEST,
    DROP_NEWEST
}
