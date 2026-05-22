package io.github.macaque0.aifei.redis.queue;

public interface RetryDelayPolicy {

    long nextDelayMillis(int attempts, RedisMessage<?> message);

    static RetryDelayPolicy fixed(final long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis can not be negative");
        }
        return (attempts, message) -> delayMillis;
    }

    static RetryDelayPolicy exponential(final long baseDelayMillis, final long maxDelayMillis) {
        if (baseDelayMillis < 0 || maxDelayMillis < 0) {
            throw new IllegalArgumentException("delay values can not be negative");
        }
        return (attempts, message) -> {
            long delay = baseDelayMillis;
            int times = Math.max(0, attempts - 1);
            for (int i = 0; i < times; i++) {
                if (delay >= maxDelayMillis / 2) {
                    return maxDelayMillis;
                }
                delay *= 2;
            }
            return Math.min(delay, maxDelayMillis);
        };
    }
}
