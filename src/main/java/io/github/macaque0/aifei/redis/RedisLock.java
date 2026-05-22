package io.github.macaque0.aifei.redis;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis 分布式锁句柄。
 *
 * <p>锁通过唯一 token 标识持有者，释放和续期都会校验 token，避免误删其他线程或其他机器重新获取到的锁。
 * 推荐使用 try-with-resources 自动释放：</p>
 *
 * <pre>{@code
 * try (RedisLock lock = RedisKit.tryLock(RedisKit.key("lock:job"), 30000)) {
 *     if (lock == null) {
 *         return;
 *     }
 *     // do business
 * }
 * }</pre>
 */
public class RedisLock implements AutoCloseable {

    private final Redis redis;
    private final String key;
    private final String token;
    private final long expireMillis;
    private final long lockedAtMillis;
    private final AtomicBoolean released = new AtomicBoolean(false);

    RedisLock(Redis redis, String key, String token, long expireMillis, long lockedAtMillis) {
        this.redis = redis;
        this.key = key;
        this.token = token;
        this.expireMillis = expireMillis;
        this.lockedAtMillis = lockedAtMillis;
    }

    public String getKey() {
        return key;
    }

    public String getToken() {
        return token;
    }

    public long getExpireMillis() {
        return expireMillis;
    }

    public long getLockedAtMillis() {
        return lockedAtMillis;
    }

    public boolean isReleased() {
        return released.get();
    }

    /**
     * 续期当前锁。
     *
     * <p>只有 Redis 中的 token 仍然等于当前句柄 token 时才会成功。</p>
     */
    public boolean renew(long expireMillis) {
        if (released.get()) {
            return false;
        }
        return redis.renewLock(key, token, expireMillis);
    }

    /**
     * 主动释放当前锁。
     *
     * <p>重复调用只会第一次真正访问 Redis，后续返回 false。</p>
     */
    public boolean unlock() {
        if (!released.compareAndSet(false, true)) {
            return false;
        }
        try {
            return redis.unlock(key, token);
        } catch (RuntimeException e) {
            released.set(false);
            throw e;
        }
    }

    @Override
    public void close() {
        unlock();
    }
}
