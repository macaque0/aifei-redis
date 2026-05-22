package io.github.macaque0.aifei.redis;

import cn.aifei.util.PropKit;

public class RedisConfig {

    private String host = "127.0.0.1";
    private int port = 6379;
    private String user;
    private String password;
    private int database = 0;
    private boolean ssl = false;
    private int timeoutMillis = 2000;
    private String keyPrefix = "aifei";

    private int poolMaxTotal = 32;
    private int poolMaxIdle = 16;
    private int poolMinIdle = 0;
    private boolean poolTestOnBorrow = true;

    private boolean queueEnabled = true;
    private boolean queueMaintainerEnabled = true;
    private long queueMaintainIntervalMillis = 1000;
    private int queueMaintainBatchSize = 100;
    private long queueDefaultVisibilityTimeoutMillis = 30000;
    private int queueDefaultMaxRetries = 16;
    private String queueDeadLetterSuffix = "dead";

    public static RedisConfig fromPropKit() {
        RedisConfig c = new RedisConfig();
        c.host = PropKit.get("redis.host", c.host);
        c.port = PropKit.getInt("redis.port", c.port);
        c.user = blankToNull(PropKit.get("redis.user", c.user));
        c.password = blankToNull(PropKit.get("redis.password", c.password));
        c.database = PropKit.getInt("redis.database", c.database);
        c.ssl = PropKit.getBoolean("redis.ssl", c.ssl);
        c.timeoutMillis = PropKit.getInt("redis.timeoutMillis", c.timeoutMillis);
        c.keyPrefix = PropKit.get("redis.keyPrefix", c.keyPrefix);

        c.poolMaxTotal = PropKit.getInt("redis.pool.maxTotal", c.poolMaxTotal);
        c.poolMaxIdle = PropKit.getInt("redis.pool.maxIdle", c.poolMaxIdle);
        c.poolMinIdle = PropKit.getInt("redis.pool.minIdle", c.poolMinIdle);
        c.poolTestOnBorrow = PropKit.getBoolean("redis.pool.testOnBorrow", c.poolTestOnBorrow);

        c.queueEnabled = PropKit.getBoolean("redis.queue.enabled", c.queueEnabled);
        c.queueMaintainerEnabled = PropKit.getBoolean("redis.queue.maintainerEnabled", c.queueMaintainerEnabled);
        c.queueMaintainIntervalMillis = PropKit.getLong("redis.queue.maintainIntervalMillis", c.queueMaintainIntervalMillis);
        c.queueMaintainBatchSize = PropKit.getInt("redis.queue.maintainBatchSize", c.queueMaintainBatchSize);
        c.queueDefaultVisibilityTimeoutMillis = PropKit.getLong("redis.queue.defaultVisibilityTimeoutMillis", c.queueDefaultVisibilityTimeoutMillis);
        c.queueDefaultMaxRetries = PropKit.getInt("redis.queue.defaultMaxRetries", c.queueDefaultMaxRetries);
        c.queueDeadLetterSuffix = PropKit.get("redis.queue.deadLetterSuffix", c.queueDeadLetterSuffix);
        return c;
    }

    public void validate() {
        if (!hasText(host)) {
            throw new IllegalArgumentException("redis.host can not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("redis.port must be between 1 and 65535");
        }
        if (database < 0) {
            throw new IllegalArgumentException("redis.database can not be negative");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("redis.timeoutMillis must be positive");
        }
        if (poolMaxTotal <= 0) {
            throw new IllegalArgumentException("redis.pool.maxTotal must be positive");
        }
        if (poolMaxIdle < 0 || poolMinIdle < 0) {
            throw new IllegalArgumentException("redis.pool idle values can not be negative");
        }
        if (queueMaintainIntervalMillis <= 0) {
            throw new IllegalArgumentException("redis.queue.maintainIntervalMillis must be positive");
        }
        if (queueMaintainBatchSize <= 0) {
            throw new IllegalArgumentException("redis.queue.maintainBatchSize must be positive");
        }
        if (queueDefaultVisibilityTimeoutMillis <= 0) {
            throw new IllegalArgumentException("redis.queue.defaultVisibilityTimeoutMillis must be positive");
        }
        if (queueDefaultMaxRetries < 0) {
            throw new IllegalArgumentException("redis.queue.defaultMaxRetries can not be negative");
        }
        if (!hasText(queueDeadLetterSuffix)) {
            throw new IllegalArgumentException("redis.queue.deadLetterSuffix can not be blank");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String blankToNull(String value) {
        return hasText(value) ? value.trim() : null;
    }

    public String getHost() {
        return host;
    }

    public RedisConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public RedisConfig setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUser() {
        return user;
    }

    public RedisConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public RedisConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getDatabase() {
        return database;
    }

    public RedisConfig setDatabase(int database) {
        this.database = database;
        return this;
    }

    public boolean isSsl() {
        return ssl;
    }

    public RedisConfig setSsl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public RedisConfig setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public RedisConfig setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
        return this;
    }

    public int getPoolMaxTotal() {
        return poolMaxTotal;
    }

    public RedisConfig setPoolMaxTotal(int poolMaxTotal) {
        this.poolMaxTotal = poolMaxTotal;
        return this;
    }

    public int getPoolMaxIdle() {
        return poolMaxIdle;
    }

    public RedisConfig setPoolMaxIdle(int poolMaxIdle) {
        this.poolMaxIdle = poolMaxIdle;
        return this;
    }

    public int getPoolMinIdle() {
        return poolMinIdle;
    }

    public RedisConfig setPoolMinIdle(int poolMinIdle) {
        this.poolMinIdle = poolMinIdle;
        return this;
    }

    public boolean isPoolTestOnBorrow() {
        return poolTestOnBorrow;
    }

    public RedisConfig setPoolTestOnBorrow(boolean poolTestOnBorrow) {
        this.poolTestOnBorrow = poolTestOnBorrow;
        return this;
    }

    public boolean isQueueEnabled() {
        return queueEnabled;
    }

    public RedisConfig setQueueEnabled(boolean queueEnabled) {
        this.queueEnabled = queueEnabled;
        return this;
    }

    public boolean isQueueMaintainerEnabled() {
        return queueMaintainerEnabled;
    }

    public RedisConfig setQueueMaintainerEnabled(boolean queueMaintainerEnabled) {
        this.queueMaintainerEnabled = queueMaintainerEnabled;
        return this;
    }

    public long getQueueMaintainIntervalMillis() {
        return queueMaintainIntervalMillis;
    }

    public RedisConfig setQueueMaintainIntervalMillis(long queueMaintainIntervalMillis) {
        this.queueMaintainIntervalMillis = queueMaintainIntervalMillis;
        return this;
    }

    public int getQueueMaintainBatchSize() {
        return queueMaintainBatchSize;
    }

    public RedisConfig setQueueMaintainBatchSize(int queueMaintainBatchSize) {
        this.queueMaintainBatchSize = queueMaintainBatchSize;
        return this;
    }

    public long getQueueDefaultVisibilityTimeoutMillis() {
        return queueDefaultVisibilityTimeoutMillis;
    }

    public RedisConfig setQueueDefaultVisibilityTimeoutMillis(long queueDefaultVisibilityTimeoutMillis) {
        this.queueDefaultVisibilityTimeoutMillis = queueDefaultVisibilityTimeoutMillis;
        return this;
    }

    public int getQueueDefaultMaxRetries() {
        return queueDefaultMaxRetries;
    }

    public RedisConfig setQueueDefaultMaxRetries(int queueDefaultMaxRetries) {
        this.queueDefaultMaxRetries = queueDefaultMaxRetries;
        return this;
    }

    public String getQueueDeadLetterSuffix() {
        return queueDeadLetterSuffix;
    }

    public RedisConfig setQueueDeadLetterSuffix(String queueDeadLetterSuffix) {
        this.queueDeadLetterSuffix = queueDeadLetterSuffix;
        return this;
    }
}
