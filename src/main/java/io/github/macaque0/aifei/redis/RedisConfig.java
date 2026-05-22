package io.github.macaque0.aifei.redis;

import cn.aifei.util.PropKit;

/**
 * Redis 插件的集中配置。
 *
 * <p>这里的默认值以“开箱可用的小中型业务系统”为目标：连接池适中、队列能力默认开启、
 * 消费日志默认关闭，避免在未显式配置时产生过多日志或泄露消息正文。</p>
 */
public class RedisConfig {

    // Redis 连接配置。
    private String host = "127.0.0.1";
    private int port = 6379;
    // Redis ACL 用户名，Redis 6.0+ 才支持；Redis 5.x 请保持为空。
    private String user;
    private String password;
    private int database = 0;
    private boolean ssl = false;
    private int timeoutMillis = 2000;
    // 队列等插件管理的 key 会自动加此前缀；普通 Redis API 可通过 RedisKit.key(...) 手动生成前缀 key。
    private String keyPrefix = "aifei";

    // Jedis 连接池配置。
    private int poolMaxTotal = 32;
    private int poolMaxIdle = 16;
    private int poolMinIdle = 0;
    private boolean poolTestOnBorrow = true;

    // 队列全局配置。
    private boolean queueEnabled = true;
    // 维护线程负责延迟消息提升、reserved 超时重试、TTL 死信等后台清理动作。
    private boolean queueMaintainerEnabled = true;
    private long queueMaintainIntervalMillis = 1000;
    private int queueMaintainBatchSize = 100;
    // 可靠队列 reserve 后的默认不可见时间，超时未 ack 会被重新投递或进入死信。
    private long queueDefaultVisibilityTimeoutMillis = 30000;
    private int queueDefaultMaxRetries = 16;
    private String queueDeadLetterSuffix = "dead";
    // 注解监听和消费日志配置。
    private boolean queueListenerEnabled = true;
    private String queueListenerPackages;
    private boolean queueMessageLogEnabled = false;
    // 默认不记录 body，避免手机号、邮件内容、订单详情等敏感信息进入日志。
    private boolean queueMessageLogBodyEnabled = false;

    /**
     * 从 Aifei 的 PropKit 读取配置。空字符串会被当成未配置处理，便于配置文件里保留空项。
     */
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
        c.queueListenerEnabled = PropKit.getBoolean("redis.queue.listenerEnabled", c.queueListenerEnabled);
        c.queueListenerPackages = blankToNull(PropKit.get("redis.queue.listenerPackages", c.queueListenerPackages));
        c.queueMessageLogEnabled = PropKit.getBoolean("redis.queue.messageLogEnabled", c.queueMessageLogEnabled);
        c.queueMessageLogBodyEnabled = PropKit.getBoolean("redis.queue.messageLogBodyEnabled", c.queueMessageLogBodyEnabled);
        return c;
    }

    /**
     * 启动前做基础校验，尽早暴露配置错误，避免插件半启动后再在工作线程里失败。
     */
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

    public boolean isQueueListenerEnabled() {
        return queueListenerEnabled;
    }

    public RedisConfig setQueueListenerEnabled(boolean queueListenerEnabled) {
        this.queueListenerEnabled = queueListenerEnabled;
        return this;
    }

    public String getQueueListenerPackages() {
        return queueListenerPackages;
    }

    public RedisConfig setQueueListenerPackages(String queueListenerPackages) {
        this.queueListenerPackages = queueListenerPackages;
        return this;
    }

    public boolean isQueueMessageLogEnabled() {
        return queueMessageLogEnabled;
    }

    public RedisConfig setQueueMessageLogEnabled(boolean queueMessageLogEnabled) {
        this.queueMessageLogEnabled = queueMessageLogEnabled;
        return this;
    }

    public boolean isQueueMessageLogBodyEnabled() {
        return queueMessageLogBodyEnabled;
    }

    public RedisConfig setQueueMessageLogBodyEnabled(boolean queueMessageLogBodyEnabled) {
        this.queueMessageLogBodyEnabled = queueMessageLogBodyEnabled;
        return this;
    }
}
