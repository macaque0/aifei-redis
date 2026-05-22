package io.github.macaque0.aifei.redis;

import cn.aifei.aop.Aop;
import cn.aifei.aop.AopKit;
import cn.aifei.plugin.Plugin;
import io.github.macaque0.aifei.redis.queue.DefaultRedisQueueFactory;
import io.github.macaque0.aifei.redis.queue.RedisQueueFactory;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueMaintainer;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URLEncoder;

public class RedisPlugin implements Plugin {

    private static final Redis INJECTABLE_REDIS = injectable(Redis.class, () -> RedisKit.getRedis());
    private static final RedisQueueFactory INJECTABLE_QUEUE_FACTORY = injectable(RedisQueueFactory.class, () -> RedisQueueKit.getQueueFactory());

    private final RedisConfig config;
    private Redis redis;
    private DefaultRedisQueueFactory queueFactory;
    private RedisQueueMaintainer queueMaintainer;

    public RedisPlugin() {
        this(loadConfig());
    }

    public RedisPlugin(RedisConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        this.config = config;
    }

    @Override
    public synchronized void start() {
        if (redis != null) {
            return;
        }
        config.validate();
        registerInjectableSingletons();
        JedisPool pool = createPool(config);
        redis = new DefaultRedis(config, pool);
        RedisKit.init(redis);

        if (config.isQueueEnabled()) {
            queueFactory = new DefaultRedisQueueFactory(redis, config);
            RedisQueueKit.init(queueFactory);
            if (config.isQueueMaintainerEnabled()) {
                queueMaintainer = new RedisQueueMaintainer(queueFactory, config.getQueueMaintainIntervalMillis());
                queueMaintainer.start();
            }
        }
    }

    @Override
    public synchronized void stop() {
        RedisQueueMaintainer oldMaintainer = queueMaintainer;
        RedisQueueFactory oldQueueFactory = queueFactory;
        Redis oldRedis = redis;
        queueMaintainer = null;
        queueFactory = null;
        redis = null;

        if (oldMaintainer != null) {
            oldMaintainer.close();
        }
        if (oldQueueFactory != null) {
            RedisQueueKit.clearInit(oldQueueFactory);
        } else {
            RedisQueueKit.clearInit();
        }
        if (oldRedis != null) {
            try {
                oldRedis.close();
            } finally {
                RedisKit.clearInit(oldRedis);
            }
        } else {
            RedisKit.clearInit();
        }
    }

    protected JedisPool createPool(RedisConfig config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getPoolMaxTotal());
        poolConfig.setMaxIdle(config.getPoolMaxIdle());
        poolConfig.setMinIdle(config.getPoolMinIdle());
        poolConfig.setTestOnBorrow(config.isPoolTestOnBorrow());

        if (hasText(config.getUser())) {
            return new JedisPool(poolConfig, redisUri(config), config.getTimeoutMillis());
        }
        return new JedisPool(poolConfig,
                config.getHost().trim(),
                config.getPort(),
                config.getTimeoutMillis(),
                trimToNull(config.getPassword()),
                config.getDatabase(),
                config.isSsl());
    }

    private static RedisConfig loadConfig() {
        try {
            return RedisConfig.fromPropKit();
        } catch (IllegalStateException e) {
            return new RedisConfig();
        }
    }

    private static URI redisUri(RedisConfig config) {
        try {
            String scheme = config.isSsl() ? "rediss" : "redis";
            String user = URLEncoder.encode(config.getUser().trim(), "UTF-8");
            String password = trimToNull(config.getPassword()) == null ? "" : URLEncoder.encode(config.getPassword().trim(), "UTF-8");
            return URI.create(scheme + "://" + user + ":" + password + "@" +
                    config.getHost().trim() + ":" + config.getPort() + "/" + config.getDatabase());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid redis config", e);
        }
    }

    private static void registerInjectableSingletons() {
        registerSingleton(Redis.class, INJECTABLE_REDIS);
        registerSingleton(RedisQueueFactory.class, INJECTABLE_QUEUE_FACTORY);
    }

    private static void registerSingleton(Class<?> type, Object value) {
        try {
            AopKit.get().addSingletonObject(type, value);
        } catch (RuntimeException e) {
            Object existing = currentAopObject(type);
            if (existing == value) {
                return;
            }
            throw new IllegalStateException("Aop singleton for " + type.getName() + " already exists", e);
        }
    }

    private static Object currentAopObject(Class<?> type) {
        try {
            return Aop.get(type);
        } catch (RuntimeException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T injectable(Class<T> type, TargetSupplier<T> supplier) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            if ("close".equals(method.getName()) && method.getParameterTypes().length == 0) {
                return null;
            }
            try {
                return method.invoke(supplier.get(), args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        });
    }

    private interface TargetSupplier<T> {
        T get();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String trimToNull(String value) {
        return hasText(value) ? value.trim() : null;
    }
}
