package io.github.macaque0.aifei.redis;

import cn.aifei.aop.Aop;
import cn.aifei.aop.AopKit;
import cn.aifei.plugin.Plugin;
import io.github.macaque0.aifei.redis.queue.DefaultRedisQueueFactory;
import io.github.macaque0.aifei.redis.queue.LoggingRedisQueueEventListener;
import io.github.macaque0.aifei.redis.queue.RedisQueueEventKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueFactory;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerContainer;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueMaintainer;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URLEncoder;

/**
 * Aifei 插件入口，负责 Redis 连接池、RedisKit、队列工厂和后台线程的生命周期管理。
 */
public class RedisPlugin implements Plugin {

    // 注入到 Aifei AOP 容器的是代理对象，真正调用时再从 Kit 里取当前实例。
    private static final Redis INJECTABLE_REDIS = injectable(Redis.class, () -> RedisKit.getRedis());
    private static final RedisQueueFactory INJECTABLE_QUEUE_FACTORY = injectable(RedisQueueFactory.class, () -> RedisQueueKit.getQueueFactory());

    private final RedisConfig config;
    private Redis redis;
    private DefaultRedisQueueFactory queueFactory;
    private RedisQueueMaintainer queueMaintainer;
    private RedisQueueListenerContainer queueListenerContainer;

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
        try {
            config.validate();
            registerInjectableSingletons();
            JedisPool pool = createPool(config);
            redis = new DefaultRedis(config, pool);
            RedisKit.init(redis);
            initQueueEventListener();

            // 队列能力可以整体关闭；关闭后仍可只使用 Redis 常用 API。
            if (config.isQueueEnabled()) {
                queueFactory = new DefaultRedisQueueFactory(redis, config);
                RedisQueueKit.init(queueFactory);
                if (config.isQueueListenerEnabled()) {
                    queueListenerContainer = new RedisQueueListenerContainer(queueFactory);
                    RedisQueueListenerKit.init(queueListenerContainer);
                    if (hasText(config.getQueueListenerPackages())) {
                        queueListenerContainer.scan(config.getQueueListenerPackages());
                    }
                    queueListenerContainer.start();
                }
                if (config.isQueueMaintainerEnabled()) {
                    queueMaintainer = new RedisQueueMaintainer(queueFactory, config.getQueueMaintainIntervalMillis());
                    queueMaintainer.start();
                }
            }
        } catch (RuntimeException e) {
            // 任意一步启动失败都回滚全局 Kit，避免留下半初始化状态。
            stop();
            throw e;
        } catch (Error e) {
            stop();
            throw e;
        }
    }

    @Override
    public synchronized void stop() {
        RedisQueueMaintainer oldMaintainer = queueMaintainer;
        RedisQueueListenerContainer oldListenerContainer = queueListenerContainer;
        RedisQueueFactory oldQueueFactory = queueFactory;
        Redis oldRedis = redis;
        queueMaintainer = null;
        queueListenerContainer = null;
        queueFactory = null;
        redis = null;

        // 先摘除全局引用，再关闭线程和连接，防止停止过程中仍被新请求拿到旧实例。
        if (oldListenerContainer != null) {
            oldListenerContainer.close();
            RedisQueueListenerKit.clearInit(oldListenerContainer);
        } else {
            RedisQueueListenerKit.clearInit();
        }
        RedisQueueEventKit.clearListener();
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
            // ACL 用户名需要走 URI 形式；Redis 5.x 不支持 user，应留空走普通构造器。
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

    private void initQueueEventListener() {
        // 内置日志监听器按配置启用；业务也可以在插件启动后覆盖成自定义监听器。
        if (config.isQueueMessageLogEnabled()) {
            RedisQueueEventKit.setListener(new LoggingRedisQueueEventListener(config.isQueueMessageLogBodyEnabled()));
        } else {
            RedisQueueEventKit.clearListener();
        }
    }

    /**
     * 注册可注入代理，解决插件启动顺序和实际 Redis 实例创建时间之间的先后问题。
     */
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
        // 代理对象本身长期存在，方法调用时委托给当前 Kit 中的真实对象。
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
