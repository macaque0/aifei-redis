package io.github.macaque0.aifei.redis.queue;

public class RedisQueueListenerKit {

    private static volatile RedisQueueListenerContainer container;

    public static void init(RedisQueueListenerContainer container) {
        if (container == null) {
            throw new IllegalArgumentException("container can not be null");
        }
        RedisQueueListenerKit.container = container;
    }

    public static RedisQueueListenerContainer getContainer() {
        RedisQueueListenerContainer ret = container;
        if (ret == null) {
            throw new IllegalStateException("RedisQueueListenerKit has not been initialized. Add RedisPlugin first.");
        }
        return ret;
    }

    public static void register(Object bean) {
        getContainer().register(bean);
    }

    public static void scan(String basePackages) {
        getContainer().scan(basePackages);
    }

    public static void clearInit() {
        container = null;
    }

    public static void clearInit(RedisQueueListenerContainer expected) {
        if (container == expected) {
            container = null;
        }
    }
}
