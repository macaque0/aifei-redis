package io.github.macaque0.aifei.redis.queue;

import java.util.function.Consumer;

/**
 * 队列事件的全局入口。
 *
 * <p>这里刻意做成轻量静态入口，方便 Worker、注解监听容器和业务自定义组件统一上报事件。</p>
 */
public class RedisQueueEventKit {

    private static volatile RedisQueueEventListener listener = RedisQueueEventListener.NOOP;

    private RedisQueueEventKit() {
    }

    public static void setListener(RedisQueueEventListener listener) {
        RedisQueueEventKit.listener = listener == null ? RedisQueueEventListener.NOOP : listener;
    }

    public static RedisQueueEventListener getListener() {
        return listener;
    }

    public static void clearListener() {
        listener = RedisQueueEventListener.NOOP;
    }

    public static void consumeStart(String queue, RedisMessage<?> message, String consumerId) {
        fire(l -> l.onConsumeStart(queue, message, consumerId));
    }

    public static void consumeSuccess(String queue, RedisMessage<?> message, String consumerId, long elapsedMillis) {
        fire(l -> l.onConsumeSuccess(queue, message, consumerId, elapsedMillis));
    }

    public static void consumeFailure(String queue, RedisMessage<?> message, String consumerId,
                                      Throwable error, long elapsedMillis) {
        fire(l -> l.onConsumeFailure(queue, message, consumerId, error, elapsedMillis));
    }

    public static void retry(String queue, RedisMessage<?> message, String consumerId,
                             Throwable error, long delayMillis, long elapsedMillis) {
        fire(l -> l.onRetry(queue, message, consumerId, error, delayMillis, elapsedMillis));
    }

    public static void dead(String queue, RedisMessage<?> message, String consumerId,
                            Throwable error, long elapsedMillis) {
        fire(l -> l.onDead(queue, message, consumerId, error, elapsedMillis));
    }

    public static void consumerError(String queue, String consumerId, Throwable error) {
        fire(l -> l.onConsumerError(queue, consumerId, error));
    }

    private static void fire(Consumer<RedisQueueEventListener> action) {
        try {
            action.accept(listener);
        } catch (Throwable e) {
            // 事件监听是旁路能力，任何异常都不能反向影响队列消费。
            System.err.println("Redis queue event listener failed");
            e.printStackTrace(System.err);
        }
    }
}
