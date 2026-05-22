package io.github.macaque0.aifei.redis.queue;

/**
 * 队列消费事件监听器，用于接入日志、审计、指标和告警。
 *
 * <p>实现类的异常会被 RedisQueueEventKit 隔离，不会影响消息消费结果。</p>
 */
public interface RedisQueueEventListener {

    RedisQueueEventListener NOOP = new RedisQueueEventListener() {
    };

    default void onConsumeStart(String queue, RedisMessage<?> message, String consumerId) {
    }

    /**
     * 普通/延迟/可靠队列消费成功都会触发。
     */
    default void onConsumeSuccess(String queue, RedisMessage<?> message, String consumerId, long elapsedMillis) {
    }

    /**
     * 普通队列和延迟队列的业务方法失败会触发；消息此时已经被 poll 出来，不会自动重试。
     */
    default void onConsumeFailure(String queue, RedisMessage<?> message, String consumerId, Throwable error, long elapsedMillis) {
    }

    /**
     * 可靠队列业务失败但尚未达到最大尝试次数时触发。
     */
    default void onRetry(String queue, RedisMessage<?> message, String consumerId, Throwable error,
                         long delayMillis, long elapsedMillis) {
    }

    /**
     * 可靠队列达到最大尝试次数进入死信时触发。
     */
    default void onDead(String queue, RedisMessage<?> message, String consumerId, Throwable error, long elapsedMillis) {
    }

    /**
     * poll/reserve 线程自身异常，或业务自定义回调异常时触发。
     */
    default void onConsumerError(String queue, String consumerId, Throwable error) {
    }
}
