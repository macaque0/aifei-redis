package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.codec.RedisCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 队列消费者注解，把手写 poll/reserve 循环封装成业务方法。
 *
 * <p>普通队列和延迟队列是最多一次语义；可靠队列模式会自动 ack、retry 和 dead。</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RedisQueueListener {

    /**
     * 队列名称，例如 "email-notify"。
     */
    String value();

    /**
     * 消费模式：普通队列、延迟队列或可靠队列。
     */
    RedisQueueListenerMode mode() default RedisQueueListenerMode.NORMAL;

    /**
     * 无参数方法或 RedisMessage 泛型无法推断时，可显式指定消息 body 类型。
     */
    Class<?> bodyType() default Void.class;

    /**
     * 自定义 codec。默认按参数类型选择：String、byte[] 或 JSON。
     */
    Class<? extends RedisCodec> codec() default RedisCodec.class;

    /**
     * 可靠队列消费者标识；为空时按类名和方法名自动生成。
     */
    String consumerId() default "";

    /**
     * 并发消费线程数。
     */
    int concurrency() default 1;

    /**
     * 可靠队列单次批量 reserve 数量；普通/延迟模式会忽略该配置。
     */
    int batchSize() default 1;

    /**
     * 单次阻塞拉取最长等待时间。
     */
    long pollTimeoutMillis() default 1000;

    /**
     * 空轮询或异常后的短暂休眠时间，避免空转占满 CPU。
     */
    long idleSleepMillis() default 50;

    /**
     * 可靠队列 reserve 后的不可见时间；小于等于 0 时使用全局默认值。
     */
    long visibilityTimeoutMillis() default -1;

    /**
     * 可靠队列最大尝试次数；小于 0 时使用全局默认值。
     */
    int maxRetries() default -1;

    /**
     * 可靠队列固定重试延迟；小于 0 时使用默认重试策略。
     */
    long retryDelayMillis() default -1;
}
