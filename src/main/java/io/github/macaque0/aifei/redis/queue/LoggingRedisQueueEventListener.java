package io.github.macaque0.aifei.redis.queue;

import cn.aifei.log.Log;

/**
 * 内置队列消费日志监听器。
 *
 * <p>默认只记录 queue、message id、consumerId、attempts 和耗时，不记录 body。</p>
 */
public class LoggingRedisQueueEventListener implements RedisQueueEventListener {

    private final boolean includeBody;

    public LoggingRedisQueueEventListener() {
        this(false);
    }

    public LoggingRedisQueueEventListener(boolean includeBody) {
        // 生产环境建议保持 false，避免敏感业务数据进入日志系统。
        this.includeBody = includeBody;
    }

    @Override
    public void onConsumeStart(String queue, RedisMessage<?> message, String consumerId) {
        info("Redis queue consume start " + describe(queue, message, consumerId, -1, -1));
    }

    @Override
    public void onConsumeSuccess(String queue, RedisMessage<?> message, String consumerId, long elapsedMillis) {
        info("Redis queue consume success " + describe(queue, message, consumerId, elapsedMillis, -1));
    }

    @Override
    public void onConsumeFailure(String queue, RedisMessage<?> message, String consumerId,
                                 Throwable error, long elapsedMillis) {
        error("Redis queue consume failure " + describe(queue, message, consumerId, elapsedMillis, -1), error);
    }

    @Override
    public void onRetry(String queue, RedisMessage<?> message, String consumerId,
                        Throwable error, long delayMillis, long elapsedMillis) {
        warn("Redis queue consume retry " + describe(queue, message, consumerId, elapsedMillis, delayMillis), error);
    }

    @Override
    public void onDead(String queue, RedisMessage<?> message, String consumerId, Throwable error, long elapsedMillis) {
        error("Redis queue consume dead " + describe(queue, message, consumerId, elapsedMillis, -1), error);
    }

    @Override
    public void onConsumerError(String queue, String consumerId, Throwable error) {
        error("Redis queue consumer error queue=" + safe(queue) + " consumerId=" + safe(consumerId), error);
    }

    private String describe(String queue, RedisMessage<?> message, String consumerId, long elapsedMillis, long delayMillis) {
        StringBuilder builder = new StringBuilder();
        builder.append("queue=").append(safe(queue))
                .append(" id=").append(message == null ? "" : safe(message.getId()))
                .append(" consumerId=").append(safe(consumerId))
                .append(" attempts=").append(message == null ? 0 : message.getAttempts());
        if (elapsedMillis >= 0) {
            builder.append(" elapsedMillis=").append(elapsedMillis);
        }
        if (delayMillis >= 0) {
            builder.append(" delayMillis=").append(delayMillis);
        }
        if (includeBody && message != null) {
            builder.append(" body=").append(safe(String.valueOf(message.getBody())));
        }
        return builder.toString();
    }

    private static void info(String message) {
        try {
            Log.get(LoggingRedisQueueEventListener.class).info(message);
        } catch (Throwable e) {
            // Aifei Log 未初始化时退回标准输出，日志能力不影响主流程。
            System.out.println(message);
        }
    }

    private static void warn(String message, Throwable error) {
        try {
            Log.get(LoggingRedisQueueEventListener.class).warn(message, error);
        } catch (Throwable e) {
            System.err.println(message);
            print(error);
        }
    }

    private static void error(String message, Throwable error) {
        try {
            Log.get(LoggingRedisQueueEventListener.class).error(message, error);
        } catch (Throwable e) {
            System.err.println(message);
            print(error);
        }
    }

    private static void print(Throwable error) {
        if (error != null) {
            error.printStackTrace(System.err);
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value.replace('\n', ' ').replace('\r', ' ');
    }
}
