package io.github.macaque0.aifei.redis.queue;

public interface RedisQueueWorkerListener<T> {

    default void onStart(RedisQueueWorker<T> worker) {
    }

    default void onStop(RedisQueueWorker<T> worker) {
    }

    default void onSuccess(RedisMessage<T> message) {
    }

    default void onRetry(RedisMessage<T> message, Throwable error, long delayMillis) {
    }

    default void onDead(RedisMessage<T> message, Throwable error) {
    }

    default void onError(Throwable error) {
    }
}
