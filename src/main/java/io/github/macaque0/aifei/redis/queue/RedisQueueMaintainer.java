package io.github.macaque0.aifei.redis.queue;

import cn.aifei.log.Log;

import java.util.List;

public class RedisQueueMaintainer implements AutoCloseable {

    private static final long ERROR_LOG_INTERVAL_MILLIS = 30000;

    private final DefaultRedisQueueFactory factory;
    private final long intervalMillis;
    private volatile boolean running;
    private volatile long lastErrorLogMillis;
    private Thread thread;

    public RedisQueueMaintainer(DefaultRedisQueueFactory factory, long intervalMillis) {
        if (factory == null) {
            throw new IllegalArgumentException("factory can not be null");
        }
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("intervalMillis must be positive");
        }
        this.factory = factory;
        this.intervalMillis = intervalMillis;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "aifei-redis-queue-maintainer");
        thread.setDaemon(true);
        thread.start();
    }

    public void maintainOnce() {
        List<RedisQueueMaintenance> queues = factory.maintenances();
        for (RedisQueueMaintenance queue : queues) {
            try {
                queue.maintain();
            } catch (Throwable e) {
                reportMaintenanceError(queue, e);
            }
        }
    }

    @Override
    public synchronized void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(Math.min(intervalMillis, 2000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    private void run() {
        while (running) {
            maintainOnce();
            sleep(intervalMillis);
        }
    }

    private void reportMaintenanceError(RedisQueueMaintenance queue, Throwable error) {
        String queueName = queue == null ? "unknown" : queue.maintenanceName();
        RedisQueueEventKit.consumerError(queueName, "aifei-redis-queue-maintainer", error);
        long now = System.currentTimeMillis();
        if (now - lastErrorLogMillis < ERROR_LOG_INTERVAL_MILLIS) {
            return;
        }
        lastErrorLogMillis = now;
        logError("Redis queue maintainer failed: " + queueName, error);
    }

    private static void logError(String message, Throwable error) {
        try {
            Log.get(RedisQueueMaintainer.class).error(message, error);
        } catch (Throwable ignored) {
            System.err.println(message);
            if (error != null) {
                error.printStackTrace(System.err);
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
