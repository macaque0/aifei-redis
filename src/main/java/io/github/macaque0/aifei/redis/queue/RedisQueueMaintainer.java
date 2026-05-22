package io.github.macaque0.aifei.redis.queue;

import java.util.List;

public class RedisQueueMaintainer implements AutoCloseable {

    private final DefaultRedisQueueFactory factory;
    private final long intervalMillis;
    private volatile boolean running;
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
            queue.maintain();
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
            try {
                maintainOnce();
            } catch (Throwable ignored) {
                // Keep the maintainer alive. Callers can still run explicit
                // queue operations, and the next cycle can recover.
            }
            sleep(intervalMillis);
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
