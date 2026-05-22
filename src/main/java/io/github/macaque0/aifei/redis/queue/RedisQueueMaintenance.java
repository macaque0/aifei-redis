package io.github.macaque0.aifei.redis.queue;

interface RedisQueueMaintenance {

    void maintain();

    default String maintenanceName() {
        return getClass().getSimpleName();
    }
}
