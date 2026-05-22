package io.github.macaque0.aifei.redis.badlistener;

import io.github.macaque0.aifei.redis.queue.RedisQueueListener;

public class BadQueueListener {

    @RedisQueueListener("bad")
    public void handle(String first, String second) {
    }
}
