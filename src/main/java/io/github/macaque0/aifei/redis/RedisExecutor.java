package io.github.macaque0.aifei.redis;

import redis.clients.jedis.Jedis;

public interface RedisExecutor<T> {

    T execute(Jedis jedis);
}
