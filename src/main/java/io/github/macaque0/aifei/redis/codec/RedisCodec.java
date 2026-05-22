package io.github.macaque0.aifei.redis.codec;

public interface RedisCodec<T> {

    String encode(T value);

    T decode(String value);
}
