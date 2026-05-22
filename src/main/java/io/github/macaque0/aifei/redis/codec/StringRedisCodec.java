package io.github.macaque0.aifei.redis.codec;

public class StringRedisCodec implements RedisCodec<String> {

    public static final StringRedisCodec INSTANCE = new StringRedisCodec();

    @Override
    public String encode(String value) {
        return value;
    }

    @Override
    public String decode(String value) {
        return value;
    }
}
