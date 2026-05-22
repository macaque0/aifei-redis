package io.github.macaque0.aifei.redis.codec;

import java.util.Base64;

public class ByteArrayRedisCodec implements RedisCodec<byte[]> {

    public static final ByteArrayRedisCodec INSTANCE = new ByteArrayRedisCodec();

    @Override
    public String encode(byte[] value) {
        return value == null ? null : Base64.getEncoder().encodeToString(value);
    }

    @Override
    public byte[] decode(String value) {
        return value == null ? null : Base64.getDecoder().decode(value);
    }
}
