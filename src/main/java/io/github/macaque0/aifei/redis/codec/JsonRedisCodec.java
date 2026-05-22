package io.github.macaque0.aifei.redis.codec;

import com.alibaba.fastjson2.JSON;

public class JsonRedisCodec<T> implements RedisCodec<T> {

    private final Class<T> type;

    public JsonRedisCodec(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("type can not be null");
        }
        this.type = type;
    }

    @Override
    public String encode(T value) {
        return value == null ? null : JSON.toJSONString(value);
    }

    @Override
    public T decode(String value) {
        return value == null ? null : JSON.parseObject(value, type);
    }
}
