package io.github.macaque0.aifei.redis.codec;

import io.github.macaque0.aifei.redis.RedisException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class JdkRedisCodec<T> implements RedisCodec<T> {

    @Override
    public String encode(T value) {
        if (value == null) {
            return null;
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bytes);
            out.writeObject(value);
            out.close();
            return Base64.getEncoder().encodeToString(bytes.toByteArray());
        } catch (Exception e) {
            throw new RedisException("JDK encode failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T decode(String value) {
        if (value == null) {
            return null;
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(value);
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Object ret = in.readObject();
            in.close();
            return (T) ret;
        } catch (Exception e) {
            throw new RedisException("JDK decode failed", e);
        }
    }
}
