package io.github.macaque0.aifei.redis;

import io.github.macaque0.aifei.redis.codec.ByteArrayRedisCodec;
import io.github.macaque0.aifei.redis.codec.JdkRedisCodec;
import io.github.macaque0.aifei.redis.codec.JsonRedisCodec;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CodecTest {

    @Test
    public void byteArrayCodecRoundTrips() {
        byte[] bytes = new byte[]{1, 2, 3};
        assertArrayEquals(bytes, ByteArrayRedisCodec.INSTANCE.decode(ByteArrayRedisCodec.INSTANCE.encode(bytes)));
    }

    @Test
    public void jdkCodecRoundTrips() {
        JdkRedisCodec<Box> codec = new JdkRedisCodec<>();
        Box box = new Box("a", 1);
        assertEquals(box, codec.decode(codec.encode(box)));
    }

    @Test
    public void jsonCodecRoundTrips() {
        JsonRedisCodec<Box> codec = new JsonRedisCodec<>(Box.class);
        Box box = new Box("b", 2);
        assertEquals(box, codec.decode(codec.encode(box)));
    }

    public static class Box implements Serializable {
        public String name;
        public int value;

        public Box() {
        }

        Box(String name, int value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Box)) {
                return false;
            }
            Box other = (Box) o;
            return value == other.value && name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[]{name, value});
        }
    }
}
