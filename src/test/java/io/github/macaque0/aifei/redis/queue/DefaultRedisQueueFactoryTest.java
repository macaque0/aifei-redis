package io.github.macaque0.aifei.redis.queue;

import io.github.macaque0.aifei.redis.Redis;
import io.github.macaque0.aifei.redis.RedisConfig;
import io.github.macaque0.aifei.redis.RedisException;
import io.github.macaque0.aifei.redis.codec.StringRedisCodec;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultRedisQueueFactoryTest {

    @After
    public void tearDown() {
        RedisQueueEventKit.clearListener();
    }

    @Test
    public void maintenanceRegistrationReplacesDuplicateQueueNames() {
        DefaultRedisQueueFactory factory = new DefaultRedisQueueFactory(redisProxy(false), new RedisConfig());

        factory.delayQueue("jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions());
        factory.delayQueue("jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions());
        factory.reliableQueue("jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions());
        factory.reliableQueue("jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions());
        factory.priorityQueue("jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions());
        factory.priorityQueue("jobs", StringRedisCodec.INSTANCE, new RedisQueueOptions());

        assertEquals(3, factory.maintenances().size());
    }

    @Test
    public void maintainerReportsEachQueueFailureAndContinues() {
        CapturingQueueEventListener events = new CapturingQueueEventListener();
        RedisQueueEventKit.setListener(events);
        DefaultRedisQueueFactory factory = new DefaultRedisQueueFactory(redisProxy(true), new RedisConfig());
        factory.reliableQueue("bad-reliable", StringRedisCodec.INSTANCE, new RedisQueueOptions());
        factory.delayQueue("bad-delay", StringRedisCodec.INSTANCE, new RedisQueueOptions());

        captureErr(() -> new RedisQueueMaintainer(factory, 1000).maintainOnce());

        assertTrue(events.errors.contains("bad-reliable"));
        assertTrue(events.errors.contains("bad-delay"));
    }

    private static Redis redisProxy(boolean throwOnExecute) {
        return (Redis) Proxy.newProxyInstance(Redis.class.getClassLoader(), new Class<?>[]{Redis.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if ("key".equals(method.getName())) {
                        return args[0];
                    }
                    if ("execute".equals(method.getName()) && throwOnExecute) {
                        throw new RedisException("planned maintenance failure");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static void captureErr(Runnable runnable) {
        PrintStream oldErr = System.err;
        System.setErr(new PrintStream(new ByteArrayOutputStream()));
        try {
            runnable.run();
        } finally {
            System.setErr(oldErr);
        }
    }

    private static class CapturingQueueEventListener implements RedisQueueEventListener {

        final List<String> errors = new CopyOnWriteArrayList<>();

        @Override
        public void onConsumerError(String queue, String consumerId, Throwable error) {
            errors.add(queue);
        }
    }
}
