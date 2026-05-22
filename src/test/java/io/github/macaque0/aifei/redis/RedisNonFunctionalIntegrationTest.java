package io.github.macaque0.aifei.redis;

import io.github.macaque0.aifei.redis.codec.StringRedisCodec;
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueOptions;
import io.github.macaque0.aifei.redis.queue.RedisQueueWorker;
import io.github.macaque0.aifei.redis.queue.RetryDelayPolicy;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RedisNonFunctionalIntegrationTest {

    private RedisPlugin plugin;
    private TcpProxy proxy;
    private String prefix;

    @Before
    public void setUp() {
        Assume.assumeTrue(Boolean.getBoolean("redis.nonfunctional"));
        prefix = "aifei-redis-nf-" + System.currentTimeMillis();
    }

    @After
    public void tearDown() {
        if (proxy != null) {
            proxy.setOpen(true);
        }
        if (plugin != null) {
            cleanup();
            plugin.stop();
        }
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    public void pressureReliableQueueProducerConsumerThroughput() throws Exception {
        int messages = intProp("redis.nf.messages", 1000);
        int producers = intProp("redis.nf.producers", 4);
        int consumers = intProp("redis.nf.consumers", 4);
        long timeoutMillis = longProp("redis.nf.timeoutMillis", 30000);
        startPlugin(required("redis.host"), Integer.getInteger("redis.port", 6379),
                Math.max(8, producers + consumers + 4));

        Set<String> consumedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger sequence = new AtomicInteger();
        CountDownLatch consumed = new CountDownLatch(messages);
        RedisQueueWorker<String> worker = RedisQueueKit.worker("nf-pressure", StringRedisCodec.INSTANCE,
                        new RedisQueueOptions()
                                .setVisibilityTimeoutMillis(5000)
                                .setMaxRetries(3)
                                .setRetryDelayPolicy(RetryDelayPolicy.fixed(10)))
                .consumerId("nf-pressure-c")
                .concurrency(consumers)
                .pollTimeoutMillis(100)
                .idleSleepMillis(1)
                .messageHandler(message -> {
                    if (!consumedIds.add(message.getBody())) {
                        duplicates.incrementAndGet();
                    }
                    consumed.countDown();
                });

        ExecutorService executor = Executors.newFixedThreadPool(producers);
        long started = System.nanoTime();
        worker.start();
        try {
            for (int i = 0; i < producers; i++) {
                executor.submit(() -> {
                    int index;
                    while ((index = sequence.getAndIncrement()) < messages) {
                        String id = "p-" + index;
                        worker.getQueue().offer(id, id);
                    }
                });
            }
            executor.shutdown();
            assertTrue("producers timed out", executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS));
            assertTrue("consumers timed out", consumed.await(timeoutMillis, TimeUnit.MILLISECONDS));
            waitUntil(() -> worker.getQueue().reservedSize() == 0, timeoutMillis);
        } finally {
            worker.close();
            executor.shutdownNow();
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertEquals(messages, consumedIds.size());
        assertEquals(0, duplicates.get());
        assertEquals(0, worker.getQueue().deadSize());
        System.out.println("nf.pressure.messages=" + messages +
                ", elapsedMillis=" + elapsedMillis +
                ", throughputPerSecond=" + throughput(messages, elapsedMillis));
    }

    @Test
    public void soakWorkerKeepsProcessingWithRetries() throws Exception {
        long durationMillis = longProp("redis.nf.soakMillis", 5000);
        int ratePerSecond = intProp("redis.nf.ratePerSecond", 50);
        long timeoutMillis = longProp("redis.nf.timeoutMillis", 30000);
        startPlugin(required("redis.host"), Integer.getInteger("redis.port", 6379), 12);

        Set<String> consumedIds = ConcurrentHashMap.newKeySet();
        ConcurrentHashMap<String, AtomicInteger> handlerAttempts = new ConcurrentHashMap<>();
        AtomicInteger produced = new AtomicInteger();
        RedisQueueWorker<String> worker = RedisQueueKit.worker("nf-soak", StringRedisCodec.INSTANCE,
                        new RedisQueueOptions()
                                .setVisibilityTimeoutMillis(1000)
                                .setMaxRetries(5)
                                .setRetryDelayPolicy(RetryDelayPolicy.fixed(20)))
                .consumerId("nf-soak-c")
                .concurrency(intProp("redis.nf.consumers", 2))
                .pollTimeoutMillis(100)
                .idleSleepMillis(5)
                .messageHandler(message -> handleSoakMessage(message, handlerAttempts, consumedIds));

        long started = System.nanoTime();
        worker.start();
        try {
            long deadline = System.currentTimeMillis() + durationMillis;
            long sleepMillis = Math.max(1, 1000 / Math.max(1, ratePerSecond));
            while (System.currentTimeMillis() < deadline) {
                int index = produced.getAndIncrement();
                String id = "s-" + index + (index % 10 == 0 ? "-retry" : "");
                worker.getQueue().offer(id, id);
                Thread.sleep(sleepMillis);
            }
            waitUntil(() -> consumedIds.size() == produced.get(), Math.max(timeoutMillis, durationMillis * 2));
        } finally {
            worker.close();
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertTrue(produced.get() > 0);
        assertEquals(produced.get(), consumedIds.size());
        assertEquals(0, worker.getQueue().deadSize());
        assertEquals(0, worker.getQueue().reservedSize());
        System.out.println("nf.soak.produced=" + produced.get() +
                ", elapsedMillis=" + elapsedMillis +
                ", throughputPerSecond=" + throughput(produced.get(), elapsedMillis));
    }

    @Test
    public void networkFaultProxyDropsConnectionsAndClientRecovers() throws Exception {
        String targetHost = required("redis.host");
        int targetPort = Integer.getInteger("redis.port", 6379);
        proxy = new TcpProxy(targetHost, targetPort);
        proxy.start();
        startPlugin("127.0.0.1", proxy.getPort(), 4);

        String key = RedisKit.key("nf:fault");
        RedisKit.set(key, "before");
        assertEquals("before", RedisKit.get(key));

        proxy.setOpen(false);
        try {
            RedisKit.get(key);
            fail("redis command should fail while proxy is closed");
        } catch (RedisException expected) {
            assertTrue(expected.getMessage().contains("Redis command failed"));
        }

        proxy.setOpen(true);
        waitUntil(() -> {
            try {
                RedisKit.set(key, "after");
                return "after".equals(RedisKit.get(key));
            } catch (RuntimeException e) {
                return false;
            }
        }, longProp("redis.nf.timeoutMillis", 30000));
    }

    private static void handleSoakMessage(RedisMessage<String> message,
                                          ConcurrentHashMap<String, AtomicInteger> handlerAttempts,
                                          Set<String> consumedIds) {
        AtomicInteger attempts = handlerAttempts.computeIfAbsent(message.getBody(), key -> new AtomicInteger());
        if (message.getBody().endsWith("-retry") && attempts.incrementAndGet() == 1) {
            throw new IllegalStateException("planned retry");
        }
        consumedIds.add(message.getBody());
    }

    private void startPlugin(String host, int port, int poolMaxTotal) {
        RedisConfig config = new RedisConfig()
                .setHost(host)
                .setPort(port)
                .setUser(blankToNull(System.getProperty("redis.user")))
                .setPassword(blankToNull(System.getProperty("redis.password")))
                .setDatabase(Integer.getInteger("redis.database", 0))
                .setSsl(Boolean.getBoolean("redis.ssl"))
                .setTimeoutMillis(Integer.getInteger("redis.timeoutMillis", 2000))
                .setKeyPrefix(prefix)
                .setPoolMaxTotal(poolMaxTotal)
                .setPoolMaxIdle(poolMaxTotal)
                .setQueueMaintainIntervalMillis(50)
                .setQueueMaintainBatchSize(100);
        plugin = new RedisPlugin(config);
        plugin.start();
    }

    private void cleanup() {
        try {
            RedisKit.execute(jedis -> {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams params = new ScanParams().match(prefix + "*").count(200);
                do {
                    ScanResult<String> result = jedis.scan(cursor, params);
                    if (!result.getResult().isEmpty()) {
                        jedis.del(result.getResult().toArray(new String[0]));
                    }
                    cursor = result.getCursor();
                } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
                return null;
            });
        } catch (RuntimeException ignored) {
            // Best-effort cleanup for fault-injection tests.
        }
    }

    private static void waitUntil(Check check, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("condition not met within " + timeoutMillis + " ms");
    }

    private static String required(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static int intProp(String name, int defaultValue) {
        return Integer.getInteger(name, defaultValue);
    }

    private static long longProp(String name, long defaultValue) {
        return Long.getLong(name, defaultValue);
    }

    private static long throughput(int count, long elapsedMillis) {
        return elapsedMillis <= 0 ? count : Math.round(count * 1000.0 / elapsedMillis);
    }

    private interface Check {
        boolean ok() throws Exception;
    }

    private static class TcpProxy implements Closeable {

        private final String targetHost;
        private final int targetPort;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean open = new AtomicBoolean(true);
        private ServerSocket serverSocket;

        TcpProxy(String targetHost, int targetPort) {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
        }

        void start() throws IOException {
            serverSocket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            running.set(true);
            executor.submit(this::acceptLoop);
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        void setOpen(boolean value) {
            open.set(value);
            if (!value) {
                closeActiveSockets();
            }
        }

        @Override
        public void close() {
            running.set(false);
            closeQuietly(serverSocket);
            closeActiveSockets();
            executor.shutdownNow();
        }

        private void acceptLoop() {
            while (running.get()) {
                try {
                    Socket client = serverSocket.accept();
                    if (!open.get()) {
                        closeQuietly(client);
                        continue;
                    }
                    Socket upstream = new Socket();
                    upstream.connect(new InetSocketAddress(targetHost, targetPort), 2000);
                    sockets.add(client);
                    sockets.add(upstream);
                    executor.submit(() -> pump(client, upstream));
                    executor.submit(() -> pump(upstream, client));
                } catch (IOException e) {
                    if (running.get()) {
                        sleep(20);
                    }
                }
            }
        }

        private void pump(Socket from, Socket to) {
            try {
                InputStream input = from.getInputStream();
                OutputStream output = to.getOutputStream();
                byte[] buffer = new byte[8192];
                int read;
                while (open.get() && (read = input.read(buffer)) >= 0) {
                    output.write(buffer, 0, read);
                    output.flush();
                }
            } catch (IOException ignored) {
            } finally {
                sockets.remove(from);
                sockets.remove(to);
                closeQuietly(from);
                closeQuietly(to);
            }
        }

        private void closeActiveSockets() {
            for (Socket socket : sockets) {
                closeQuietly(socket);
            }
            sockets.clear();
        }

        private static void closeQuietly(Closeable closeable) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
