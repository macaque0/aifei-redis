package io.github.macaque0.aifei.redis.queue;

import cn.aifei.aop.Aop;
import cn.aifei.log.Log;
import io.github.macaque0.aifei.redis.codec.ByteArrayRedisCodec;
import io.github.macaque0.aifei.redis.codec.JsonRedisCodec;
import io.github.macaque0.aifei.redis.codec.RedisCodec;
import io.github.macaque0.aifei.redis.codec.StringRedisCodec;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * {@link RedisQueueListener} 注解的运行容器。
 *
 * <p>容器负责扫描/注册业务 bean，按注解模式创建对应的普通、延迟或可靠消费 runner。</p>
 */
public class RedisQueueListenerContainer implements AutoCloseable {

    private final RedisQueueFactory queueFactory;
    private final List<ListenerRunner> runners = new CopyOnWriteArrayList<>();
    private final Set<String> registrations = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RedisQueueListenerContainer(RedisQueueFactory queueFactory) {
        if (queueFactory == null) {
            throw new IllegalArgumentException("queueFactory can not be null");
        }
        this.queueFactory = queueFactory;
    }

    public void scan(String basePackages) {
        for (String basePackage : splitPackages(basePackages)) {
            Set<Class<?>> classes = scanClasses(basePackage);
            for (Class<?> type : classes) {
                register(Aop.get(type));
            }
        }
    }

    public void register(Object bean) {
        if (bean == null) {
            throw new IllegalArgumentException("bean can not be null");
        }
        Class<?> type = bean.getClass();
        for (Method method : listenerMethods(type)) {
            RedisQueueListener annotation = method.getAnnotation(RedisQueueListener.class);
            String key = registrationKey(method, annotation);
            if (!registrations.add(key)) {
                // 同一个 bean 重复注册时忽略，避免启动多组消费者重复处理消息。
                continue;
            }
            ListenerRunner runner = createRunner(bean, method, annotation);
            try {
                runners.add(runner);
                if (running.get()) {
                    runner.start();
                }
            } catch (RuntimeException e) {
                runners.remove(runner);
                registrations.remove(key);
                runner.close();
                throw e;
            }
        }
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        for (ListenerRunner runner : runners) {
            runner.start();
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (ListenerRunner runner : runners) {
            runner.close();
        }
        runners.clear();
        registrations.clear();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ListenerRunner createRunner(Object bean, Method method, RedisQueueListener annotation) {
        validate(annotation, method);
        MethodInvoker invoker = new MethodInvoker(bean, method);
        Class<?> bodyType = resolveBodyType(method, annotation);
        RedisCodec codec = resolveCodec(bodyType, annotation);
        RedisQueueOptions options = options(annotation);

        if (annotation.mode() == RedisQueueListenerMode.RELIABLE) {
            // 可靠模式复用 RedisQueueWorker，天然具备 ack、retry 和 dead letter。
            RedisQueueWorker<?> worker = queueFactory.worker(annotation.value(), codec, options)
                    .consumerId(consumerId(annotation, method))
                    .concurrency(annotation.concurrency())
                    .batchSize(annotation.batchSize())
                    .pollTimeoutMillis(annotation.pollTimeoutMillis())
                    .idleSleepMillis(annotation.idleSleepMillis())
                    .messageHandler(invoker::invoke);
            return new WorkerRunner(worker);
        }

        // 普通/延迟模式是 poll 后即删除消息，业务异常只记录失败事件，不会自动重试。
        MessagePoller poller;
        if (annotation.mode() == RedisQueueListenerMode.DELAY) {
            RedisDelayQueue<?> queue = queueFactory.delayQueue(annotation.value(), codec, options);
            poller = queue::poll;
        } else {
            RedisQueue<?> queue = queueFactory.queue(annotation.value(), codec, options);
            poller = queue::poll;
        }
        return new PollingRunner(annotation.value(), annotation.concurrency(), annotation.pollTimeoutMillis(),
                annotation.idleSleepMillis(), poller, invoker);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static RedisCodec resolveCodec(Class<?> bodyType, RedisQueueListener annotation) {
        if (annotation.codec() != RedisCodec.class) {
            try {
                return annotation.codec().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("@RedisQueueListener codec must have a no-arg constructor: " +
                        annotation.codec().getName(), e);
            }
        }
        if (bodyType == String.class) {
            return StringRedisCodec.INSTANCE;
        }
        if (bodyType == byte[].class) {
            return ByteArrayRedisCodec.INSTANCE;
        }
        // 对象类型默认使用 JSON，业务需要特殊格式时可在注解上指定 codec。
        return new JsonRedisCodec(bodyType);
    }

    private static RedisQueueOptions options(RedisQueueListener annotation) {
        RedisQueueOptions options = new RedisQueueOptions();
        if (annotation.visibilityTimeoutMillis() > 0) {
            options.setVisibilityTimeoutMillis(annotation.visibilityTimeoutMillis());
        }
        if (annotation.maxRetries() >= 0) {
            options.setMaxRetries(annotation.maxRetries());
        }
        if (annotation.retryDelayMillis() >= 0) {
            options.setRetryDelayPolicy(RetryDelayPolicy.fixed(annotation.retryDelayMillis()));
        }
        return options;
    }

    private static void validate(RedisQueueListener annotation, Method method) {
        if (annotation.value() == null || annotation.value().trim().isEmpty()) {
            throw new IllegalArgumentException("@RedisQueueListener value can not be blank: " + method);
        }
        if (annotation.concurrency() <= 0) {
            throw new IllegalArgumentException("@RedisQueueListener concurrency must be positive: " + method);
        }
        if (annotation.batchSize() <= 0) {
            throw new IllegalArgumentException("@RedisQueueListener batchSize must be positive: " + method);
        }
        if (annotation.pollTimeoutMillis() < 0) {
            throw new IllegalArgumentException("@RedisQueueListener pollTimeoutMillis can not be negative: " + method);
        }
        if (annotation.idleSleepMillis() < 0) {
            throw new IllegalArgumentException("@RedisQueueListener idleSleepMillis can not be negative: " + method);
        }
        if (method.getParameterTypes().length > 1) {
            throw new IllegalArgumentException("@RedisQueueListener method can have at most one parameter: " + method);
        }
        method.setAccessible(true);
    }

    private static String consumerId(RedisQueueListener annotation, Method method) {
        if (hasText(annotation.consumerId())) {
            return annotation.consumerId().trim();
        }
        return "listener-" + annotation.value() + "-" +
                method.getDeclaringClass().getSimpleName() + "-" + method.getName();
    }

    private static Class<?> resolveBodyType(Method method, RedisQueueListener annotation) {
        if (annotation.bodyType() != Void.class) {
            return annotation.bodyType();
        }
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 0) {
            return String.class;
        }
        if (!RedisMessage.class.isAssignableFrom(parameterTypes[0])) {
            return parameterTypes[0];
        }
        // RedisMessage<T> 参数必须声明具体泛型，才能选择正确的 codec。
        Type genericType = method.getGenericParameterTypes()[0];
        if (genericType instanceof ParameterizedType) {
            Type actual = ((ParameterizedType) genericType).getActualTypeArguments()[0];
            if (actual instanceof Class<?>) {
                return (Class<?>) actual;
            }
        }
        throw new IllegalArgumentException("RedisMessage listener parameter must declare a concrete body type: " + method);
    }

    private static boolean hasListenerMethod(Class<?> type) {
        return !listenerMethods(type).isEmpty();
    }

    private static Set<Class<?>> scanClasses(String basePackage) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = RedisQueueListenerContainer.class.getClassLoader();
            }
            String packagePath = basePackage.replace('.', '/');
            Enumeration<URL> resources = loader.getResources(packagePath);
            Set<Class<?>> classes = new LinkedHashSet<>();
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                if ("file".equals(url.getProtocol())) {
                    // 开发环境从 classpath 目录扫描。
                    String path = URLDecoder.decode(url.getPath(), "UTF-8");
                    scanDirectory(loader, new File(path), basePackage, classes);
                } else if ("jar".equals(url.getProtocol())) {
                    // 打包部署后从 jar 中扫描。
                    JarURLConnection connection = (JarURLConnection) url.openConnection();
                    scanJar(loader, connection.getJarFile(), packagePath, classes);
                }
            }
            return classes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan @RedisQueueListener package: " + basePackage, e);
        }
    }

    private static void scanDirectory(ClassLoader loader, File directory, String packageName, Set<Class<?>> classes) {
        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectory(loader, file, packageName + "." + file.getName(), classes);
            } else if (file.getName().endsWith(".class") && !file.getName().contains("$")) {
                String simpleName = file.getName().substring(0, file.getName().length() - ".class".length());
                addListenerClass(loader, packageName + "." + simpleName, classes);
            }
        }
    }

    private static void scanJar(ClassLoader loader, JarFile jarFile, String packagePath, Set<Class<?>> classes) {
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            if (entry.isDirectory() || !name.startsWith(packagePath) || !name.endsWith(".class") || name.contains("$")) {
                continue;
            }
            String className = name.substring(0, name.length() - ".class".length()).replace('/', '.');
            addListenerClass(loader, className, classes);
        }
    }

    private static void addListenerClass(ClassLoader loader, String className, Set<Class<?>> classes) {
        try {
            Class<?> type = Class.forName(className, false, loader);
            if (hasListenerMethod(type)) {
                classes.add(type);
            }
        } catch (LinkageError | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load @RedisQueueListener class: " + className, e);
        }
    }

    private static List<Method> listenerMethods(Class<?> type) {
        List<Method> methods = new ArrayList<>();
        Class<?> current = type;
        while (current != null && current != Object.class) {
            for (Method method : current.getDeclaredMethods()) {
                if (method.isAnnotationPresent(RedisQueueListener.class)) {
                    methods.add(method);
                }
            }
            current = current.getSuperclass();
        }
        return methods;
    }

    private static String registrationKey(Method method, RedisQueueListener annotation) {
        return method.getDeclaringClass().getName() + "#" + method.toGenericString() +
                "#" + annotation.mode() + "#" + annotation.value();
    }

    private static List<String> splitPackages(String basePackages) {
        List<String> ret = new ArrayList<>();
        if (!hasText(basePackages)) {
            return ret;
        }
        for (String value : basePackages.split(",")) {
            if (hasText(value)) {
                ret.add(value.trim());
            }
        }
        return ret;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private interface ListenerRunner extends AutoCloseable {

        void start();

        @Override
        void close();
    }

    private static class WorkerRunner implements ListenerRunner {

        private final RedisQueueWorker<?> worker;

        WorkerRunner(RedisQueueWorker<?> worker) {
            this.worker = worker;
        }

        @Override
        public void start() {
            worker.start();
        }

        @Override
        public void close() {
            worker.close();
        }
    }

    private interface MessagePoller {

        RedisMessage<?> poll(long timeoutMillis);
    }

    private static class PollingRunner implements ListenerRunner {

        private final String queueName;
        private final int concurrency;
        private final long pollTimeoutMillis;
        private final long idleSleepMillis;
        private final MessagePoller poller;
        private final MethodInvoker invoker;
        private final List<Thread> threads = new ArrayList<>();
        private final AtomicBoolean running = new AtomicBoolean(false);

        PollingRunner(String queueName, int concurrency, long pollTimeoutMillis, long idleSleepMillis,
                      MessagePoller poller, MethodInvoker invoker) {
            this.queueName = queueName;
            this.concurrency = concurrency;
            this.pollTimeoutMillis = pollTimeoutMillis;
            this.idleSleepMillis = idleSleepMillis;
            this.poller = poller;
            this.invoker = invoker;
        }

        @Override
        public synchronized void start() {
            if (!running.compareAndSet(false, true)) {
                return;
            }
            for (int i = 0; i < concurrency; i++) {
                Thread thread = new Thread(this::runLoop, "aifei-redis-listener-" + queueName + "-" + i);
                thread.setDaemon(true);
                threads.add(thread);
                thread.start();
            }
        }

        @Override
        public synchronized void close() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            for (Thread thread : threads) {
                thread.interrupt();
            }
            for (Thread thread : threads) {
                try {
                    thread.join(Math.max(100, pollTimeoutMillis + idleSleepMillis));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            threads.clear();
        }

        private void runLoop() {
            while (running.get()) {
                try {
                    RedisMessage<?> message = poller.poll(pollTimeoutMillis);
                    if (message == null) {
                        sleep(idleSleepMillis);
                        continue;
                    }
                    invoke(message);
                } catch (Exception e) {
                    if (!running.get()) {
                        return;
                    }
                    RedisQueueEventKit.consumerError(queueName, Thread.currentThread().getName(), e);
                    logError("Redis queue listener failed: " + queueName, e);
                    sleep(idleSleepMillis);
                } catch (Error e) {
                    RedisQueueEventKit.consumerError(queueName, Thread.currentThread().getName(), e);
                    logError("Redis queue listener stopped by error: " + queueName, e);
                    throw e;
                }
            }
        }

        private void invoke(RedisMessage<?> message) {
            String consumerId = Thread.currentThread().getName();
            long started = System.currentTimeMillis();
            RedisQueueEventKit.consumeStart(queueName, message, consumerId);
            try {
                invoker.invoke(message);
                RedisQueueEventKit.consumeSuccess(queueName, message, consumerId, elapsedSince(started));
            } catch (Throwable e) {
                // 普通/延迟队列的消息已经被取走，这里只能记录失败并继续下一条。
                RedisQueueEventKit.consumeFailure(queueName, message, consumerId, e, elapsedSince(started));
                logError("Redis queue listener business method failed: " + queueName, e);
                sleep(idleSleepMillis);
            }
        }
    }

    private static class MethodInvoker {

        private final Object bean;
        private final Method method;
        private final boolean messageParameter;

        MethodInvoker(Object bean, Method method) {
            this.bean = bean;
            this.method = method;
            Class<?>[] parameterTypes = method.getParameterTypes();
            this.messageParameter = parameterTypes.length == 1 && RedisMessage.class.isAssignableFrom(parameterTypes[0]);
        }

        void invoke(RedisMessage<?> message) throws Exception {
            try {
                if (method.getParameterTypes().length == 0) {
                    method.invoke(bean);
                } else if (messageParameter) {
                    // 业务方法声明 RedisMessage<T> 时，传入完整消息，便于读取 id/header/attempts。
                    method.invoke(bean, message);
                } else {
                    method.invoke(bean, message.getBody());
                }
            } catch (InvocationTargetException e) {
                Throwable target = e.getTargetException();
                if (target instanceof Exception) {
                    throw (Exception) target;
                }
                if (target instanceof Error) {
                    throw (Error) target;
                }
                throw new RuntimeException(target);
            }
        }
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static long elapsedSince(long started) {
        return Math.max(0, System.currentTimeMillis() - started);
    }

    private static void logError(String message, Throwable error) {
        try {
            Log.get(RedisQueueListenerContainer.class).error(message, error);
        } catch (Throwable ignored) {
            System.err.println(message);
            error.printStackTrace(System.err);
        }
    }
}
