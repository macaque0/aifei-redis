# aifei-redis

`aifei-redis` is a Redis plugin for Aifei projects. It wraps Redis connection management, common Redis commands, and a set of lightweight Redis-backed queues in one module.

For Chinese documentation, see [README.zh-CN.md](README.zh-CN.md).

## What It Provides

- `Redis` / `RedisKit`: thin wrappers for common Redis APIs, with `execute` for native Jedis access.
- `RedisPlugin`: Aifei plugin lifecycle integration.
- `RedisQueue*` / `RedisQueueKit`: lightweight queues based on Redis data structures.

Queue types:

| Type | Use case | Delivery semantics |
| --- | --- | --- |
| Normal queue | Simple FIFO jobs where losing a popped message is acceptable | At-most-once |
| Delay queue | Delayed jobs, timeout checks, scheduled notifications | At-most-once |
| Reliable queue | Jobs that need ack, retry, dead letter, and replay | At-least-once |
| Priority queue | Higher priority jobs should run first, with reliable ack/retry | At-least-once |
| Streams queue | Multiple consumer groups each need a copy of an event | Redis Streams group semantics |

This plugin is a good fit for small to medium async workloads. It is not a replacement for Kafka, RabbitMQ, RocketMQ, or another dedicated MQ when you need very high throughput, complex routing, exactly-once processing, or transactional messaging.

## Requirements

- JDK 8+
- Maven 3+
- Aifei 1.0.1
- Redis 5.0+

Redis 6.0+ or 7.0+ is recommended for production. ACL usernames require Redis 6.0+, so Redis 5.x deployments should leave `redis.user` empty.

## Maven

```xml
<dependency>
    <groupId>io.github.macaque0</groupId>
    <artifactId>aifei-redis</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

Register the plugin in Aifei config:

```java
import cn.aifei.config.Plugins;
import io.github.macaque0.aifei.redis.RedisPlugin;

public void config(Plugins plugins) {
    plugins.add(new RedisPlugin());
}
```

Or build it manually:

```java
import io.github.macaque0.aifei.redis.RedisConfig;
import io.github.macaque0.aifei.redis.RedisPlugin;

plugins.add(new RedisPlugin(new RedisConfig()
        .setHost("127.0.0.1")
        .setPort(6379)
        .setPassword("secret")
        .setDatabase(0)
        .setKeyPrefix("aifei-admin")));
```

## Configuration

Add settings to `app-config.txt`:

```properties
redis.host = 127.0.0.1
redis.port = 6379
redis.user =
redis.password =
redis.database = 0
redis.ssl = false
redis.timeoutMillis = 2000
redis.keyPrefix = aifei

redis.pool.maxTotal = 32
redis.pool.maxIdle = 16
redis.pool.minIdle = 0
redis.pool.testOnBorrow = true

redis.queue.enabled = true
redis.queue.maintainerEnabled = true
redis.queue.maintainIntervalMillis = 1000
redis.queue.maintainBatchSize = 100
redis.queue.defaultVisibilityTimeoutMillis = 30000
redis.queue.defaultMaxRetries = 16
redis.queue.deadLetterSuffix = dead
redis.queue.listenerEnabled = true
redis.queue.listenerPackages =
redis.queue.messageLogEnabled = false
redis.queue.messageLogBodyEnabled = false
```

## Common Redis APIs

```java
import io.github.macaque0.aifei.redis.RedisKit;
import io.github.macaque0.aifei.redis.RedisLock;

String sessionKey = RedisKit.key("session:" + token);
RedisKit.setex(sessionKey, 7200, userId);
String currentUserId = RedisKit.get(sessionKey);

String userKey = RedisKit.key("user:" + userId);
RedisKit.hset(userKey, "name", "Alice");
String name = RedisKit.hget(userKey, "name");

RedisKit.incr(RedisKit.key("counter:sms"));
RedisKit.rpush(RedisKit.key("list:jobs"), "job-1", "job-2");
long size = RedisKit.llen(RedisKit.key("list:jobs"));

String pong = RedisKit.execute(jedis -> jedis.ping());
```

Common Redis APIs use the key you pass in. Use `RedisKit.key("...")` when you want the configured `redis.keyPrefix` namespace. Queue APIs add the prefix automatically.

Distributed lock:

```java
try (RedisLock lock = RedisKit.tryLock(RedisKit.key("lock:activity:" + activityId), 30000)) {
    if (lock == null) {
        return;
    }
    publishActivityResult(activityId);
}
```

Use a custom token when the lock has to be released or renewed across method boundaries:

```java
String token = orderId + ":" + requestId;
RedisLock lock = RedisKit.tryLock(RedisKit.key("lock:order:" + orderId), token, 30000);
if (lock != null) {
    try {
        processOrder(orderId);
        lock.renew(30000);
    } finally {
        lock.unlock();
    }
}
```

Supported command groups include String, counter, Hash, List, Set, ZSet, distributed lock, `scan`, `eval`, and native `execute`.

## Normal Queue

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueue;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;

RedisQueue<String> queue = RedisQueueKit.queue("simple-jobs", String.class);

queue.offer("job-1");
queue.offer("biz-id-2", "job-2");

RedisMessage<String> message = queue.poll(5000);
if (message != null) {
    handle(message.getBody());
}
```

You can also use an annotation listener to hide the polling loop:

```java
import io.github.macaque0.aifei.redis.queue.RedisQueueListener;

public class SimpleJobListener {

    @RedisQueueListener("simple-jobs")
    public void handle(String body) {
        handleJob(body);
    }
}
```

Register a listener manually:

```java
import cn.aifei.aop.Aop;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerKit;

RedisQueueListenerKit.register(Aop.get(SimpleJobListener.class));
```

Or configure package scanning:

```properties
redis.queue.listenerPackages = com.example.listener,com.example.service
```

Normal queue listeners have the same semantics as manual `poll`: if the method throws, the message has already been removed.

Listener codec selection is automatic: `String` uses `StringRedisCodec`, `byte[]` uses `ByteArrayRedisCodec`, and other types use JSON. You can also provide a custom no-arg codec:

```java
@RedisQueueListener(value = "simple-jobs", codec = MyJobCodec.class)
public void handle(MyJob job) {
    handleJob(job);
}
```

## Delay Queue

```java
import io.github.macaque0.aifei.redis.queue.RedisDelayQueue;
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;

RedisDelayQueue<String> delayQueue =
        RedisQueueKit.delayQueue("order-timeout", String.class);

delayQueue.offer("order-1001", 30_000);

RedisMessage<String> due = delayQueue.poll(5000);
if (due != null) {
    closeExpiredOrder(due.getBody());
}

delayQueue.cancel("order-1001");
```

Delay queues also support annotation listeners:

```java
import io.github.macaque0.aifei.redis.queue.RedisQueueListener;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerMode;

public class OrderTimeoutListener {

    @RedisQueueListener(
            value = "order-timeout",
            mode = RedisQueueListenerMode.DELAY)
    public void handle(String orderId) {
        closeExpiredOrder(orderId);
    }
}
```

Delay listeners are still at-most-once. If the method throws, the message has already been removed. Use reliable mode when retry and dead letter are required.

## Reliable Queue

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisReliableQueue;

RedisReliableQueue<String> queue =
        RedisQueueKit.reliableQueue("sms", String.class);

queue.offer("sms-" + requestId, "18800000000");

RedisMessage<String> message = queue.reserve("sms-worker-1", 5000);
if (message != null) {
    try {
        sendSms(message.getBody());
        queue.ack(message.getId());
    } catch (Exception e) {
        queue.retryLater(message.getId(), 30_000);
    }
}

queue.replayDead("sms-" + requestId);
```

Batch APIs reduce Redis round trips for burst workloads:

```java
queue.offerBatch(Arrays.asList("18800000000", "18800000001", "18800000002"));

List<RedisMessage<String>> messages = queue.reserveBatch("sms-worker-1", 100, 1000);
try {
    for (RedisMessage<String> msg : messages) {
        sendSms(msg.getBody());
    }
    queue.ackBatch(messages.stream().map(RedisMessage::getId).toArray(String[]::new));
} catch (Exception e) {
    for (RedisMessage<String> msg : messages) {
        queue.retryLater(msg.getId(), 30_000);
    }
}
```

Reliable queue listeners are recommended for production jobs:

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueListener;
import io.github.macaque0.aifei.redis.queue.RedisQueueListenerMode;

public class SmsListener {

    @RedisQueueListener(
            value = "sms",
            mode = RedisQueueListenerMode.RELIABLE,
            consumerId = "sms-worker",
            concurrency = 4,
            batchSize = 50,
            visibilityTimeoutMillis = 30_000,
            maxRetries = 10,
            retryDelayMillis = 1000)
    public void handle(RedisMessage<String> message) {
        sendSms(message.getBody());
    }
}
```

In reliable mode, a successful method return automatically acknowledges the message. A thrown exception triggers retry and eventually dead letter.

## Message Logs And Audit Events

The plugin exposes one queue event hook for annotation listeners and `RedisQueueWorker`: consume start, success, normal/delay listener failure, reliable retry, dead letter, and consumer thread errors.

Enable the built-in logger:

```properties
redis.queue.messageLogEnabled = true
redis.queue.messageLogBodyEnabled = false
```

The built-in logger records queue name, message id, consumer id, attempts, elapsed time, and retry delay. Message body logging is disabled by default to avoid leaking sensitive data.

You can also plug in metrics, audit, or alerting code:

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueEventKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueEventListener;

RedisQueueEventKit.setListener(new RedisQueueEventListener() {
    @Override
    public void onConsumeSuccess(String queue, RedisMessage<?> message,
                                 String consumerId, long elapsedMillis) {
        metrics.timer("redis.queue.consume", "queue", queue).record(elapsedMillis);
    }

    @Override
    public void onDead(String queue, RedisMessage<?> message,
                       String consumerId, Throwable error, long elapsedMillis) {
        alert("queue dead letter: " + queue + ", id=" + message.getId(), error);
    }
});
```

Set a custom listener after `RedisPlugin` starts. Listener exceptions are isolated and do not affect message consumption.

## Worker

`RedisQueueWorker` runs the reliable queue loop for you. It reserves messages, acknowledges successful handling, retries failed handling, and moves exhausted messages to dead letter.

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueWorker;
import io.github.macaque0.aifei.redis.queue.RedisQueueWorkerListener;
import io.github.macaque0.aifei.redis.queue.RetryDelayPolicy;

RedisQueueWorker<String> worker = RedisQueueKit.worker("sms", String.class)
        .consumerId("sms-worker")
        .concurrency(4)
        .batchSize(100)
        .pollTimeoutMillis(1000)
        .idleSleepMillis(50)
        .visibilityTimeoutMillis(30_000)
        .maxRetries(10)
        .retryDelayPolicy(RetryDelayPolicy.exponential(1000, 60_000))
        .listener(new RedisQueueWorkerListener<String>() {
            @Override
            public void onDead(RedisMessage<String> message, Throwable error) {
                // send metrics or alert here
            }
        })
        .handler(mobile -> {
            sendSms(mobile);
        });

worker.start();
worker.close();
```

`batchSize` defaults to `1`. Increase it for high-throughput reliable workers; each worker thread reserves up to that many messages and acknowledges successes with `ackBatch`.

## Priority Queue

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisPriorityQueue;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;

RedisPriorityQueue<String> priorityQueue =
        RedisQueueKit.priorityQueue("sms-priority", String.class);

priorityQueue.offer("normal-sms", 1);
priorityQueue.offer("vip-sms", 10);

RedisMessage<String> message = priorityQueue.reserve("priority-worker", 5000);
if (message != null) {
    try {
        sendSms(message.getBody());
        priorityQueue.ack(message.getId());
    } catch (Exception e) {
        priorityQueue.retryLater(message.getId(), 10_000);
    }
}
```

## Redis Streams Groups

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisStreamQueue;

import java.util.List;

RedisStreamQueue<String> stream =
        RedisQueueKit.streamQueue("order-events", String.class);

stream.createGroup("inventory");
stream.createGroup("notify");

stream.add("order-created-1001");

List<RedisMessage<String>> messages =
        stream.readGroup("inventory", "inventory-1", 10, 5000);

for (RedisMessage<String> message : messages) {
    handleEvent(message.getBody());
    stream.ack("inventory", message.getId());
}

List<RedisMessage<String>> claimed =
        stream.claimIdle("inventory", "inventory-2", 60_000, 10);
```

## Operations

Queues support pause, resume, and stats:

```java
queue.pause();
queue.resume();
boolean paused = queue.isPaused();
Object stats = queue.stats();
```

## Semantics

- Normal queue: at-most-once. A popped message is removed immediately.
- Delay queue: at-most-once. Messages are promoted when due; timing is not millisecond-precise.
- Reliable queue: at-least-once. Consumers must `ack`; duplicate delivery is possible.
- Priority queue: at-least-once with priority ordering; FIFO is only best effort inside the same priority.
- Streams queue: each group receives a copy, and consumers inside one group compete.
- Exactly-once and database transaction messages are intentionally not built in.

## Testing

Run unit tests:

```bash
mvn test
```

Redis integration tests are skipped by default:

```bash
mvn "-Dredis.integration=true" "-Dredis.host=127.0.0.1" "-Dredis.port=6379" "-Dredis.password=<password>" "-Dredis.database=0" test
```

Non-functional tests cover pressure, worker soak, and network-fault recovery. They are skipped by default:

```bash
mvn "-Dtest=RedisNonFunctionalIntegrationTest" "-Dredis.nonfunctional=true" "-Dredis.host=127.0.0.1" "-Dredis.port=6379" "-Dredis.password=<password>" "-Dredis.database=0" "-Dredis.nf.messages=1000" "-Dredis.nf.soakMillis=5000" "-Dredis.nf.timeoutMillis=120000" test
```

See [doc/redis-plugin-test-cases.md](doc/redis-plugin-test-cases.md) for the full test case list.
