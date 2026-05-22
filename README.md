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
```

## Common Redis APIs

```java
import io.github.macaque0.aifei.redis.RedisKit;

RedisKit.setex("session:" + token, 7200, userId);
String currentUserId = RedisKit.get("session:" + token);

RedisKit.hset("user:" + userId, "name", "Alice");
String name = RedisKit.hget("user:" + userId, "name");

RedisKit.incr("counter:sms");
RedisKit.rpush("list:jobs", "job-1", "job-2");
long size = RedisKit.llen("list:jobs");

String pong = RedisKit.execute(jedis -> jedis.ping());
```

Supported command groups include String, counter, Hash, List, Set, ZSet, `scan`, `eval`, and native `execute`.

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
mvn "-Dtest=RedisNonFunctionalIntegrationTest" "-Dredis.nonfunctional=true" "-Dredis.host=127.0.0.1" "-Dredis.port=6379" "-Dredis.password=<password>" "-Dredis.database=0" "-Dredis.nf.messages=1000" "-Dredis.nf.soakMillis=5000" test
```

See [doc/redis-plugin-test-cases.md](doc/redis-plugin-test-cases.md) for the full test case list.
