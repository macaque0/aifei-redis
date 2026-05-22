# aifei-redis 中文说明

`aifei-redis` 是面向 Aifei 项目的 Redis 插件。它把 Redis 连接池、常用命令封装和轻量队列能力放在一个插件里，适合在业务系统里快速接入缓存、计数、分布式临时状态、异步任务、延迟任务和轻量消息消费。

它提供两层能力：

- `Redis` / `RedisKit`：常用 Redis API 的薄封装，保留 `execute` 入口用于访问原生 Jedis。
- `RedisQueue*` / `RedisQueueKit`：基于 Redis 数据结构实现的轻量队列，包括普通队列、延迟队列、可靠队列、优先级队列和 Redis Streams 消费组队列。

## 适用场景

- 需要在 Aifei 项目中快速接入 Redis。
- 需要一个轻量任务队列，但暂时不想引入 Kafka、RabbitMQ、RocketMQ 等独立消息中间件。
- 任务量中小、业务能接受至少一次投递、消费者可以做幂等。
- 需要延迟任务、失败重试、死信、暂停恢复、队列统计等基础生产能力。

不建议把它当作高吞吐消息总线或 exactly-once 事务消息方案。高并发大规模消息流、跨服务强事务消息、复杂路由和广播订阅，仍建议使用专业 MQ。

## 版本要求

- JDK 8+
- Maven 3+
- Aifei 1.0.1
- Redis 5.0+

生产环境建议 Redis 6.0+ 或 7.0+。如果使用 Redis 5.x，`redis.user` 必须留空，因为 ACL 用户名是 Redis 6.0+ 才支持的能力。

## Maven 依赖

```xml
<dependency>
    <groupId>io.github.macaque0</groupId>
    <artifactId>aifei-redis</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 快速接入

在 Aifei 插件配置中注册：

```java
import cn.aifei.config.Plugins;
import io.github.macaque0.aifei.redis.RedisPlugin;

public void config(Plugins plugins) {
    plugins.add(new RedisPlugin());
}
```

也可以手动创建配置：

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

## 配置项

配置可以放到 Aifei 的 `app-config.txt`：

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

常用配置说明：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `redis.keyPrefix` | `aifei` | 普通 Redis key 前缀，队列 key 也使用它做命名空间 |
| `redis.pool.maxTotal` | `32` | Jedis 连接池最大连接数 |
| `redis.queue.maintainerEnabled` | `true` | 是否启动后台维护线程，用于延迟消息提升、超时消息重试等 |
| `redis.queue.maintainIntervalMillis` | `1000` | 队列维护线程扫描间隔 |
| `redis.queue.maintainBatchSize` | `100` | 每次维护最多处理的消息数 |
| `redis.queue.defaultVisibilityTimeoutMillis` | `30000` | 可靠队列消息被 reserve 后的可见性超时时间 |
| `redis.queue.defaultMaxRetries` | `16` | 默认最大重试次数 |
| `redis.queue.deadLetterSuffix` | `dead` | 死信集合 key 后缀 |

## 常用 Redis API

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

支持的常用 API 包括：

- String：`get`、`set`、`setex`、`del`、`exists`、`expire`、`pexpire`、`ttl`、`pttl`
- Counter：`incr`、`incrBy`、`decr`、`decrBy`
- Hash：`hset`、`hget`、`hdel`、`hgetAll`
- List：`lpush`、`rpush`、`lpop`、`rpop`、`llen`、`lrange`
- Set：`sadd`、`smembers`、`srem`
- ZSet：`zadd`、`zrange`、`zrangeByScore`、`zrem`
- 扩展入口：`scan`、`eval`、`execute`

## 队列怎么选

| 队列类型 | 适合场景 | 投递语义 |
| --- | --- | --- |
| 普通队列 `RedisQueue` | 简单异步任务、允许消费者崩溃时丢消息 | 最多一次 |
| 延迟队列 `RedisDelayQueue` | 定时执行、延迟通知、超时检查 | 最多一次 |
| 可靠队列 `RedisReliableQueue` | 订单处理、短信发送、需要 ack/retry/dead 的任务 | 至少一次 |
| 优先级队列 `RedisPriorityQueue` | 高优任务优先执行，同时需要 ack/retry/dead | 至少一次 |
| Streams 队列 `RedisStreamQueue` | 多个消费组各自收到一份消息 | Redis Streams 消费组语义 |

## 普通队列

普通队列是 FIFO，`poll` 后消息立即从 Redis 删除，适合低风险任务。

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

## 延迟队列

延迟队列保证消息不会早于指定时间被消费，但不保证毫秒级准时。

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

## 可靠队列

可靠队列使用 `reserve/ack` 模型。消费者拿到消息后必须 `ack`，否则可见性超时后会进入重试流程；超过最大重试次数会进入死信。

```java
import io.github.macaque0.aifei.redis.queue.RedisMessage;
import io.github.macaque0.aifei.redis.queue.RedisQueueKit;
import io.github.macaque0.aifei.redis.queue.RedisQueueOptions;
import io.github.macaque0.aifei.redis.queue.RedisReliableQueue;
import io.github.macaque0.aifei.redis.queue.RetryDelayPolicy;

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

RedisQueueOptions options = new RedisQueueOptions()
        .setVisibilityTimeoutMillis(30_000)
        .setMaxRetries(10)
        .setRetryDelayPolicy(RetryDelayPolicy.exponential(1000, 60_000))
        .setMessageTtlMillis(24 * 60 * 60 * 1000L);

RedisReliableQueue<String> tuned =
        RedisQueueKit.reliableQueue("sms-tuned",
                io.github.macaque0.aifei.redis.codec.StringRedisCodec.INSTANCE,
                options);
```

死信可以人工回放：

```java
queue.replayDead("sms-" + requestId);
queue.replayDeadBatch(100);
```

## Worker 自动消费

`RedisQueueWorker` 会自动循环 `reserve`，handler 成功时自动 `ack`，失败时按重试策略 `retryLater` 或进入死信。

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
                // 这里可以记录指标或发送告警
            }
        })
        .handler(mobile -> {
            sendSms(mobile);
        });

worker.start();

// 应用停止时调用
worker.close();
```

## 优先级队列

优先级越大越先消费。同优先级内尽量按创建时间排序，但不作为强 FIFO 承诺。

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

## Streams 消费组

Redis Streams 适合“多个业务组都要收到同一事件”的场景。例如库存组和通知组都要消费订单事件。

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

## 运维能力

所有队列都支持：

```java
queue.pause();
queue.resume();
boolean paused = queue.isPaused();
Object stats = queue.stats();
```

`stats` 中包含 ready、delay、reserved、dead、priority、stream length 和 paused 等指标，适合接入监控面板。

## 语义边界

- 普通队列：最多一次，消费者崩溃可能丢消息。
- 延迟队列：最多一次，保证不早于指定时间投递，不保证毫秒级准时。
- 可靠队列：至少一次，业务必须做好幂等。
- 优先级队列：至少一次，高优先级优先，同优先级 FIFO 只是尽力而为。
- Streams 队列：每个 group 收到一份，同 group 内多个 consumer 竞争。
- 不内置 exactly-once、数据库事务消息和跨系统强一致。

## 测试

单元测试：

```powershell
mvn test
```

真实 Redis 集成测试默认跳过，打开参数后执行：

```powershell
mvn "-Dredis.integration=true" "-Dredis.host=127.0.0.1" "-Dredis.port=6379" "-Dredis.password=<password>" "-Dredis.database=0" test
```

非功能测试包括压测、worker soak、网络断连恢复，默认跳过：

```powershell
mvn "-Dtest=RedisNonFunctionalIntegrationTest" "-Dredis.nonfunctional=true" "-Dredis.host=127.0.0.1" "-Dredis.port=6379" "-Dredis.password=<password>" "-Dredis.database=0" "-Dredis.nf.messages=1000" "-Dredis.nf.soakMillis=5000" test
```

更多测试用例见 [doc/redis-plugin-test-cases.md](doc/redis-plugin-test-cases.md)。
