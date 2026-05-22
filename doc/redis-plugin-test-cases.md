# Aifei Redis Plugin 功能测试用例

> 目标：基于 Redis 5.0+ 验证常用 Redis API、队列核心能力、异常恢复和配置行为。集成测试使用独立 `redis.keyPrefix`，测试完成后按前缀清理数据。

## 1. 基础 Redis API

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| R-01 | 连接和版本检查 | 启动 `RedisPlugin`，执行 `INFO server` | 能连接，版本 >= 5.0 | `RedisVersionIntegrationTest` |
| R-02 | String API | `set/get/setex/ttl/expire/del/exists` | 值和过期时间正确，删除后不存在 | `RedisIntegrationTest` |
| R-03 | 计数器 API | `incr/decr` | 原子递增递减正确 | `RedisIntegrationTest` |
| R-04 | Hash API | `hset/hget/hgetAll/hdel` | 字段读写、删除正确 | `RedisIntegrationTest` |
| R-05 | List API | `lpush/rpush/lpop/rpop/llen` | 列表顺序和长度正确 | `RedisIntegrationTest` |
| R-06 | Set API | `sadd/smembers/srem` | 成员集合正确 | `RedisIntegrationTest` |
| R-07 | ZSet API | `zadd/zrange/zrangeByScore/zrem` | 排序集合按 score 返回 | `RedisIntegrationTest` |
| R-08 | Scan 和脚本 | `scan/eval/execute` | 扫描、Lua 和原生 Jedis 回调可用 | `RedisIntegrationTest` |
| R-09 | 分布式锁 | `tryLock/unlock/renewLock` | 互斥获取、token 校验释放、续期和等待获取正确 | `RedisIntegrationTest` |
| R-10 | Codec | String、byte[]、JDK、JSON 编解码 | 编解码前后一致 | `CodecTest` |

## 2. 队列配置和 Key

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| QK-01 | 集群 hash tag | 创建 `QueueKeySet` | 同一队列所有 key 包含 `{queueName}` | `QueueKeySetTest` |
| QK-02 | 自定义死信后缀 | 配置 `redis.queue.deadLetterSuffix=failed` | 死信集合使用 `:failed` 后缀 | `QueueKeySetTest`, `RedisQueueFullIntegrationTest` |
| QK-03 | 非法配置 | 端口非法、死信后缀为空 | 启动前校验失败 | `RedisConfigTest` |

## 3. 普通队列

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| NQ-01 | FIFO | 连续 `offer a/b/c` 后 `poll` | 按入队顺序消费 | `RedisQueueFullIntegrationTest` |
| NQ-02 | 容量限制 | `maxLength=2` 且 `DROP_OLDEST` | 超限后丢弃最早消息 | `RedisQueueFullIntegrationTest` |
| NQ-03 | 暂停和恢复 | `pause/poll/resume/poll` | 暂停时不可消费，恢复后可消费 | `RedisQueueFullIntegrationTest` |
| NQ-04 | 统计 | 调用 `stats` | 队列名和容量统计正确 | `RedisQueueFullIntegrationTest` |

## 4. 延迟队列

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| DQ-01 | 延迟未到期 | `offer(body, 200ms)` 后立即 `poll` | 返回空 | `RedisQueueFullIntegrationTest` |
| DQ-02 | 延迟到期 | 等待到期后 `poll` | 返回消息 | `RedisQueueFullIntegrationTest` |
| DQ-03 | 取消延迟消息 | `offer(id)` 后 `cancel(id)` | 延迟集合为空，不再消费 | `RedisQueueFullIntegrationTest` |

## 5. 可靠队列

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| RQ-01 | Reserve/Ack | `offer/reserve/ack` | 消息进入 reserved，ack 后 payload/reserved 清理 | `RedisQueueFullIntegrationTest` |
| RQ-02 | 可见性超时 | reserve 后不 ack，等待超时 | 触发重试或死信 | `RedisQueueFullIntegrationTest` |
| RQ-03 | 最大重试死信 | `maxRetries=1`，第一次超时后再次维护 | 消息进入死信集合 | `RedisQueueFullIntegrationTest` |
| RQ-04 | 死信回放 | `replayDead` 后 reserve | 消息重新可消费，attempts 重置为 1，元数据保留 | `RedisQueueFullIntegrationTest` |
| RQ-05 | 批量操作 | `offerBatch/reserveBatch/ackBatch` | 批量数量和清理正确 | `RedisQueueFullIntegrationTest` |
| RQ-06 | 暂停恢复 | `pause/reserve/resume/reserve` | 暂停时不可 reserve，恢复后可 reserve | `RedisQueueFullIntegrationTest` |
| RQ-07 | TTL | 设置 `messageTtlMillis` | 过期消息进入死信，未过期消息正常 | `RedisQueueFullIntegrationTest` |

## 6. 优先级队列

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| PQ-01 | 优先级消费 | 低优先级先入队，高优先级后入队 | 先消费高优先级消息 | `RedisQueueFullIntegrationTest` |
| PQ-02 | Ack 清理 | reserve 后 ack | reserved、payload、priorityValue 清理 | `RedisQueueFullIntegrationTest` |
| PQ-03 | 重试维护 | reserve 后超时 | 按重试策略重新入 ready 或 dead | `RedisQueueFullIntegrationTest` |

## 7. Stream 多消费者队列

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| SQ-01 | 多消费组 | 两个 group 读取同一 stream 消息 | 每个 group 都能收到一份 | `RedisQueueFullIntegrationTest` |
| SQ-02 | Pending | group 消费未 ack | pending 可查询 | `RedisQueueFullIntegrationTest` |
| SQ-03 | Claim | 空闲 pending 转给新 consumer | 新 consumer claim 成功 | `RedisQueueFullIntegrationTest` |
| SQ-04 | Ack | ack stream 消息 | pending 被确认 | `RedisQueueFullIntegrationTest` |
| SQ-05 | 暂停恢复 | pause 后读取 | 暂停时 readGroup 返回空 | `RedisQueueFullIntegrationTest` |

## 8. Worker

| 用例 ID | 场景 | 操作 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| WK-01 | 自动消费 | 启动 worker 后批量入队 | handler 被调用，成功消息批量 ack | `RedisQueueFullIntegrationTest`, `RedisQueueWorkerTest` |
| WK-02 | 自动 ack | handler 成功返回 | reserved 清空 | `RedisQueueFullIntegrationTest` |
| WK-03 | 失败重试/死信 | handler 抛异常 | 按重试策略 nack/retry/dead | `RedisQueueFullIntegrationTest`, `RedisQueueWorkerTest` |
| WK-04 | 普通队列注解监听 | `@RedisQueueListener("queue")` 方法接收 body | 入队后自动 poll 并调用业务方法 | `RedisQueueListenerIntegrationTest` |
| WK-05 | 延迟队列注解监听 | `@RedisQueueListener(mode=DELAY)` 消费延迟消息 | 到期后自动 poll 并调用业务方法 | `RedisQueueListenerIntegrationTest` |
| WK-06 | 可靠队列注解监听 | `@RedisQueueListener(mode=RELIABLE)` 方法首轮失败 | 自动重试，成功后 ack，无死信 | `RedisQueueListenerIntegrationTest` |
| WK-07 | 注解监听重复注册 | 同一 bean 重复 `register` | 只创建一组 runner | `RedisQueueListenerContainerTest` |
| WK-08 | 插件启动失败回滚 | 扫描到非法 listener 方法 | 启动失败后全局 Kit 清理干净 | `RedisPluginLifecycleTest` |
| WK-09 | 队列消费事件 | 注解监听消费普通、延迟、可靠队列消息 | 记录 start/success/retry 等事件，不改变消费结果 | `RedisQueueListenerIntegrationTest` |

## 9. 执行命令

```powershell
mvn test
mvn "-Dredis.integration=true" "-Dredis.host=<host>" "-Dredis.port=<port>" "-Dredis.password=<password>" "-Dredis.database=<db>" test
mvn -DskipTests package
```

## 10. 非功能测试

非功能测试默认跳过，只有显式设置 `-Dredis.nonfunctional=true` 才会运行。短时 smoke 可在本地或测试环境执行；上线前建议按真实峰值放大参数并延长 soak 时间。

| 用例 ID | 场景 | 默认参数 | 预期 | 自动化覆盖 |
| --- | --- | --- | --- | --- |
| NF-01 | 并发压测 | `1000` 条消息，`4` 生产者，`4` 消费者，最长等待 `120s` | 全部消费、无重复、无死信、reserved 清零 | `RedisNonFunctionalIntegrationTest` |
| NF-02 | 长时间 worker soak | `5s`，每秒 `50` 条，10% 消息首轮计划性失败 | 全部最终消费、无死信、reserved 清零 | `RedisNonFunctionalIntegrationTest` |
| NF-03 | 网络抖动/断连恢复 | 本地 TCP 代理转发到真实 Redis，中途关闭再恢复 | 断连时命令失败，恢复后客户端可继续读写 | `RedisNonFunctionalIntegrationTest` |

示例命令：

```powershell
mvn "-Dredis.nonfunctional=true" "-Dredis.host=<host>" "-Dredis.port=<port>" "-Dredis.password=<password>" "-Dredis.database=<db>" "-Dredis.nf.messages=1000" "-Dredis.nf.soakMillis=5000" "-Dredis.nf.timeoutMillis=120000" test
```

常用参数：

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `redis.nf.messages` | 压测总消息数 | `1000` |
| `redis.nf.producers` | 压测生产者线程数 | `4` |
| `redis.nf.consumers` | worker 消费线程数 | `4`，soak 默认 `2` |
| `redis.nf.offerBatchSize` | 生产端批量入队大小 | `1` |
| `redis.nf.batchSize` | 可靠 worker 单次批量 reserve/ack 大小 | `1` |
| `redis.nf.soakMillis` | worker soak 持续时间 | `5000` |
| `redis.nf.ratePerSecond` | soak 生产速率 | `50` |
| `redis.nf.timeoutMillis` | 每个阶段等待超时 | `120000` |
