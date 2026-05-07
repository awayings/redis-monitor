# Redis Monitor

Java 8 多模块 Redis 监控与分析平台。在线监控 Redis 运行状态，离线分析 `MONITOR` 日志，并按 **key 模式** 给出内存增量归因报告。

## 模块组成

| 模块 | 职责 |
|------|------|
| `redis-monitor-common`   | 通用常量、共享数据模型、JSON 工具，零外部依赖 |
| `redis-monitor-core`     | Redis 连接管理（Jedis + Lettuce）、`INFO` / `STATS` / `MONITOR` 采样 |
| `redis-monitor-analyzer` | 慢查询分析、内存增量分析（含 CLI）、key 模式聚类 |
| `redis-monitor-alert`    | 阈值告警引擎 |
| `redis-monitor-web`      | Spring Boot REST / WebSocket 后端 |

## 环境要求

- JDK 8+
- Maven 3.6+
- Redis 4.0+

## 构建

```bash
# 全量构建（含测试）
mvn clean package

# 跳过测试
mvn clean package -DskipTests

# 单独构建 analyzer 模块（生成可执行 fat-jar）
mvn -pl redis-monitor-analyzer -am clean package
```

`redis-monitor-analyzer` 通过 `maven-shade-plugin` 打成可执行 jar，主类 `com.yj.redis.monitor.analyzer.increment.MemoryIncrementAnalyzer`。

## 运行 Web 服务

```bash
cd redis-monitor-web
mvn spring-boot:run
```

默认监听 `8080`，连接 `localhost:6379`。

---

## Analyzer CLI 用法

`MemoryIncrementAnalyzer` 是一个独立的命令行工具，用于在一段时间内观测 Redis 写入流量，并按 **key 模式** 排名给出内存增量归因。支持两种数据源：

- `live` 模式：连接到 Redis，订阅 `MONITOR` 流，并发起 `MEMORY USAGE` 采样
- `file` 模式：扫描指定目录下的 `.log` 文件（离线 `MONITOR` 日志），无需连接 Redis

### 启动方式

```bash
# 1) 通过 fat-jar 运行
java -jar redis-monitor-analyzer/target/redis-monitor-analyzer-1.0.0-SNAPSHOT.jar [options]

# 2) 通过 Maven exec 插件运行
mvn -pl redis-monitor-analyzer exec:java -Dexec.args="--host=127.0.0.1 --port=6379 --duration=300"
```

按 `Ctrl-C` 可提前结束并打印当前累计的报告（通过 JVM ShutdownHook 触发）。

### 参数列表

| 参数 | 默认值 | 适用模式 | 说明 |
|------|--------|----------|------|
| `--source=<live\|file>`         | `live`        | 共用 | 数据源：在线监控 / 离线日志解析 |
| `--host=<host>`                 | `localhost`   | live | Redis 主机 |
| `--port=<port>`                 | `6379`        | live | Redis 端口 |
| `--password=<password>`         | 无            | live | Redis 密码（可选） |
| `--duration=<seconds>`          | `300`         | live | 监控时长（秒） |
| `--input-dir=<path>`            | 无            | file | `.log` 文件目录，`source=file` 时必填 |
| `--samples-per-pattern=<n>`     | `10`          | live | 每个模式的 `MEMORY USAGE` 采样上限 |
| `--ttl-samples-per-pattern=<n>` | `5`           | 共用 | 每个模式的 TTL 采样上限 |
| `--upgrade-threshold=<n>`       | `10`          | 共用 | 段升级阈值：相同位置出现 N 个不同值后升级为通配符 `*` |
| `--top-n=<n>`                   | `20`          | 共用 | 报告输出 Top N |
| `--output=<console\|json>`      | `console`     | 共用 | 输出格式 |
| `--print-interval=<seconds>`    | `30`          | 共用 | 中间报告打印周期，`0` 关闭 |

参数格式必须为 `--key=value`，无值或非法值会立刻报错并打印 usage。

### 使用示例

```bash
# 在线监控本地 Redis 5 分钟，输出 Top 30
java -jar redis-monitor-analyzer-*.jar \
    --host=127.0.0.1 --port=6379 \
    --duration=300 --top-n=30

# 在线监控带密码的 Redis，并以 JSON 输出，每分钟打一次中间报告
java -jar redis-monitor-analyzer-*.jar \
    --host=10.0.0.10 --port=6380 --password=secret \
    --duration=900 \
    --output=json --print-interval=60

# 离线分析整批 MONITOR 日志
java -jar redis-monitor-analyzer-*.jar \
    --source=file \
    --input-dir=/data/redis_logs/batch_20260427_143001 \
    --top-n=50
```

### 报告输出

控制台模式下会打印：

1. **Top N 模式表**：每个模式的 `Writes / Write/s / AvgTTL / AvgMem / Increment / Balanced`
2. **`*` 兜底桶的样例 key**：用于排查未被聚类的异常 key
3. **No-TTL 样例**：识别到的"无过期、持续增长"的代表性 key（最多 5 条）

JSON 模式产出与上述等价的结构化结果，可直接喂给后续分析管道。

---

## 项目特点

### 内存增量评估模型

`incrementBytes` 是首要排序指标，**balanced ref** 是参考值，二者解决的是不同问题。

#### 增量公式

每个模式独立维护以下统计：

```
writeCount             // 监控窗口内的写入次数
writeRatePerSecond     = writeCount / durationSec
avgMemoryBytes         = mean(memorySamples)        // 蓄水池采样
avgTtlSeconds          = mean(ttlSamples) / 1000.0  // 蓄水池采样

incrementBytes         = writeCount * avgMemoryBytes
balancedBytes          = writeRatePerSecond * avgTtlSeconds * avgMemoryBytes
```

- **`incrementBytes`（主指标）**：监控期间该模式 *新写入* 的总字节量，用来回答"窗口内谁在涨内存"。
- **`balancedBytes`（参考值）**：在写入与过期速率均衡的稳态下，该模式驻留的内存量；只在该模式存在 TTL 样本时计算，否则留空。可用来对比"短期增量"与"长期占用"。

> 假设：同一模式下所有 key 共享 TTL；过期速率 ≈ 写入速率。这两条假设决定了 `balancedBytes` 是 *估计*，不是测量。

#### 内存样本来源

| 模式 | 来源 | 备注 |
|------|------|------|
| `live` | `MEMORY USAGE key` | 由独立后台线程异步采集，`samplesPerPattern` 控制配额，避免给 Redis 增加负担 |
| `file` | 命令值参数的字符串长度 | 离线日志拿不到 RSS，使用值长度作为 *相对* 排序代理；不是真实字节，但能在不同模式之间保留排名一致性 |

#### TTL 样本来源（双通道）

1. **直接抽取**：`SETEX` / `PSETEX` / `SET ... EX|PX|EXAT|PXAT` / `EXPIRE` 系列等内联 TTL 命令直接读出 TTL，并把模式标记为 `hasTtlFromCommand`。
2. **延迟查询**：`SET / HSET / SADD` 这类裸写命令，将 (key, pattern) 入队，约 1s 后再发 `TTL key`。延迟的目的是给客户端"`SET` + `EXPIRE` pipeline"留出落地时间，避免误把瞬时 -1 算成"无 TTL"。

延迟通道在以下情况主动丢弃任务：
- 该模式 TTL 配额已满
- 该模式已通过直接抽取拿到 TTL（避免 -1 污染已知 TTL）

如果一个模式的 TTL 平均值 ≤ 0，则视为"无过期"模式：不计算 `balancedBytes`，并把代表 key 写入 `NoTtlKeyStore`（环形 5 条），单独列出"持续增长"风险点。

#### 蓄水池采样（Reservoir Sampling）

TTL 与 Memory 都使用固定容量的蓄水池采样：

- 容量未满时直接追加
- 容量已满时以 `capacity / totalSeen` 的概率随机替换
- 数学上保证窗口内任意一次写入被采到的概率是均等的

这样：
- 单模式查询 Redis 的次数有上限（`samplesPerPattern` × 模式数）
- 不需要存全量 key，内存占用恒定
- 长尾 key 也有公平的入选机会

---

### Key 聚类算法

目标：把 `user:1001:profile`、`user:1002:profile`、`user:abc:profile` 等成千上万的 key 自动归并为同一个 *模式* `user:*:profile`，作为后续统计的基本单位。

实现采用 **两层策略**：

1. **分隔符分段聚类**（针对带 `:` 或 `_` 的常规 key）
2. **前缀字典树（Prefix Trie）聚类**（针对无分隔符的 key）

并通过一个 LRU `keyToPattern` 缓存（默认 10,000 条）短路重复 key 的聚类计算。

#### 1. 分段聚类：增量 + 类型化

对 `user:1001:profile`：

- 自动检测分隔符：优先 `:`，其次 `_`
- 按分隔符切段：`["user", "1001", "profile"]`
- 每个段被分类为：

  | 类型 | 正则 | 示例 |
  |------|------|------|
  | `NUMBER`  | `^\d+$` | `1001` |
  | `UUID`    | `^[0-9a-fA-F]{8}-?...{12}$` | `e8a1...` |
  | `DATE`    | `^\d{4}[-/]?\d{2}[-/]?\d{2}$` | `20260427` |
  | `HEX`     | `^[0-9a-fA-F]{16,}$` | `deadbeef...` |
  | `STRING` / `FIXED` | 兜底 | `profile` |

  **检测顺序：UUID → DATE → NUMBER → HEX → FIXED**。`DATE` 在 `NUMBER` 之前是为了让 `20260427` 被正确识别为日期而非纯数字。

- 已有模式中按 *相同段数* + *相同分隔符* 寻找候选：
  - **Exact match**：每个 `FIXED` 位等值，每个变量位匹配类型正则 → 计数 +1
  - **Near match**：变量位全部匹配，只有 *一个* `FIXED` 位不等 → 在该位置记录新的 distinct 值
  - **未命中**：以全 `FIXED` 段创建新模式

- **升级（upgrade）规则**：当某个 `FIXED` 位收集到 `UPGRADE_THRESHOLD` 个不同值（默认 10），将其升级为变量段。升级后立即 `mergeDuplicatePatterns()` 合并因升级而重名的模式（例如 `user:1001:profile` 和 `user:1002:profile` 升级后都变成 `user:*:profile`）。

> 这样处理可以做到：常见前缀（如 `user`）保持具体，业务变化部位（如 user id）自动收敛为 `*`，避免 key 爆炸。

#### 2. 前缀字典树：处理无分隔符 key

对 `userprofile1001`、`userprofile1002`... 这类不带分隔符的 key，用字符级 Prefix Trie 聚类。**不用 LCS（最长公共子串）**，因为 LCS 可能匹配到任意中间子串，对 Redis key 没有业务语义；前缀树天然捕获"业务前缀"（如 `userprofile_`），更符合 Redis 命名习惯，且插入与抽取都是 O(key length)。

**插入**：将每个 key 按字符插入 Trie，路径上每个节点 `count++`。重复 key 只更新计数不重复建节点。

**模式抽取（在打报告时执行）**：DFS 自根开始，对每个分支判断"切点"：

- **分叉点**：当前节点有 ≥ 2 个 `count ≥ threshold` 的子节点 → 切，输出 `<前缀>*`
- **链尾**：当前节点 `count ≥ threshold`，但所有子节点都 < threshold → 切
- 否则递归进入唯一仍超阈值的子节点

DFS 中那些无法被任何模式覆盖的 key（subtree count < threshold）统一并入兜底模式 `*`。

#### 3. 内存防护

字典树最坏情况下会爆炸式生长，因此设置三道防护墙，触发任一即放弃 Trie 路径，把该 key 直接归入 `*`：

| 防护 | 阈值 | 防的是什么 |
|------|------|-----------|
| Key 长度 | 200 字符 | 形态异常（如序列化 JSON 当 key） |
| 唯一 key 数 | 100,000 | 全是 UUID 这种几乎不共享前缀的 key |
| Trie 节点数 | 500,000 | 对抗性 key 形状（共享前缀但分叉极多） |

整个 Trie 的内存上限约 **110MB**，对一个 CLI 工具完全可控。

#### 4. 兜底模式 `*`

以下场景的 key 会被放入 `*` 模式：

- key 反序列化全部失败（hex 兜底）
- 字典树被防护墙拒收
- 字典树中未达到阈值、无法独立成簇的散点

`*` 模式额外保留最多 10 条样例 key 与单独显示，便于排查"分布太散"的写入流量。

---

## 测试

```bash
# 全量
mvn test

# 单模块
mvn test -pl redis-monitor-analyzer

# 单测试类
mvn test -pl redis-monitor-analyzer -Dtest=PatternClustererTest
```

`redis-monitor-analyzer` 中有针对每个组件的单元测试，以及端到端集成测试 `MemoryIncrementAnalyzerIntegrationTest`（需要可用的 Redis 实例，主机/端口在测试源码顶部配置）。

## 目录约定

- 包名：`com.yj.redis.monitor.{module-name}`
- 主入口：`redis-monitor-web/src/main/java/com/yj/redis/monitor/web/RedisMonitorApplication.java`
- 配置：`redis-monitor-web/src/main/resources/application.yml`，前缀 `redis.monitor`
- 设计文档：`docs/superpowers/specs/`、`docs/superpowers/plans/`
