# Redis Memory Increment Analyzer — Design Spec

## 1. Background & Goal

Redis memory usage is continuously rising. We need a diagnostic command-line tool to identify which **key patterns** are driving the growth during a monitoring window, and estimate their **memory increment distribution**.

Key characteristics:
- Tool runs as a CLI, monitoring Redis via `MONITOR` for a configurable duration.
- All state kept in memory. No persistence.
- Keys follow a `prefix:variable` naming convention, separated by `:`.
- Goal: output the Top N key patterns ranked by memory increment during the monitoring period.
- Balanced memory formula (`writeRate * avgTtl * avgMemory`) is included as a **reference value**, not the primary metric.

## 2. Architecture

Single-threaded main pipeline for MONITOR parsing + aggregation. One additional thread for asynchronous `MEMORY USAGE` sampling.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MemoryIncrementAnalyzer                           │
│                   (main entry + orchestrator)                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐  │
│  │ Monitor     │───▶│ Command     │───▶│ PatternClusterer        │  │
│  │ Stream      │    │ Parser      │    │ (incremental prefix     │  │
│  │ (Jedis)     │    │             │    │  clustering)            │  │
│  └─────────────┘    └─────────────┘    └─────────────────────────┘  │
│         │                    │                   │                   │
│         │                    │                   │                   │
│         │                    │                   ▼                   │
│         │                    │    ┌─────────────────────────┐        │
│         │                    │    │ keyToPattern cache      │        │
│         │                    │    │ (avoids re-clustering)  │        │
│         │                    │    └─────────────────────────┘        │
│         │                    │                   │                   │
│         │                    └───────────────────┘                   │
│         │                              │                             │
│         ▼                              ▼                             │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              PatternStatsAggregator                          │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │    │
│  │  │ writeCount  │  │ ttlSum      │  │ memorySampleQueue   │  │    │
│  │  │ per pattern │  │ per pattern │  │ (ConcurrentQueue)   │  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  │    │
│  └────────────────────────┬────────────────────────────────────┘    │
│                           │                                          │
│                           ▼                                          │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  NoTtlKeyStore (sliding window, max 5 samples)               │    │
│  └────────────────────────┬────────────────────────────────────┘    │
│                           │                                          │
│                           ▼                                          │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              ReportPrinter (end-of-duration output)          │    │
│  │   Memory increment distribution | Balanced ref | No-TTL ex  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Thread 2: MemorySamplerThread                               │    │
│  │  (独立 Jedis 连接)                                            │    │
│  │  从队列取 key ──▶ MEMORY USAGE ──▶ 结果写回 Aggregator       │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. Component Responsibilities

### 3.1 MonitorStream
- Connects to Redis via Jedis `MONITOR`.
- Reads the stream line by line in the **main thread**.
- Feeds raw command lines to `CommandParser`.

### 3.2 CommandParser
- Parses MONITOR output lines to extract: timestamp, db-index, command name, arguments.
- Filters for commands that write keys.
- Extracts key names and command-specific TTL arguments.

**Tracked commands:**

| Command | TTL Source |
|---------|-----------|
| `SET key val [EX/PX/EXAT/PXAT]` | Arguments |
| `SETEX key seconds val` | `seconds` argument |
| `PSETEX key ms val` | `ms` argument |
| `SETNX key val` | None |
| `GETSET key val` | None |
| `MSET k1 v1 [k2 v2 ...]` | None |
| `HSET key f1 v1 [f2 v2 ...]` | None |
| `HMSET key f1 v1 [f2 v2 ...]` | None |
| `HSETNX key f v` | None |
| `SADD key m1 [m2 ...]` | None |
| `ZADD key [opts] s1 m1 [s2 m2 ...]` | None |
| `RESTORE key ttl serialized` | `ttl` argument |

**Design note:** We do NOT distinguish "new key creation" from "overwrite of existing key". All write commands increment the pattern's `writeCount`.

### 3.3 PatternClusterer (Smart Clustering)

**Algorithm: Incremental Segment-based Pattern Extraction**

Keys are split by `:`. Each segment is classified as fixed or variable, with variable segments typed.

**Data structure:**
```java
class PatternClusterState {
    String currentPattern;         // e.g. "user:*:profile"
    int keyCount;
    List<SegmentType> segmentTypes; // FIXED, NUMBER, UUID, DATE, HEX, STRING
}
```

**Processing a new key:**
1. If key contains no `:`, route to `NoColonHandler`.
2. Split by `:` into segments.
3. Check `keyToPattern` cache first. If this exact key was seen before, reuse the pattern.
4. If not in cache, find matching `PatternClusterState`:
   - Same segment count.
   - For each position: FIXED segments must match exactly; variable segments must match the type regex.
5. If match found: increment count.
6. If no match: create new pattern with all segments as FIXED.

**Segment type regexes:**
| Type | Pattern |
|------|---------|
| NUMBER | `^\d+$` |
| UUID | `^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$` |
| DATE | `^\d{4}[-/]?\d{2}[-/]?\d{2}$` |
| HEX | `^[0-9a-fA-F]{16,}$` |
| STRING | fallback |

**Upgrade rule:**
When a FIXED segment has seen `UPGRADE_THRESHOLD` (default 10) distinct values, upgrade to variable (`*`).

**No-colon keys:**
- All collected into a single pattern `*`.

**Pattern output format:**
- Variable segments rendered as `*`.
- Example: `user:*:profile`, `session:*:data`, `log:*`.

### 3.4 PatternStatsAggregator

Central in-memory store for all statistics.

```java
class PatternStats {
    String pattern;
    long writeCount;                    // total writes during monitor period
    long ttlWriteCount;                 // writes with explicit TTL
    long ttlSumMs;                      // sum of TTLs (only for ttlWriteCount)
    ReservoirSampler<Long> memorySamples; // fixed-size reservoir (default 100)
}
```

**Computed fields (at report time):**

| Field | Formula |
|-------|---------|
| `writeRatePerSecond` | `writeCount / durationSec` |
| `avgTtlSeconds` | `ttlWriteCount > 0 ? (ttlSumMs / 1000.0 / ttlWriteCount) : 0` |
| `avgMemoryBytes` | `memorySamples.mean()` |
| **`incrementBytes`** | **`writeCount * avgMemoryBytes`** |
| `balancedBytes` | `writeRatePerSecond * avgTtlSeconds * avgMemoryBytes` (only if avgTtl > 0) |

**Assumption:** We assume expiration rate ≈ write rate (balanced state assumption). This is used only for the `balancedBytes` reference value. The primary output metric is `incrementBytes`.

### 3.5 MemorySamplerThread

Runs in a **separate thread** with its own Jedis connection.

```java
class MemorySamplerThread extends Thread {
    BlockingQueue<SampleTask> queue;    // tasks from main thread
    Jedis jedis;                        // independent connection
    
    void run() {
        while (running) {
            SampleTask task = queue.poll(timeout);
            if (task != null) {
                long memory = jedis.memoryUsage(task.key);
                task.callback.accept(memory);   // write back to PatternStats
            }
        }
    }
}
```

**Sampling strategy:** Per-pattern fixed count (NOT percentage).
- Configurable: `--samples-per-pattern` (default 10).
- Each pattern tracks how many samples have been taken.
- Once a pattern reaches the limit, no more `MEMORY USAGE` queries for that pattern.
- This caps Redis load at: `number_of_patterns * samples_per_pattern`.

### 3.6 NoTtlKeyStore

Retains up to 5 examples of keys written **without TTL**.

```java
class NoTtlKeyStore {
    private static final int MAX_SIZE = 5;
    private final Queue<NoTtlKeySample> samples = new ArrayDeque<>();
    
    void offer(String key, String pattern, long memoryBytes, String command) {
        if (samples.size() >= MAX_SIZE) samples.poll();
        samples.offer(new NoTtlKeySample(key, pattern, memoryBytes, command));
    }
}
```

When a key is written without TTL:
1. Increment the pattern's `writeCount` (normal flow).
2. Do NOT increment `ttlWriteCount` or `ttlSumMs`.
3. Add to `NoTtlKeyStore` (FIFO, max 5).

These keys are reported separately as "continuously growing, no expiry".

### 3.7 ReportPrinter

Outputs to stdout at end of monitoring duration (or on Ctrl-C).

**Console format:**

```
═══════════════════════════════════════════════════════════════════
  Redis Memory Increment Analysis Report
  Host: localhost:6379  |  Duration: 300s
═══════════════════════════════════════════════════════════════════

【Memory Increment Distribution (Top N by incrementBytes)】
Rank  Pattern                Writes   WriteRate   AvgTTL     AvgMem    Increment    BalancedRef
───────────────────────────────────────────────────────────────────────────────────────────────
 1    order:*:detail           800      2.7/s     1800s     10,240 B     8.19 MB     14.75 MB
 2    user:*:profile         1,200      4.0/s     3600s      2,048 B     2.46 MB      7.37 MB
 3    cache:*                  500      1.7/s      300s        512 B       256 KB      1.25 MB
 4    session:*                300      1.0/s     7200s      1,024 B       307 KB      7.37 MB
 5    log:*                     50      0.2/s        -        5,120 B       256 KB          -
───────────────────────────────────────────────────────────────────────────────────────────────
Total (with TTL)                                                        11.52 MB     30.74 MB

【No-TTL Key Samples (continuous growth, no expiry limit)】
Pattern            Key                      Memory    Command
─────────────────────────────────────────────────────────────────
user:*:profile     user:admin:profile       4,096 B   HSET user:admin:profile ...
config:*           config:feature_flags     2,048 B   SET config:feature_flags ...
... (max 5)

═══════════════════════════════════════════════════════════════════
```

**JSON format** (when `--output=json`):

```json
{
  "meta": {
    "host": "localhost",
    "port": 6379,
    "durationSec": 300,
    "samplesPerPattern": 10,
    "totalPatterns": 45,
    "totalWriteCount": 2850
  },
  "patterns": [
    {
      "pattern": "order:*:detail",
      "writeCount": 800,
      "writeRatePerSecond": 2.7,
      "avgTtlSec": 1800,
      "avgMemoryBytes": 10240,
      "incrementBytes": 8192000,
      "balancedBytes": 14745600
    }
  ],
  "noTtlSamples": [
    {
      "key": "user:admin:profile",
      "pattern": "user:*:profile",
      "memoryBytes": 4096,
      "command": "HSET user:admin:profile name admin"
    }
  ]
}
```

## 4. CLI Arguments

```
java -cp ... com.yj.redis.monitor.analyzer.MemoryIncrementAnalyzer \
     --host=<host>                    default: localhost
     --port=<port>                    default: 6379
     --duration=<seconds>             default: 300
     --samples-per-pattern=<n>        default: 10
     --upgrade-threshold=<n>          default: 10
     --output=<console|json>          default: console
     --top-n=<n>                      default: 20
```

**Removed from original design:**
- `--ttl-sample-rate`: TTL is extracted from command arguments or left as "no TTL". No sampling needed.
- `--memory-sample-rate`: Replaced by `--samples-per-pattern` (fixed count per pattern).
- `--ttl-default`: No default TTL assumption. If no TTL in command, it's a no-TTL key.

## 5. Error Handling

| Scenario | Behavior |
|----------|----------|
| Redis connection lost at startup | Print error, exit with code 1. |
| MONITOR stream disconnected mid-run | Retry 3 times; if still failing, output partial report and exit with code 1. |
| `MEMORY USAGE` timeout (>5s) | Skip this sample, log warning, continue. |
| MONITOR parse failure | Log warning, skip malformed line, continue. |
| No keys captured during duration | Print empty report and exit with code 0. |
| User Ctrl-C / SIGINT | JVM shutdown hook triggers immediate report output with current stats. |
| keyToPattern cache too large (>1M entries) | Enable LRU eviction to prevent OOM. |

## 6. Threading Model

| Component | Thread | Reason |
|-----------|--------|--------|
| MONITOR read + parse + cluster + aggregate | **Main thread** | Parsing is not the bottleneck; avoids lock complexity in aggregation. |
| `MEMORY USAGE` sampling | **1 background thread** | Prevents blocking the main pipeline on slow Redis queries. |

Communication between threads via a `BlockingQueue<SampleTask>`:
- Main thread enqueues keys to sample.
- Sampler thread dequeues and executes `MEMORY USAGE`.
- Result is written back via a callback that updates `PatternStats.memorySamples` (thread-safe queue).

## 7. Testing Strategy

| Test Type | Coverage |
|-----------|----------|
| **Unit tests** | `CommandParser`: parsing of each tracked command and TTL extraction. |
| **Unit tests** | `PatternClusterer`: synthetic key sequences → expected pattern merging. |
| **Unit tests** | `ReservoirSampler`: mathematical correctness of fixed-size sampling. |
| **Unit tests** | `PatternStatsAggregator`: incrementBytes and balancedBytes computation. |
| **Integration test** | Start embedded Redis, write keys with known patterns/TTLs, run analyzer, verify report values. |
| **Performance test** | Use `redis-benchmark` to generate high QPS, verify no command loss in MONITOR stream. |

## 8. Module Placement

New code lives in `redis-monitor-analyzer` module:
- Package: `com.yj.redis.monitor.analyzer.increment`
- Main class: `MemoryIncrementAnalyzer`
- Dependencies: `redis-monitor-core` (for Jedis connection), `redis-monitor-common`.
