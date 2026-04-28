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

| Category | Commands |
|----------|----------|
| Write with inline TTL | `SET key val [EX/PX/EXAT/PXAT]`, `SETEX key seconds val`, `PSETEX key ms val` |
| Write without TTL | `SETNX key val`, `GETSET key val`, `MSET k1 v1 ...`, `HSET key f1 v1 ...`, `HMSET key f1 v1 ...`, `HSETNX key f v`, `SADD key m1 ...`, `ZADD key ...`, `RESTORE key ttl serialized` |
| TTL-only (no write) | `EXPIRE key seconds`, `PEXPIRE key ms`, `EXPIREAT key timestamp`, `PEXPIREAT key ms-timestamp` |

**TTL extraction strategy:**
- Commands with inline TTL (`SETEX`, `SET ... EX`, etc.): parse TTL directly from arguments.
- TTL-only commands (`EXPIRE`, `PEXPIRE`, etc.): parse TTL directly from arguments.
- Write commands without TTL arguments: defer to `TtlSampler` with delayed query (see 3.7).

**Design note:** We do NOT distinguish "new key creation" from "overwrite of existing key". All write commands increment the pattern's `writeCount`. `EXPIRE`-family commands do NOT increment `writeCount` (they do not write key data).

### 3.3 KeyDeserializer

MONITOR output provides key names as strings. However, in some Redis deployments keys are stored as serialized Java objects. We support two deserialization strategies, tried in sequence:

**Strategy: Fallback chain**

```java
class KeyDeserializer {
    String deserialize(byte[] keyBytes) {
        // 1. Try JdkKeyDeserializer first
        try {
            return jdkDeserializer.deserialize(keyBytes);
        } catch (Exception e) {
            // 2. Fallback to StringDeserializer
            try {
                return stringDeserializer.deserialize(keyBytes);
            } catch (Exception e2) {
                // 3. Last resort: raw bytes as hex string
                return Hex.encodeHexString(keyBytes);
            }
        }
    }
}
```

**JdkKeyDeserializer:**
- Uses Java `ObjectInputStream` to deserialize key bytes.
- Expects deserialized result to be a `String`.
- Used when keys were written via JdkSerializationRedisSerializer.

**StringDeserializer:**
- Attempts to decode key bytes directly as UTF-8 string.
- Used when keys are plain strings (most common case).

**Note:** The fallback order can be swapped via configuration if the deployment predominantly uses plain string keys. Default order: Jdk first, String fallback.

### 3.4 PatternClusterer (Smart Clustering)

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

**No-colon keys — Prefix Trie Clustering:**

For keys without `:` delimiter, we use a **Prefix Trie** for clustering instead of a single catch-all bucket. Trie is preferred over LCS because:
- LCS may match arbitrary middle substrings (e.g., "X" from "abcXdef" / "ghiXjkl") which have no semantic meaning as a Redis key pattern.
- Prefix Trie naturally captures the leading business prefix (e.g., `user_profile_`), which is the meaningful pattern in Redis naming conventions.
- Insertion and pattern extraction are O(key length), much faster than LCS.

**Data structure:**
```java
class PrefixTrie {
    TrieNode root = new TrieNode();
    int totalNodes = 1;  // include root
    int uniqueKeys = 0;

    // Memory guards
    static final int MAX_UNIQUE_KEYS = 100_000;
    static final int MAX_NODES = 500_000;
    static final int MAX_KEY_LENGTH = 200;
    static final Set<String> seenKeys = new HashSet<>();

    class TrieNode {
        Map<Character, TrieNode> children = new HashMap<>();
        int count = 0;
    }

    boolean insert(String key) {
        if (key.length() > MAX_KEY_LENGTH) return false;
        if (!seenKeys.add(key)) {
            // duplicate key — just increment count along existing path
            TrieNode node = root;
            for (char c : key.toCharArray()) {
                node = node.children.get(c);
                node.count++;
            }
            return true;
        }
        if (seenKeys.size() > MAX_UNIQUE_KEYS) return false;

        TrieNode node = root;
        for (char c : key.toCharArray()) {
            TrieNode child = node.children.get(c);
            if (child == null) {
                if (totalNodes >= MAX_NODES) return false;
                child = new TrieNode();
                node.children.put(c, child);
                totalNodes++;
            }
            node = child;
            node.count++;
        }
        uniqueKeys++;
        return true;
    }
}
```

**Memory control strategy:**

| Guard | Threshold | Trigger behavior |
|-------|-----------|------------------|
| Key length | 200 chars | Key too long → skip Trie, directly to `*` |
| Unique keys | 100,000 | Too many distinct no-colon keys → skip Trie, directly to `*` |
| Trie nodes | 500,000 | Trie too large → skip Trie, directly to `*` |

**Memory footprint estimate:**
- Each `TrieNode` ≈ 80-100 bytes (HashMap overhead + int + object header).
- 500,000 nodes ≈ 50MB raw + HashMap expansion ≈ **~100MB maximum**.
- `seenKeys` Set: 100,000 strings × average 50 chars ≈ **~10MB**.
- Total PrefixTrie memory budget: **~110MB**, well within JVM heap tolerance for a CLI tool.

**Why three guards instead of one:**
- **Key length guard:** catches pathological keys (e.g., serialized JSON as key name).
- **Unique keys guard:** prevents memory explosion from random keys (e.g., UUIDs) where each key is unique and shares almost no prefix.
- **Node count guard:** final safety net against adversarial key shapes that share prefixes but branch heavily.

**Pattern extraction (at report time):**
DFS from root with `UPGRADE_THRESHOLD` (default 10):

```
extractPatterns(node, prefix):
    for each (char c, child) in node.children:
        if child.count >= UPGRADE_THRESHOLD:
            newPrefix = prefix + c
            if child has >= 2 children OR all grandchildren < threshold:
                // Branch point or end of high-frequency chain → cut here
                output pattern: newPrefix + "*"
                // All keys under this subtree belong to this pattern
            else:
                extractPatterns(child, newPrefix)
```

**Cut conditions (match any):**
1. **Branch point:** node has ≥ 2 children with count ≥ threshold. Example: `user_profile_` → children `1`(count=2), `2`(count=1). Cut at `user_profile_*`.
2. **End of chain:** node.count ≥ threshold, but all children count < threshold. Cut here.
3. **Leaf node:** reached the end of a key. Cut here (exact key match).

**Example:**
```
Keys: "user_profile_1001", "user_profile_1002", "user_profile_2001"

Trie:
u(3) → s(3) → e(3) → r(3) → _(3) → p(3) → r(3) → o(3) → f(3) → i(3) → l(3) → e(3) → _(3)
                                                                          │
                                                                          ├── 1(2) → 0(2) → 0(2) → 1(1)
                                                                          │
                                                                          └── 2(1) → 0(1) → 0(1) → 1(1)

At "user_profile_" (count=3 ≥ 10? No, 3 < 10 with default threshold).
If threshold=2: count=3 ≥ 2, and has 2 children → branch point → pattern: "user_profile_*"
```

**Unclusterable keys:**
- Keys not belonging to any extracted pattern (subtree count < threshold) are merged into the fallback pattern `*`.
- Keys rejected by Trie memory guards (too long, too many unique keys, or Trie node limit reached) also land in `*`.
- Keys that fail all deserialization attempts (hex fallback) also land in `*`.

**Pattern output format:**
- Variable segments rendered as `*`.
- Example: `user:*:profile`, `session:*:data`, `log:*`.

### 3.5 PatternStatsAggregator

Central in-memory store for all statistics.

```java
class PatternStats {
    String pattern;
    long writeCount;                    // total writes during monitor period
    ReservoirSampler<Long> ttlSamples;  // fixed-size TTL reservoir (default 5)
    ReservoirSampler<Long> memorySamples; // fixed-size memory reservoir (default 10)
}
```

**Computed fields (at report time):**

| Field | Formula |
|-------|---------|
| `writeRatePerSecond` | `writeCount / durationSec` |
| `avgTtlSeconds` | `ttlSamples.mean() / 1000.0` (if ttlSamples not empty) |
| `avgMemoryBytes` | `memorySamples.mean()` |
| **`incrementBytes`** | **`writeCount * avgMemoryBytes`** |
| `balancedBytes` | `writeRatePerSecond * avgTtlSeconds * avgMemoryBytes` (only if ttlSamples not empty) |

**Assumption:** We assume all keys within the same pattern share the same TTL. The `avgTtlSeconds` is computed from a small fixed-size sample per pattern. We also assume expiration rate ≈ write rate for the `balancedBytes` reference value. The primary output metric is `incrementBytes`.

### 3.6 MemorySamplerThread

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
- Each pattern tracks how many memory samples have been taken.
- Once a pattern reaches the limit, no more `MEMORY USAGE` queries for that pattern.
- This caps Redis load at: `number_of_patterns * samples_per_pattern`.

### 3.7 TtlSampler

Two paths feed into TTL sampling: direct extraction from command arguments, and delayed query for commands without inline TTL.

#### Path A: Direct extraction (inline TTL commands)

When the `CommandParser` sees a command that carries TTL in its arguments, the TTL value is extracted immediately and added to the pattern's `ttlSamples` reservoir.

**Sources for direct extraction:**
- `SETEX` / `PSETEX` / `SET ... EX|PX|EXAT|PXAT` — parse TTL from the command itself
- `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` — parse TTL / timestamp from the command itself

Once a pattern has received TTL from direct extraction, it is marked (`hasTtlFromCommand = true`).

#### Path B: Delayed query (write commands without inline TTL)

For write commands that do not carry TTL arguments (e.g. plain `SET`, `HSET`, `SADD`), the system cannot know the TTL at command-parse time. Instead of querying immediately, the key is placed into a **per-pattern delay queue** and queried later.

**Why delay?** In production, clients often send `SET` followed by `EXPIRE` in a pipeline. An immediate `TTL` query would see -1 because `EXPIRE` has not yet reached Redis. A ~1 second delay lets the pipeline settle.

**Delayed-query thread workflow:**

```
For each (key, pattern) that reaches its delay deadline:
    If pattern.ttlSamples is already full:
        Discard the task — no query issued.
    Else if pattern.hasTtlFromCommand == true:
        Discard the task — avoid a -1 sample polluting
        TTL values already obtained from EXPIRE/SETEX.
    Else:
        Issue TTL key to Redis.
        If result > 0:
            Add to pattern.ttlSamples.
        Else if result == -1 (no expiry):
            Add to pattern.ttlSamples as "no TTL" indicator.
        Else if result == -2 (key gone):
            Ignore — key was deleted or evicted during the delay.
```

**Key discard rules summarized:**

| Condition at delay expiry | Action | Rationale |
|---------------------------|--------|-----------|
| Pattern's TTL reservoir is full | Skip query | Sampling quota already met |
| Pattern already has TTL from direct extraction | Skip query | Prevent -1 from plain SET/HSET from diluting known TTL |
| Neither above | Execute `TTL key` | Genuine unknown — determine whether key has expiry |

#### No-TTL handling

If a pattern's TTL samples average to ≤ 0 (all samples indicate no expiry), the pattern is treated as "no TTL":
- `balancedBytes` is not computed for this pattern.
- A representative key is added to `NoTtlKeyStore` (see 3.8).

**Assumption:** All keys within the same pattern share the same TTL. The sampled TTL is used for all writes in that pattern when computing `balancedBytes`.

### 3.8 NoTtlKeyStore

Retains up to 5 examples of keys from **patterns with no TTL**.

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

When a pattern's TTL sampling returns an average ≤ 0 (all sampled keys have no TTL):
1. The pattern is marked as "no TTL".
2. One representative key from this pattern is added to `NoTtlKeyStore` (FIFO, max 5).
3. `balancedBytes` is not computed for this pattern.

These keys are reported separately as "continuously growing, no expiry".

### 3.9 ReportPrinter

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
     --ttl-samples-per-pattern=<n>    default: 5
     --upgrade-threshold=<n>          default: 10
     --output=<console|json>          default: console
     --top-n=<n>                      default: 20
```

**Removed from original design:**
- `--ttl-sample-rate` (percentage): Replaced by `--ttl-samples-per-pattern` (fixed count per pattern).
- `--memory-sample-rate` (percentage): Replaced by `--samples-per-pattern` (fixed count per pattern).
- `--ttl-default`: No global default. Per-pattern TTL is determined by sampling.

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
