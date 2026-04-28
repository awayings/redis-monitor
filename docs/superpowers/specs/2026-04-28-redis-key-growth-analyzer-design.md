# Redis Key Growth Analyzer — Design Spec

## 1. Background & Goal

Redis memory usage is continuously rising. We need a diagnostic command-line tool to identify which **newly added key patterns** are driving the growth, and estimate their **eventual steady-state memory footprint**.

Key characteristics:
- Redis data size: ~90 GB. Full snapshot scan is impractical.
- Keys use **JdkSerialization** for their names.
- Most keys follow a `fixed-prefix:variable-param` naming convention, separated by `:`.
- Goal: find the Top 20 key patterns with the highest estimated balanced memory.

## 2. Architecture

Single-process real-time processing (Option A). All state kept in memory.

```
┌────────────────────────────────────────────────────────────┐
│                   KeyGrowthAnalyzer                         │
│                  (main entry + orchestrator)                │
├────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐│
│  │ Monitor     │───▶│ Command     │───▶│ KeyPattern      ││
│  │ Stream      │    │ Parser      │    │ Clusterer       ││
│  │ (Jedis)     │    │             │    │                 ││
│  └─────────────┘    └─────────────┘    └─────────────────┘│
│         │                    │                   │         │
│         ▼                    ▼                   ▼         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐│
│  │ JdkKey      │    │ Ttl         │    │ Memory          ││
│  │ Deserializer│    │ Resolver    │    │ Estimator       ││
│  └─────────────┘    └─────────────┘    └─────────────────┘│
│         │                    │                   │         │
│         └────────────────────┴───────────────────┘         │
│                              │                             │
│                              ▼                             │
│                    ┌─────────────────┐                     │
│                    │ PatternStats    │                     │
│                    │ Aggregator      │                     │
│                    └─────────────────┘                     │
│                              │                             │
│                              ▼                             │
│                    ┌─────────────────┐                     │
│                    │ ReportPrinter   │                     │
│                    │ (Top 20 output) │                     │
│                    └─────────────────┘                     │
└────────────────────────────────────────────────────────────┘
```

## 3. Component Responsibilities

### 3.1 MonitorStream
- Connects to Redis via Jedis `MONITOR`.
- Reads the stream line by line in a dedicated thread.
- Feeds raw command lines to `CommandParser`.

### 3.2 CommandParser
- Parses MONITOR output lines to extract: timestamp, db-index, command name, arguments.
- Filters for commands that may create new keys (whitelist).
- Extracts key names and command-specific TTL arguments.
- Ignores `LPUSH`, `RPUSH`, `INCR`, `DECR`, `INCRBY` per user requirement.

**Tracked commands:**

| Command | TTL Source |
|---------|-----------|
| `SET key val [EX/PX/EXAT/PXAT] [KEEPTTL]` | Arguments; KEEPTTL means retain existing TTL (sample query) |
| `SETEX key seconds val` | `seconds` argument |
| `PSETEX key ms val` | `ms` argument |
| `SETNX key val` | None; sample query |
| `GETSET key val` | None; sample query |
| `MSET k1 v1 [k2 v2 ...]` | None; all keys sampled |
| `HSET key f1 v1 [f2 v2 ...]` | None; sample query |
| `HMSET key f1 v1 [f2 v2 ...]` | None; sample query |
| `HSETNX key f v` | None; sample query |
| `SADD key m1 [m2 ...]` | None; sample query |
| `ZADD key [opts] s1 m1 [s2 m2 ...]` | None; sample query |
| `RESTORE key ttl serialized` | `ttl` argument |

### 3.3 JdkKeyDeserializer
- Deserializes Redis key bytes using Java native `ObjectInputStream`.
- Expects deserialized result to be a `String`.
- If deserialization fails, falls back to raw bytes represented as a hex string for debugging.

### 3.4 KeyPatternClusterer (Smart Clustering)

**Algorithm: Incremental Segment-based Pattern Extraction**

Keys are split by `:`. Each segment is classified as fixed or variable, with variable segments typed.

**Data structure:**
```java
class KeyPattern {
    int segmentCount;
    SegmentType[] segmentTypes;   // FIXED, NUMBER, UUID, DATE, HEX, STRING
    String[] fixedValues;         // null for variable segments
    long keyCount;
    Map<Integer, SampleReservoir> segmentSamples; // per-segment sample reservoir
}
```

**Processing a new key:**
1. If key contains no `:`, route to `NoColonHandler`.
2. Split by `:` into segments.
3. Find matching `KeyPattern`:
   - Same segment count.
   - For each position: FIXED segments must match exactly; variable segments must match the type regex.
4. If match found: increment count, update sample reservoir, then `tryUpgradeFixedSegments()`.
5. If no match: create new pattern with all segments as FIXED.

**Segment type regexes:**
| Type | Pattern |
|------|---------|
| NUMBER | `^\d+$` |
| UUID | `^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$` |
| DATE | `^\d{4}[-/]?\d{2}[-/]?\d{2}$` |
| HEX | `^[0-9a-fA-F]{16,}$` |
| STRING | fallback |

**Upgrade rule:**
When a FIXED segment accumulates `UPGRADE_THRESHOLD` (default 10) distinct values, upgrade to variable:
1. Mark as variable.
2. Inspect sample reservoir: if >= 90% match one type, assign that type; otherwise STRING.
3. Discard fixed value and samples.

**No-colon keys:**
- All collected into a single wildcard pattern `*`.
- Maintain a min-heap (size 10) of the largest keys by memory usage for display.

**Pattern output format:**
- Variable segments rendered as `*` (concise style).
- Example: `user:*:profile`, `session:*:data`, `log:*`.

### 3.5 TtlResolver
- **Command argument extraction:** parse `SET`, `SETEX`, `PSETEX`, `RESTORE` for explicit TTL.
- **Sampling:** for keys without explicit TTL, sample a configurable percentage of keys per pattern.
- Sampling method: maintain a per-pattern counter; every Nth key triggers a `TTL` / `PTTL` query via Jedis.
- Result stored as average TTL per pattern (seconds).
- If sampling fails or returns -1 (no TTL), fall back to `--ttl-default`.

### 3.6 MemoryEstimator
- Per-pattern sample rate (configurable, default 5%).
- Sampled keys queried via `MEMORY USAGE <key>` through Jedis.
- Computes running average memory per key per pattern.
- If `MEMORY USAGE` fails, estimate from `serializedKeyLength + valueLength + overhead`.

### 3.7 PatternStatsAggregator
Per pattern, computes:
- `keyCount`: number of keys observed during monitor period.
- `writeRate`: `keyCount / durationSeconds` (keys per second).
- `avgTtl`: average TTL in seconds.
- `avgMemory`: average bytes per key.
- **balancedMemory = writeRate * avgTtl * avgMemory** (estimated steady-state total bytes).

At report time, sort all patterns by `balancedMemory` descending.

### 3.8 ReportPrinter
Outputs to stdout. Format:

```
=== Redis Key Growth Analysis ===
Host: localhost:6379
Duration: 10m
Keys captured: 15,234
Patterns found: 42

Rank | Pattern              | Count  | Write Rate | Avg TTL | Avg Mem | Balance Mem | % Total
-----|----------------------|--------|------------|---------|---------|-------------|--------
  1  | user:*:session       | 3,420  |   5.7/s    |  3600s  |  1.2KB  |    14.8GB   |  38.2%
  2  | order:*:detail       | 1,890  |   3.2/s    |  7200s  |  2.5KB  |    13.6GB   |  35.1%
  3  | cache:product:*      | 5,600  |   9.3/s    |   300s  |  0.8KB  |     2.2GB   |   5.7%
 ... | ...                  | ...    |   ...      |   ...   |   ...   |     ...     |   ...

=== No-colon keys (Top 10) ===
Rank | Key (hex)              | Avg Mem
-----|------------------------|--------
  1  | aced0005...            |  3.5KB
 ... | ...                    |   ...
```

## 4. CLI Arguments

```
java -cp ... com.yj.redis.monitor.analyzer.KeyGrowthAnalyzer \
     --host=<host>              default: localhost
     --port=<port>              default: 6379
     --duration=<duration>      e.g. 10m, 1h (required)
     --ttl-sample-rate=<rate>   default: 0.1 (10%)
     --memory-sample-rate=<rate> default: 0.05 (5%)
     --ttl-default=<seconds>    default: 3600
     --upgrade-threshold=<n>    default: 10
```

## 5. Error Handling

| Scenario | Behavior |
|----------|----------|
| Redis connection lost | Retry 3 times with exponential backoff; if still failing, print partial report and exit with code 1. |
| MONITOR parse failure | Log warning, skip malformed line, continue. |
| Key deserialization failure | Log at debug level, use hex representation, still count in `*` pattern. |
| `MEMORY USAGE` / `TTL` query failure | Skip this sample, continue with existing average. |
| No keys captured during duration | Print empty report and exit with code 0. |

## 6. Testing Strategy

- **Unit tests** for `CommandParser`: verify parsing of each tracked command and TTL argument extraction.
- **Unit tests** for `KeyPatternClusterer`: feed synthetic key sequences and verify pattern merging and upgrade behavior.
- **Unit tests** for `JdkKeyDeserializer`: serialize known strings with `ObjectOutputStream`, verify deserialization.
- **Integration test** (optional): start embedded Redis (or testcontainers), write known keys, run analyzer for 30s, verify report.

## 7. Module Placement

New code lives in `redis-monitor-analyzer` module:
- Package: `com.yj.redis.monitor.analyzer.growth`
- Main class: `KeyGrowthAnalyzer`
- Dependencies: `redis-monitor-core` (for Jedis connection), `redis-monitor-common`.
