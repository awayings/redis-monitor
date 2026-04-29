# File-Based Input for Memory Analysis — Design Spec

## 1. Background & Goal

The `MemoryIncrementAnalyzer` currently only supports live Redis MONITOR streams as input. We need to add an **offline file-based input mode** that reads pre-recorded MONITOR logs (`.log` files) from a directory, analyzes them, and produces the same pattern statistics report — without requiring a live Redis connection.

Key characteristics:
- Log files are REDIS MONITOR output saved to disk, one line per command.
- Multiple files exist in a batch directory, each from a different Redis node.
- Each file is analyzed independently; a cross-file summary is also output.
- No Redis connection needed in file mode. Delayed TTL queries are skipped.
- Memory estimation: instead of `MEMORY USAGE key`, use the **string length of the value argument(s)** from the MONITOR output as a proxy for memory size.
- TTL comes from inline + EXPIRE commands only; delayed `TTL key` queries are skipped.
- Report is fully populated — all columns have data in file mode.

## 2. Line Format

```
1777329020.191216 [0 10.0.0.53:33414] "SETEX" "\xac\xed\x00..." "2592000" "\xac\xed\x00..."
```

- Unix timestamp (seconds.microseconds) at line start.
- `[db client:port]` — database index and client address (ignored).
- Double-quoted command name and arguments.

The same `CommandParser.tokenize()` method handles this format — it already searches for `]` and extracts double-quoted tokens.

## 3. Architecture

```
File Mode                           Live Mode (unchanged)
─────────                           ─────────────────────
FileLineSource                      MonitorStream
  │                                   │
  │  for each .log file:              │  for each MONITOR line:
  │    read line ──┐                  │    read line ──┐
  │                │                  │                │
  └────────────────┼──────┐           └────────────────┼──────┐
                   │      │                            │      │
                   ▼      ▼                            ▼      ▼
              processLine()                    processLine()
              (file mode:                     (live mode:
               addMemorySample by              addMemorySample from
               value string length,            MEMORY USAGE query,
               skip TtlSampler)                use TtlSampler)
                   │                                 │
                   ▼                                 ▼
         PatternStatsAggregator            PatternStatsAggregator
         PatternClusterer                  PatternClusterer
         CommandParser                     CommandParser
         (extracts value size)             (unchanged)
                   │                                 │
                   ▼                                 ▼
             ReportPrinter                    ReportPrinter
           (full output)                     (full output)
```

## 4. Component Changes

### 4.1 Args — New Parameters

```
--source=live|file           Input source (default: live)
--input-dir=<path>           Directory containing .log files (required when source=file)
```

When `--source=file`:
- `--host`, `--port`, `--password` are ignored (no Redis connection).
- `--duration` is ignored; duration is computed from file timestamps.

### 4.2 FileLineSource (New)

```java
class FileLineSource {
    // Constructor
    FileLineSource(String directoryPath);

    // Reads all .log files, calls handler for each line.
    // Returns the computed duration in seconds.
    double processFiles(MonitorLineHandler handler);
}
```

**Behavior:**
1. List all `.log` files in the directory, sorted by name.
2. For each file:
   a. Print file header: `=== File: <name> (N/M) ===`
   b. Read lines sequentially.
   c. Parse Unix timestamp from each line (first token before space).
   d. Track first-seen and last-seen timestamps per-file and globally (across all files).
   e. Pass each line to `MonitorLineHandler.onLine(line)`.
   f. After file EOF: capture current aggregator snapshot, diff against previous snapshot to compute per-file delta, print per-file summary. DO NOT clear aggregator — patterns accumulate across files.
3. After all files: return `(globalLastTimestamp - globalFirstTimestamp)` as the overall duration.

**Timestamp extraction:**
```java
static double extractTimestamp(String line) {
    int spaceIdx = line.indexOf(' ');
    if (spaceIdx < 0) return -1;
    return Double.parseDouble(line.substring(0, spaceIdx));
}
```

**Per-file summary:**
After each file, capture a snapshot of the aggregator's current state. The delta from the previous snapshot gives per-file-only stats. Call `ReportPrinter.printPerFileSummary(fileName, perFileStats, perFileDuration)` where `perFileDuration` is computed from that file's own first and last timestamp.

### 4.3 CommandParser — Value Size Extraction (New)

New method to extract the total string length of value argument(s):

```java
/**
 * Returns total string length of value argument(s) for memory estimation.
 * For SET/SETEX/PSETEX: length of the value token.
 * For HSET/HMSET/HSETNX: sum of field + value token lengths.
 * For SADD: sum of all member token lengths.
 * For ZADD: sum of member token lengths (scores excluded).
 * Returns -1 if value cannot be determined.
 */
public static long estimateValueSize(ParsedCommand cmd);
```

The `ParsedCommand` already stores the parsed tokens, so `estimateValueSize` works from the parsed token list.

**Token layout by command:**

| Command | Value tokens |
|---------|-------------|
| SET key value | token[2] |
| SETNX/GETSET | token[2] |
| SETEX key sec value | token[3] |
| PSETEX key ms value | token[3] |
| MSET k1 v1 k2 v2... | token[2], token[4], ... (even indices after key) |
| HSET key f v | token[2] + token[3] |
| HMSET key f1 v1 f2 v2... | token[3], token[5], ... (odd indices ≥ 3) |
| HSETNX key f v | token[2] + token[3] |
| SADD key m1 m2... | token[2] + token[3] + ... |
| ZADD key s1 m1 s2 m2... | token[3], token[5], ... (odd indices ≥ 3) |
| RESTORE key ttl serialized | token[3] |

**Design note:** This is an approximation. Redis internal encoding (jemalloc, shared integers, ziplist/listpack, hash-table overhead) means actual memory differs from string length. But the string length serves as a consistent relative metric for ranking patterns by memory impact — patterns with larger values will rank higher, which is the goal.

### 4.4 MemoryIncrementAnalyzer

**Constructor change:**
- Accept source mode; in file mode, skip creating `MemorySamplerThread`, `TtlSampler`, `RedisConnectionFactory`, `sampleQueue`.

**New method `runFileLoop()`:**
```java
private void runFileLoop() {
    FileLineSource source = new FileLineSource(args.getInputDir());
    double durationSec = source.processFiles(new MonitorLineHandler() {
        @Override
        public void onLine(String line) {
            processLine(line);
        }
        @Override
        public void onError(Exception e) {
            System.err.println("[File] Error: " + e.getMessage());
        }
    });
    this.durationSec = durationSec;
}
```

**`processLine()` change:**
Add a `sourceMode` flag. In file mode:
- After `aggregator.recordWrite(pattern)`: call `CommandParser.estimateValueSize(cmd)` to get value string length, then `aggregator.addMemorySample(pattern, valueSize)` directly in the main thread (no async queue).
- Skip `ttlSampler.scheduleDelayedTtl()` (line 141).

**`run()` change:**
```java
public void run() {
    if (args.getSource() == Source.FILE) {
        runFileLoop();
        printReport();
    } else {
        // existing live logic unchanged
    }
}
```

### 4.5 ReportPrinter

**File-mode adaptations:**
- Host/port line: show `Source: <input-dir>` instead of `Host: localhost:6379`.
- Duration: show computed duration from file timestamps.
- Memory columns (`AvgMem`, `Increment`, `BalancedRef`): show estimated value, computed from value string length (same formula as live mode).
- Primary ranking metric: same as live mode (`incrementBytes`).
- TTL note: add footnote `TTL: from inline + EXPIRE commands only (no live sampling)`.
- TTL column: show value when available, `-` when pattern has no TTL data.
- Memory footnote: add footnote `Memory: estimated from value string length` in file mode.

**New method `printPerFileSummary()`:**
A compact table for per-file output:
```
─── File: node-53.log (150.2s, 1,240 writes, 45 patterns) ───
Top 5 patterns by incrementBytes:
  Rank  Pattern                    Writes   WriteRate   AvgTTL     AvgMem   Increment
  ───────────────────────────────────────────────────────────────────────────────────
   1    order:*:detail               320      2.1/s     1800s       512 B    160 KB
   2    user:*:profile               180      1.2/s     3600s       256 B     45 KB
  ...
```

## 5. CLI Arguments Summary

```
--source=live|file             default: live
--input-dir=<path>             required when source=file
--host=<host>                  default: localhost (live only)
--port=<port>                  default: 6379 (live only)
--password=<password>          default: none (live only)
--duration=<sec>               default: 300 (live only)
--samples-per-pattern=<n>      default: 10 (live only)
--ttl-samples-per-pattern=<n>  default: 5 (live only)
--upgrade-threshold=<n>        default: 10
--top-n=<n>                    default: 20
--output=<console|json>        default: console
```

Usage example:
```
java -cp ... MemoryIncrementAnalyzer \
    --source=file \
    --input-dir=~/Dow/y/d/redis_analyze/batch_20260427_143001 \
    --top-n=20 \
    --output=console
```

## 6. Report Format (Console, File Mode)

```
═══════════════════════════════════════════════════════════════════
  Redis Memory Increment Analysis Report
  Source: ~/Dow/y/d/redis_analyze/batch_20260427_143001
  Files: 4  |  Duration: 298.5s (from timestamps)
  Memory: estimated from value string length
  TTL: from inline + EXPIRE commands only (no live sampling)
═══════════════════════════════════════════════════════════════════

─── File: redis-node-53.log (147.2s, 1,240 writes, 45 patterns) ───
Top 5 patterns by incrementBytes:
  (per-file top 5 table)
...

─── File: redis-node-101.log (150.8s, 980 writes, 38 patterns) ───
...

─── File: redis-node-12.log (145.3s, 1,560 writes, 52 patterns) ───
...

【Cross-File Summary — Top N by incrementBytes】
Rank  Pattern                Writes   WriteRate   AvgTTL     AvgMem   Increment    BalancedRef
──────────────────────────────────────────────────────────────────────────────────────────────
 1    order:*:detail          1,240      4.2/s     1800s     512 B    620 KB       3.7 MB
 2    user:*:profile            890      3.0/s     3600s     256 B    223 KB       2.7 MB
 3    cache:*                   670      2.2/s      300s     128 B     84 KB       0.1 MB
 4    session:*                 520      1.7/s        -      64 B     33 KB           -
 5    log:*                     310      1.0/s        -      96 B     30 KB           -
──────────────────────────────────────────────────────────────────────────────────────────────
Total (with TTL)                                                      927 KB       6.5 MB

【No-TTL Key Samples (continuous growth, no expiry limit)】
Pattern            Key                      Memory      Command
──────────────────────────────────────────────────────────────────────
log:*              log:events:20260427      96 B        SET log:events:20260427 ...
... (max 5)
```

JSON mode adapts similarly: `memoryBytes` fields use value string length estimation, `source` replaces `host`/`port`.

## 7. Error Handling

| Scenario | Behavior |
|----------|----------|
| `--input-dir` does not exist | Print error, exit code 1 |
| Directory contains no `.log` files | Print warning, exit code 0 |
| Malformed line (no `]`, no timestamp) | Skip line, continue |
| File read error mid-file | Print warning, skip to next file |
| Empty file | Skip file, continue |
| No commands parsed from all files | Print empty report, exit code 0 |

## 8. Module Placement

All new code in `redis-monitor-analyzer`:
- `FileLineSource.java` — `com.yj.redis.monitor.analyzer.increment`
- Changes to `Args.java`, `MemoryIncrementAnalyzer.java`, `ReportPrinter.java` — same package.

No new dependencies. `FileLineSource` only uses `java.io.*` and `java.nio.file.*`.

## 9. Testing Strategy

| Test | Coverage |
|------|----------|
| `FileLineSourceTest` | Reads a temp directory with sample `.log` files, verifies line count and handler calls |
| `FileLineSourceTest` | Timestamp extraction from various line formats |
| `FileLineSourceTest` | Empty directory, no `.log` files, malformed lines |
| `CommandParserTest` (extend) | `estimateValueSize()` for each tracked command: SET, SETEX, HSET, SADD, ZADD, etc. |
| `CommandParserTest` (extend) | Parse lines from file format (verify same as live format) |
| `ArgsTest` (extend) | `--source=file --input-dir=...` parsing, validation |
| `MemoryIncrementAnalyzerTest` (extend) | File mode: no Redis connection created, TtlSampler not started, memory sample from value size |
| Integration test | Create a temp `.log` file with known commands, run analyzer in file mode, verify report output with estimated memory values |
