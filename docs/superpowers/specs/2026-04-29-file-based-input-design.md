# File-Based Input for Memory Analysis — Design Spec

## 1. Background & Goal

The `MemoryIncrementAnalyzer` currently only supports live Redis MONITOR streams as input. We need to add an **offline file-based input mode** that reads pre-recorded MONITOR logs (`.log` files) from a directory, analyzes them, and produces the same pattern statistics report — without requiring a live Redis connection.

Key characteristics:
- Log files are REDIS MONITOR output saved to disk, one line per command.
- Multiple files exist in a batch directory, each from a different Redis node.
- Each file is analyzed independently; a cross-file summary is also output.
- No Redis connection needed in file mode. Memory sampling and delayed TTL queries are skipped.
- Report adapts: memory columns show `N/A`, TTL comes from inline + EXPIRE commands only.

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
               skip TtlSampler,                use TtlSampler,
               skip MemorySampler)             use MemorySampler)
                   │                                 │
                   ▼                                 ▼
         PatternStatsAggregator            PatternStatsAggregator
         PatternClusterer                  PatternClusterer
         CommandParser                     CommandParser
                   │                                 │
                   ▼                                 ▼
             ReportPrinter                    ReportPrinter
          (file-adapted output)            (full output)
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

### 4.3 MemoryIncrementAnalyzer

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
- Skip `ttlSampler.scheduleDelayedTtl()` (line 141).
- Skip `sampleQueue.offer()` (line 148).

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

### 4.4 ReportPrinter

**File-mode adaptations:**
- Host/port line: show `Source: <input-dir>` instead of `Host: localhost:6379`.
- Duration: show computed duration from file timestamps.
- Memory columns (`AvgMem`, `Increment`, `BalancedRef`): show `N/A`.
- Primary ranking metric: change from `incrementBytes` to `writeCount`.
- TTL note: add footnote `TTL: from inline + EXPIRE commands only (no live sampling)`.
- TTL column: show value when available, `-` when pattern has no TTL data.

**New method `printPerFileSummary()`:**
A compact table for per-file output:
```
─── File: node-53.log (150.2s, 1,240 writes, 45 patterns) ───
Top 5 patterns by write count:
  Rank  Pattern                    Writes   WriteRate   AvgTTL
  ─────────────────────────────────────────────────────────────
   1    order:*:detail               320      2.1/s     1800s
   2    user:*:profile               180      1.2/s     3600s
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
  TTL: from inline + EXPIRE commands only (no live sampling)
═══════════════════════════════════════════════════════════════════

─── File: redis-node-53.log (147.2s, 1,240 writes, 45 patterns) ───
Top 5 patterns:
  (per-file top 5 table)
...

─── File: redis-node-101.log (150.8s, 980 writes, 38 patterns) ───
...

─── File: redis-node-12.log (145.3s, 1,560 writes, 52 patterns) ───
...

【Cross-File Summary — Top N by total write count】
Rank  Pattern                Writes   WriteRate   AvgTTL     AvgMem   Increment
────────────────────────────────────────────────────────────────────────────────
 1    order:*:detail          1,240      4.2/s     1800s       N/A       N/A
 2    user:*:profile            890      3.0/s     3600s       N/A       N/A
 3    cache:*                   670      2.2/s      300s       N/A       N/A
 4    session:*                 520      1.7/s        -        N/A       N/A
 5    log:*                     310      1.0/s        -        N/A       N/A
────────────────────────────────────────────────────────────────────────────────
Total (all patterns)          4,780

【No-TTL Key Samples (continuous growth, no expiry limit)】
Pattern            Key                      Command
─────────────────────────────────────────────────────────────────
log:*              log:events:20260427      SET log:events:20260427 ...
... (max 5)
```

JSON mode adapts similarly: `memoryBytes` fields are `null`, `source` replaces `host`/`port`.

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
| `CommandParserTest` (extend) | Parse lines from file format (verify same as live format) |
| `ArgsTest` (extend) | `--source=file --input-dir=...` parsing, validation |
| `MemoryIncrementAnalyzerTest` (extend) | File mode: samplers not started, processLine skips memory/TTL sampling |
| Integration test | Create a temp `.log` file with known commands, run analyzer in file mode, verify report output |
