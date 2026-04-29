# File-Based Input for Memory Analysis — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add offline file-based input mode to MemoryIncrementAnalyzer, reading MONITOR logs from a directory and estimating memory from value string length.

**Architecture:** New `FileLineSource` reads `.log` files from a directory and feeds lines through the existing `MonitorLineHandler` callback. `CommandParser` computes value string length eagerly during parse (stored in `ParsedCommand`). `MemoryIncrementAnalyzer` dispatches between live mode (unchanged) and file mode (skips Redis samplers, uses value size for memory). `ReportPrinter` adapts output with file-mode footnotes and per-file summaries.

**Tech Stack:** Java 8, JUnit 4, no new dependencies.

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `Args.java` | Modify | Add `Source` enum, `--source`, `--input-dir` params |
| `ParsedCommand.java` | Modify | Add `valueSize` field |
| `CommandParser.java` | Modify | Add `computeValueSize()`, call it during parse for write commands |
| `PatternStatsAggregator.java` | Modify | Add `getWriteCountSnapshot()` for per-file delta |
| `FileLineSource.java` | **Create** | Read .log files, extract timestamps, feed lines to handler |
| `ReportPrinter.java` | Modify | File-mode header/footnotes, per-file summary method |
| `MemoryIncrementAnalyzer.java` | Modify | Mode dispatch, `runFileLoop()`, conditional construction |
| `ArgsTest.java` | Modify | Tests for `--source`, `--input-dir` |
| `CommandParserTest.java` | Modify | Tests for `valueSize` extraction per command type |
| `FileLineSourceTest.java` | **Create** | Tests for file listing, reading, timestamp extraction, error handling |
| `ReportPrinterTest.java` | Modify | Tests for file-mode header and per-file summary |
| `MemoryIncrementAnalyzerIntegrationTest.java` | Modify | File-mode integration test (no Redis needed) |

---

### Task 1: Add Source enum and --input-dir to Args

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/Args.java`
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ArgsTest.java`

- [ ] **Step 1: Write failing tests in ArgsTest.java**

```java
@Test
public void testParseSourceFileWithInputDir() {
    Args args = Args.parse(new String[]{"--source=file", "--input-dir=/tmp/logs"});
    assertEquals(Args.Source.FILE, args.getSource());
    assertEquals("/tmp/logs", args.getInputDir());
}

@Test
public void testParseSourceDefaultIsLive() {
    Args args = Args.parse(new String[]{});
    assertEquals(Args.Source.LIVE, args.getSource());
    assertNull(args.getInputDir());
}

@Test
public void testParseSourceLive() {
    Args args = Args.parse(new String[]{"--source=live"});
    assertEquals(Args.Source.LIVE, args.getSource());
}

@Test(expected = IllegalArgumentException.class)
public void testInvalidSourceThrows() {
    Args.parse(new String[]{"--source=invalid"});
}

@Test(expected = IllegalArgumentException.class)
public void testSourceFileWithoutInputDirThrows() {
    Args.parse(new String[]{"--source=file"});
}

@Test
public void testFileModeAcceptsInputDirOnly() {
    // When source=file, host/port should not be required
    Args args = Args.parse(new String[]{"--source=file", "--input-dir=/tmp/batch"});
    assertEquals(Args.Source.FILE, args.getSource());
    assertEquals("/tmp/batch", args.getInputDir());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ArgsTest`
Expected: FAIL — "cannot find symbol: variable FILE" / "cannot find symbol: method getSource()" / "cannot find symbol: method getInputDir()"

- [ ] **Step 3: Implement Source enum and new fields in Args.java**

In `Args.java`, add the `Source` enum (inside the class):

```java
public enum Source {
    LIVE, FILE
}
```

Add to `VALID_KEYS`:

```java
private static final Set<String> VALID_KEYS = new HashSet<>(Arrays.asList(
        "host", "port", "duration", "samples-per-pattern",
        "ttl-samples-per-pattern", "upgrade-threshold", "output", "top-n", "password",
        "source", "input-dir"
));
```

Add fields:

```java
private final Source source;
private final String inputDir;
```

Update `Builder` class — add defaults:

```java
private Source source = Source.LIVE;
private String inputDir = null;
```

Update `Args(Builder builder)` constructor — add assignments:

```java
this.source = builder.source;
this.inputDir = builder.inputDir;
```

In `parse()`, add switch cases:

```java
case "source":
    try {
        builder.source = Source.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
                "Invalid source value: " + value + ". Must be: live, file");
    }
    break;
case "input-dir":
    builder.inputDir = value;
    break;
```

After the for-loop, add validation:

```java
if (builder.source == Source.FILE && builder.inputDir == null) {
    throw new IllegalArgumentException(
            "--input-dir is required when --source=file");
}
```

Add getters:

```java
public Source getSource() {
    return source;
}

public String getInputDir() {
    return inputDir;
}
```

Update `toString()` — append after existing format string:

```java
@Override
public String toString() {
    StringBuilder sb = new StringBuilder();
    if (source == Source.FILE) {
        sb.append("source=file, inputDir=").append(inputDir);
    } else {
        sb.append("host=").append(host).append(", port=").append(port)
          .append(", duration=").append(durationSec).append("s");
    }
    sb.append(", samplesPerPattern=").append(samplesPerPattern)
      .append(", ttlSamplesPerPattern=").append(ttlSamplesPerPattern)
      .append(", upgradeThreshold=").append(upgradeThreshold)
      .append(", topN=").append(topN)
      .append(", output=").append(output.name().toLowerCase());
    if (source == Source.LIVE && password != null) {
        sb.append(", password=***");
    }
    return sb.toString();
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ArgsTest`
Expected: PASS — all 18 tests pass

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/Args.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ArgsTest.java
git commit -m "feat: add --source and --input-dir args for file-based input"
```

---

### Task 2: Add valueSize to ParsedCommand and computeValueSize to CommandParser

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ParsedCommand.java`
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/CommandParser.java`
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/CommandParserTest.java`

- [ ] **Step 1: Write failing tests in CommandParserTest.java**

Add these tests at the end of the class (before the closing `}`):

```java
@Test
public void testValueSizeForSet() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"hello\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(5, cmd.getValueSize()); // "hello".length()
}

@Test
public void testValueSizeForSetex() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETEX\" \"key1\" \"100\" \"hello\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(5, cmd.getValueSize()); // "hello".length()
}

@Test
public void testValueSizeForHset() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"HSET\" \"hk\" \"f1\" \"v1\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(4, cmd.getValueSize()); // "f1".length() + "v1".length() = 2+2
}

@Test
public void testValueSizeForSadd() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"SADD\" \"sk\" \"m1\" \"m22\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(5, cmd.getValueSize()); // "m1".length() + "m22".length() = 2+3
}

@Test
public void testValueSizeForZadd() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"ZADD\" \"zk\" \"1.0\" \"m1\" \"2.0\" \"m22\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(5, cmd.getValueSize()); // "m1".length() + "m22".length() = 2+3
}

@Test
public void testValueSizeForMset() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"MSET\" \"k1\" \"v1\" \"k2\" \"v22\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(5, cmd.getValueSize()); // "v1".length() + "v22".length() = 2+3
}

@Test
public void testValueSizeForSetnx() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETNX\" \"lockkey\" \"lockval\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(7, cmd.getValueSize()); // "lockval".length()
}

@Test
public void testValueSizeForExpireIsZero() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"EXPIRE\" \"key1\" \"600\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(-1, cmd.getValueSize()); // TTL-only commands have no value
}

@Test
public void testValueSizeForRestore() {
    String line = "1234567890.123456 [0 127.0.0.1:12345] \"RESTORE\" \"rk\" \"0\" \"serialized_value\"";
    ParsedCommand cmd = CommandParser.parse(line);
    assertNotNull(cmd);
    assertEquals(17, cmd.getValueSize()); // "serialized_value".length()
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=CommandParserTest`
Expected: FAIL — "cannot find symbol: method getValueSize()"

- [ ] **Step 3: Add valueSize field to ParsedCommand**

In `ParsedCommand.java`, add field:

```java
private final long valueSize;
```

Update constructor:

```java
public ParsedCommand(String commandName, String key, Long ttlMillis,
                     String rawLine, boolean isWrite, long valueSize) {
    this.commandName = commandName;
    this.key = key;
    this.ttlMillis = ttlMillis;
    this.rawLine = rawLine;
    this.isWrite = isWrite;
    this.valueSize = valueSize;
}
```

Add getter:

```java
/**
 * Returns the estimated value size in bytes (string length of value argument(s)),
 * or -1 if not applicable (TTL-only commands).
 */
public long getValueSize() {
    return valueSize;
}
```

- [ ] **Step 4: Add computeValueSize to CommandParser and wire it in**

In `CommandParser.java`, add the `computeValueSize` private method:

```java
/**
 * Returns total string length of value argument(s) for memory estimation.
 * Returns -1 if value cannot be determined.
 */
private static long computeValueSize(String cmd, List<String> tokens) {
    switch (cmd) {
        case "SET":
        case "SETNX":
        case "GETSET":
            if (tokens.size() > 2) {
                long base = tokens.get(2).length();
                // Skip past EX/PX/EXAT/PXAT flags to get the value
                // Already counted correctly since SET key val [EX sec|PX ms|...]
                // The value is at token[2] for these commands
                return base;
            }
            return -1;
        case "SETEX":
        case "PSETEX":
            return tokens.size() > 3 ? tokens.get(3).length() : -1;
        case "MSET":
            if (tokens.size() < 3) return -1;
            long msetSize = 0;
            // tokens: [MSET, k1, v1, k2, v2, ...]
            // Values at even indices >= 2
            for (int i = 2; i < tokens.size(); i += 2) {
                msetSize += tokens.get(i).length();
            }
            return msetSize;
        case "HSET":
        case "HSETNX":
            // tokens: [HSET, key, field, value]
            if (tokens.size() > 3) {
                return tokens.get(2).length() + tokens.get(3).length();
            }
            return -1;
        case "HMSET":
            if (tokens.size() < 4) return -1;
            long hmsetSize = 0;
            // tokens: [HMSET, key, f1, v1, f2, v2, ...]
            // Values at odd indices >= 3
            for (int i = 3; i < tokens.size(); i += 2) {
                hmsetSize += tokens.get(i).length();
            }
            return hmsetSize;
        case "SADD":
            if (tokens.size() < 3) return -1;
            long saddSize = 0;
            for (int i = 2; i < tokens.size(); i++) {
                saddSize += tokens.get(i).length();
            }
            return saddSize;
        case "ZADD":
            if (tokens.size() < 4) return -1;
            long zaddSize = 0;
            // tokens: [ZADD, key, score1, member1, score2, member2, ...]
            // Members at odd indices >= 3
            for (int i = 3; i < tokens.size(); i += 2) {
                zaddSize += tokens.get(i).length();
            }
            return zaddSize;
        case "RESTORE":
            return tokens.size() > 3 ? tokens.get(3).length() : -1;
        default:
            return -1;
    }
}
```

Update `parse()` method — compute valueSize for write commands and pass to constructor:

Change the write-command block (around line 42-48) from:

```java
if (WRITE_COMMANDS.contains(cmd)) {
    String key = tokens.size() > 1 ? tokens.get(1) : null;
    if (key == null || key.isEmpty()) {
        return null;
    }
    Long ttl = extractInlineTtl(cmd, tokens);
    return new ParsedCommand(cmd, key, ttl, monitorLine, true);
}
```

To:

```java
if (WRITE_COMMANDS.contains(cmd)) {
    String key = tokens.size() > 1 ? tokens.get(1) : null;
    if (key == null || key.isEmpty()) {
        return null;
    }
    Long ttl = extractInlineTtl(cmd, tokens);
    long valueSize = computeValueSize(cmd, tokens);
    return new ParsedCommand(cmd, key, ttl, monitorLine, true, valueSize);
}
```

Update the TTL-command block (around line 51-65) to pass `-1` for valueSize:

Change from:

```java
return new ParsedCommand(cmd, key, ttl, monitorLine, false);
```

To:

```java
return new ParsedCommand(cmd, key, ttl, monitorLine, false, -1);
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=CommandParserTest`
Expected: PASS — all 27 tests pass

- [ ] **Step 6: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ParsedCommand.java \
        redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/CommandParser.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/CommandParserTest.java
git commit -m "feat: add valueSize computation to CommandParser for file-mode memory estimation"
```

---

### Task 3: Add getWriteCountSnapshot to PatternStatsAggregator

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregator.java`
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregatorTest.java`

- [ ] **Step 1: Write failing test in PatternStatsAggregatorTest.java**

Read the existing test file first, then add:

```java
@Test
public void testGetWriteCountSnapshot() {
    PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
    agg.recordWrite("a:*");
    agg.recordWrite("a:*");
    agg.recordWrite("b:*");

    java.util.Map<String, Long> snapshot = agg.getWriteCountSnapshot();
    assertEquals(2L, snapshot.get("a:*").longValue());
    assertEquals(1L, snapshot.get("b:*").longValue());
    assertEquals(2, snapshot.size());
}

@Test
public void testGetWriteCountSnapshotReturnsCopy() {
    PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
    agg.recordWrite("a:*");

    java.util.Map<String, Long> snap1 = agg.getWriteCountSnapshot();
    agg.recordWrite("a:*");
    java.util.Map<String, Long> snap2 = agg.getWriteCountSnapshot();

    // snap1 should be unaffected by subsequent writes
    assertEquals(1L, snap1.get("a:*").longValue());
    assertEquals(2L, snap2.get("a:*").longValue());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PatternStatsAggregatorTest`
Expected: FAIL — "cannot find symbol: method getWriteCountSnapshot()"

- [ ] **Step 3: Implement getWriteCountSnapshot**

In `PatternStatsAggregator.java`, add the method:

```java
/**
 * Returns a snapshot of write counts per pattern.
 * The returned map is a detached copy — modifications to the aggregator
 * after this call do not affect the snapshot.
 */
public java.util.Map<String, Long> getWriteCountSnapshot() {
    java.util.Map<String, Long> snapshot = new java.util.HashMap<>();
    for (java.util.Map.Entry<String, PatternStats> entry : statsMap.entrySet()) {
        snapshot.put(entry.getKey(), entry.getValue().getWriteCount());
    }
    return snapshot;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PatternStatsAggregatorTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregator.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregatorTest.java
git commit -m "feat: add getWriteCountSnapshot for per-file delta computation"
```

---

### Task 4: Create FileLineSource

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/FileLineSource.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/FileLineSourceTest.java`

- [ ] **Step 1: Write failing tests in FileLineSourceTest.java**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FileLineSourceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testListLogFiles() throws IOException {
        tempFolder.newFile("a.log");
        tempFolder.newFile("b.log");
        tempFolder.newFile("readme.txt");   // non-.log, should be excluded
        tempFolder.newFile("data.LOG");     // case sensitive, should be excluded

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        List<File> files = source.listLogFiles();

        assertEquals(2, files.size());
        assertTrue(files.get(0).getName().endsWith(".log"));
        assertTrue(files.get(1).getName().endsWith(".log"));
    }

    @Test
    public void testExtractTimestamp() {
        String line = "1777329020.191216 [0 10.0.0.53:33414] \"SETEX\" \"key\" \"100\" \"val\"";
        Double ts = FileLineSource.extractTimestamp(line);
        assertEquals(1777329020.191216, ts, 0.000001);
    }

    @Test
    public void testExtractTimestampMalformed() {
        assertNull(FileLineSource.extractTimestamp("no_timestamp_here"));
        assertNull(FileLineSource.extractTimestamp(""));
        assertNull(FileLineSource.extractTimestamp(null));
    }

    @Test
    public void testReadFile() throws IOException {
        File logFile = tempFolder.newFile("test.log");
        try (FileWriter fw = new FileWriter(logFile)) {
            fw.write("100.000 [0 10.0.0.1:1234] \"SET\" \"key1\" \"val1\"\n");
            fw.write("101.000 [0 10.0.0.1:1234] \"SET\" \"key2\" \"val2\"\n");
            fw.write("102.000 [0 10.0.0.1:1234] \"SET\" \"key3\" \"val3\"\n");
        }

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        AtomicInteger lineCount = new AtomicInteger(0);

        double[] ts = source.readFile(logFile, new MonitorLineHandler() {
            @Override
            public void onLine(String line) {
                lineCount.incrementAndGet();
            }
            @Override
            public void onError(Exception e) {
                fail("Should not error: " + e.getMessage());
            }
        });

        assertEquals(3, lineCount.get());
        assertEquals(100.0, ts[0], 0.001);
        assertEquals(102.0, ts[1], 0.001);
    }

    @Test
    public void testReadFileReturnsNegativeTimestampsForEmpty() throws IOException {
        File logFile = tempFolder.newFile("empty.log");

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        double[] ts = source.readFile(logFile, new MonitorLineHandler() {
            @Override public void onLine(String line) { }
            @Override public void onError(Exception e) { }
        });

        assertTrue("First ts should be negative for empty file", ts[0] < 0);
        assertTrue("Last ts should be negative for empty file", ts[1] < 0);
    }

    @Test
    public void testReadFileSkipsMalformedLines() throws IOException {
        File logFile = tempFolder.newFile("malformed.log");
        try (FileWriter fw = new FileWriter(logFile)) {
            fw.write("garbage line\n");
            fw.write("100.000 [0 10.0.0.1:1234] \"SET\" \"key1\" \"val1\"\n");
            fw.write("another garbage\n");
            fw.write("102.000 [0 10.0.0.1:1234] \"SET\" \"key2\" \"val2\"\n");
        }

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        AtomicInteger lineCount = new AtomicInteger(0);

        double[] ts = source.readFile(logFile, new MonitorLineHandler() {
            @Override public void onLine(String line) { lineCount.incrementAndGet(); }
            @Override public void onError(Exception e) { }
        });

        // All lines are passed through; CommandParser filters later
        assertEquals(4, lineCount.get());
        assertEquals(100.0, ts[0], 0.001);
        assertEquals(102.0, ts[1], 0.001);
    }

    @Test
    public void testListLogFilesEmptyDirectory() throws IOException {
        tempFolder.newFolder(); // ensure directory exists but is empty

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        List<File> files = source.listLogFiles();
        assertTrue(files.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNonexistentDirectory() {
        new FileLineSource("/nonexistent/path/xyz");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorPathIsFile() throws IOException {
        File file = tempFolder.newFile("notadir.log");
        new FileLineSource(file.getAbsolutePath());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=FileLineSourceTest`
Expected: FAIL — FileLineSource class not found

- [ ] **Step 3: Implement FileLineSource**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FileLineSource {

    private final File directory;

    public FileLineSource(String directoryPath) {
        this.directory = new File(directoryPath);
        if (!directory.exists()) {
            throw new IllegalArgumentException(
                    "Directory does not exist: " + directoryPath);
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(
                    "Path is not a directory: " + directoryPath);
        }
    }

    /**
     * Lists .log files in the directory, sorted by name.
     */
    public List<File> listLogFiles() {
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".log"));
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }
        List<File> result = Arrays.asList(files);
        result.sort(Comparator.comparing(File::getName));
        return result;
    }

    /**
     * Reads a single log file line by line, delivering each to the handler.
     * Returns [firstTimestamp, lastTimestamp].
     * Both values are -1 if no valid timestamps were found.
     */
    public double[] readFile(File file, MonitorLineHandler handler) throws IOException {
        double firstTs = -1;
        double lastTs = -1;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                handler.onLine(line);

                Double ts = extractTimestamp(line);
                if (ts != null) {
                    if (firstTs < 0) {
                        firstTs = ts;
                    }
                    lastTs = ts;
                }
            }
        } catch (IOException e) {
            handler.onError(e);
            throw e;
        }

        return new double[]{firstTs, lastTs};
    }

    /**
     * Extracts the Unix timestamp from a MONITOR output line.
     * Returns null if the line doesn't start with a valid timestamp.
     */
    public static Double extractTimestamp(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        int spaceIdx = line.indexOf(' ');
        if (spaceIdx < 0) {
            return null;
        }
        try {
            return Double.parseDouble(line.substring(0, spaceIdx));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=FileLineSourceTest`
Expected: PASS — all 8 tests pass

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/FileLineSource.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/FileLineSourceTest.java
git commit -m "feat: add FileLineSource for reading MONITOR log files"
```

---

### Task 5: Update ReportPrinter for file mode and per-file summaries

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ReportPrinter.java`
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ReportPrinterTest.java`

- [ ] **Step 1: Write failing tests in ReportPrinterTest.java**

Add these tests:

```java
@Test
public void testFileModeConsoleReportShowsSource() {
    PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
    aggregator.recordWrite("user:*:profile");
    aggregator.addMemorySample("user:*:profile", 512);
    aggregator.recordWrite("user:*:profile");

    NoTtlKeyStore noTtlStore = new NoTtlKeyStore();
    ReportPrinter printer = ReportPrinter.forFileMode("/tmp/batch_logs");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    printer.printConsoleFile(aggregator, noTtlStore, 10, 300.0, out);
    String output = baos.toString();

    assertTrue("Output should contain Source:",
            output.contains("Source:"));
    assertTrue("Output should contain input dir path",
            output.contains("/tmp/batch_logs"));
    assertTrue("Output should contain memory estimation footnote",
            output.contains("value string length"));
    assertTrue("Output should contain TTL footnote",
            output.contains("inline"));
}

@Test
public void testPerFileSummary() {
    ReportPrinter printer = ReportPrinter.forFileMode("/tmp/batch_logs");

    java.util.Map<String, Long> writesBefore = new java.util.HashMap<>();
    java.util.Map<String, Long> writesAfter = new java.util.HashMap<>();
    writesAfter.put("order:*:detail", 320L);
    writesAfter.put("user:*:profile", 180L);
    writesAfter.put("cache:*", 50L);
    // writesBefore is empty, so deltas = writesAfter

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    printer.printPerFileSummary("node-53.log", writesBefore, writesAfter,
            150.2, 5, out);
    String output = baos.toString();

    assertTrue("Should contain file name",
            output.contains("node-53.log"));
    assertTrue("Should contain duration",
            output.contains("150.2s"));
    assertTrue("Should contain pattern name",
            output.contains("order:*:detail"));
    assertTrue("Should contain write count",
            output.contains("320"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ReportPrinterTest`
Expected: FAIL — "cannot find symbol: method forFileMode()"

- [ ] **Step 3: Implement ReportPrinter changes**

Add fields:

```java
private final boolean fileMode;
private final String inputDir;
```

Add static factory for file mode:

```java
/**
 * Creates a ReportPrinter for file-based input mode.
 */
public static ReportPrinter forFileMode(String inputDir) {
    return new ReportPrinter(inputDir);
}

private ReportPrinter(String inputDir) {
    this.host = null;
    this.port = -1;
    this.durationSec = 0;
    this.samplesPerPattern = 0;
    this.fileMode = true;
    this.inputDir = inputDir;
}
```

Add new method `printConsoleFile`:

```java
/**
 * Prints a console report for file-based input mode.
 */
public void printConsoleFile(PatternStatsAggregator aggregator,
                              NoTtlKeyStore noTtlStore,
                              int topN, double actualDurationSec,
                              PrintStream out) {
    List<PatternStats> topPatterns = aggregator.getTopPatterns(topN, (long) actualDurationSec);
    long totalWrites = aggregator.getTotalWriteCount();
    int totalPatterns = aggregator.getPatternCount();

    // Header
    out.println("=== Redis Memory Increment Analyzer Report ===");
    out.println("Source: " + inputDir);
    out.println("Duration: " + String.format("%.1f", actualDurationSec) + "s (from timestamps)");
    out.println("Total patterns: " + totalPatterns + ", Total writes: " + totalWrites);
    out.println("Memory: estimated from value string length");
    out.println("TTL: from inline + EXPIRE commands only (no live sampling)");
    out.println();

    // Table header
    out.printf("%-6s %-30s %-8s %-10s %-10s %-12s %-12s %-12s%n",
            "Rank", "Pattern", "Writes", "Write/s", "AvgTTL", "AvgMem", "Increment", "Balanced");
    out.println("------+--------------------------------+--------+----------+----------+------------+------------+------------");

    // Table body
    int rank = 1;
    for (PatternStats stats : topPatterns) {
        String pattern = truncate(stats.getPattern(), 30);
        long writes = stats.getWriteCount();
        String writeRate = String.format("%.2f", stats.getWriteRatePerSecond(actualDurationSec));
        String avgTtl = formatTtlSeconds(stats.getAvgTtlSeconds());
        String avgMem = formatBytes((long) stats.getAvgMemoryBytes());
        String increment = formatBytes((long) stats.getIncrementBytes());
        String balanced = formatBytes((long) stats.getBalancedBytes(actualDurationSec));

        out.printf("%-6d %-30s %-8d %-10s %-10s %-12s %-12s %-12s%n",
                rank, pattern, writes, writeRate, avgTtl, avgMem, increment, balanced);
        rank++;
    }

    // Total line
    out.println("------+--------------------------------+--------+----------+----------+------------+------------+------------");
    double totalIncrement = 0;
    double totalBalanced = 0;
    for (PatternStats s : topPatterns) {
        totalIncrement += s.getIncrementBytes();
        totalBalanced += s.getBalancedBytes(actualDurationSec);
    }
    out.printf("%-6s %-30s %-8d %-10s %-10s %-12s %-12s %-12s%n",
            "TOTAL", "", totalWrites, "", "", "",
            formatBytes((long) totalIncrement), formatBytes((long) totalBalanced));
    out.println();

    // No-TTL samples section (same format as live mode)
    List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
    if (!noTtlSamples.isEmpty()) {
        out.println("--- No-TTL Samples (keys without TTL detected) ---");
        out.printf("%-40s %-30s %-12s %-10s%n",
                "Key", "Pattern", "Memory", "Command");
        out.println("----------------------------------------+------------------------------+------------+----------");
        for (NoTtlKeySample sample : noTtlSamples) {
            out.printf("%-40s %-30s %-12s %-10s%n",
                    truncate(sample.getKey(), 40),
                    truncate(sample.getPattern(), 30),
                    formatBytes(sample.getMemoryBytes()),
                    sample.getCommand());
        }
        out.println();
    }
}
```

Add `printPerFileSummary` method:

```java
/**
 * Prints a per-file summary showing patterns found in this file only.
 *
 * @param fileName     the log file name
 * @param writesBefore write counts before this file
 * @param writesAfter  write counts after this file
 * @param fileDurationSec duration from file's own timestamps
 * @param topN         number of top patterns to show
 */
public void printPerFileSummary(String fileName,
                                 java.util.Map<String, Long> writesBefore,
                                 java.util.Map<String, Long> writesAfter,
                                 double fileDurationSec,
                                 int topN,
                                 PrintStream out) {
    // Compute per-file delta
    java.util.Map<String, Long> deltaWrites = new java.util.HashMap<>();
    for (java.util.Map.Entry<String, Long> e : writesAfter.entrySet()) {
        long before = writesBefore.getOrDefault(e.getKey(), 0L);
        long delta = e.getValue() - before;
        if (delta > 0) {
            deltaWrites.put(e.getKey(), delta);
        }
    }

    long totalDelta = deltaWrites.values().stream().mapToLong(Long::longValue).sum();

    out.println();
    out.println("─── File: " + fileName + " ("
            + String.format("%.1f", fileDurationSec) + "s, "
            + totalDelta + " writes, "
            + deltaWrites.size() + " patterns) ───");

    if (deltaWrites.isEmpty()) {
        out.println("  (no write commands found)");
        out.println();
        return;
    }

    // Sort by write count descending
    java.util.List<java.util.Map.Entry<String, Long>> sorted = new java.util.ArrayList<>(deltaWrites.entrySet());
    sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
    int limit = Math.min(topN, sorted.size());

    out.println("Top " + limit + " patterns by write count:");
    out.printf("  %-4s %-30s %-8s %-10s%n", "Rank", "Pattern", "Writes", "Write/s");
    out.println("  " + String.format("%-4s %-30s %-8s %-10s", "----", "------", "------", "-------").replace(' ', '-'));
    for (int i = 0; i < limit; i++) {
        java.util.Map.Entry<String, Long> e = sorted.get(i);
        String pattern = truncate(e.getKey(), 30);
        long writes = e.getValue();
        String rate = String.format("%.2f", writes / Math.max(fileDurationSec, 0.001));
        out.printf("  %-4d %-30s %-8d %-10s%n", i + 1, pattern, writes, rate);
    }
    out.println();
}
```

Add `printJsonFile` method for file-mode JSON output:

```java
/**
 * Prints a JSON report for file-based input mode.
 */
public void printJsonFile(PatternStatsAggregator aggregator,
                           NoTtlKeyStore noTtlStore,
                           int topN, double actualDurationSec,
                           PrintStream out) {
    List<PatternStats> topPatterns = aggregator.getTopPatterns(topN, (long) actualDurationSec);
    long totalWrites = aggregator.getTotalWriteCount();
    int totalPatterns = aggregator.getPatternCount();

    out.println("{");
    out.println("  \"meta\": {");
    out.println("    \"source\": " + jsonEscape(inputDir) + ",");
    out.println("    \"sourceType\": \"file\",");
    out.println("    \"durationSec\": " + String.format("%.1f", actualDurationSec) + ",");
    out.println("    \"totalPatterns\": " + totalPatterns + ",");
    out.println("    \"totalWriteCount\": " + totalWrites + ",");
    out.println("    \"memoryEstimation\": \"value_string_length\"");
    out.println("  },");

    out.println("  \"patterns\": [");
    for (int i = 0; i < topPatterns.size(); i++) {
        PatternStats stats = topPatterns.get(i);
        out.println("    {");
        out.println("      \"pattern\": " + jsonEscape(stats.getPattern()) + ",");
        out.println("      \"writeCount\": " + stats.getWriteCount() + ",");
        out.println("      \"writeRatePerSecond\": " + String.format("%.2f", stats.getWriteRatePerSecond(actualDurationSec)) + ",");
        out.println("      \"avgTtlSec\": " + String.format("%.1f", stats.getAvgTtlSeconds()) + ",");
        out.println("      \"avgMemoryBytes\": " + Math.round(stats.getAvgMemoryBytes()) + ",");
        out.println("      \"incrementBytes\": " + Math.round(stats.getIncrementBytes()) + ",");
        out.println("      \"balancedBytes\": " + Math.round(stats.getBalancedBytes(actualDurationSec)));
        if (i < topPatterns.size() - 1) {
            out.println("    },");
        } else {
            out.println("    }");
        }
    }
    out.println("  ],");

    List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
    out.println("  \"noTtlSamples\": [");
    for (int i = 0; i < noTtlSamples.size(); i++) {
        NoTtlKeySample sample = noTtlSamples.get(i);
        out.println("    {");
        out.println("      \"key\": " + jsonEscape(sample.getKey()) + ",");
        out.println("      \"pattern\": " + jsonEscape(sample.getPattern()) + ",");
        out.println("      \"memoryBytes\": " + sample.getMemoryBytes() + ",");
        out.println("      \"command\": " + jsonEscape(sample.getCommand()));
        if (i < noTtlSamples.size() - 1) {
            out.println("    },");
        } else {
            out.println("    }");
        }
    }
    out.println("  ]");
    out.println("}");
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ReportPrinterTest`
Expected: PASS — all 6 tests pass

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ReportPrinter.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ReportPrinterTest.java
git commit -m "feat: add file-mode output and per-file summary to ReportPrinter"
```

---

### Task 6: Update MemoryIncrementAnalyzer for mode dispatch

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java`

Note: No new tests at this stage — `MemoryIncrementAnalyzer` is the orchestrator whose behavior will be verified by the integration test in Task 7.

- [ ] **Step 1: Make constructor conditional on source mode**

Replace the current constructor with:

```java
public MemoryIncrementAnalyzer(Args args) {
    this.args = args;
    this.clusterer = new PatternClusterer(args.getUpgradeThreshold(), 10000);
    this.aggregator = new PatternStatsAggregator(
            args.getTtlSamplesPerPattern(), args.getSamplesPerPattern());
    this.noTtlStore = new NoTtlKeyStore();
    this.interrupted = false;
    this.reportPrinted = false;

    if (args.getSource() == Args.Source.LIVE) {
        this.sampleQueue = new LinkedBlockingQueue<>();
        this.factory = new RedisConnectionFactory(args.getHost(), args.getPort(), args.getPassword());
        this.memorySampler = new MemorySamplerThread(factory, sampleQueue);
        this.ttlSampler = new TtlSampler(factory, aggregator, args.getTtlSamplesPerPattern());
        this.printer = new ReportPrinter(args.getHost(), args.getPort(),
                args.getDurationSec(), args.getSamplesPerPattern());
    } else {
        this.sampleQueue = null;
        this.factory = null;
        this.memorySampler = null;
        this.ttlSampler = null;
        this.printer = ReportPrinter.forFileMode(args.getInputDir());
    }
}
```

- [ ] **Step 2: Update run() for mode dispatch**

Replace the `run()` method:

```java
public void run() {
    if (args.getSource() == Args.Source.FILE) {
        runFileMode();
    } else {
        runLiveMode();
    }
}

private void runLiveMode() {
    System.out.println("Redis Memory Increment Analyzer");
    System.out.println("Config: " + args);
    System.out.println();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        interrupted = true;
        if (monitorStream != null) {
            monitorStream.stop();
        }
        if (!reportPrinted) {
            printReport();
        }
    }, "ShutdownHook"));

    memorySampler.start();
    ttlSampler.start();

    runMonitorLoop();

    if (!reportPrinted) {
        printReport();
    }

    memorySampler.shutdown();
    ttlSampler.shutdown();
}

private void runFileMode() {
    System.out.println("Redis Memory Increment Analyzer (File Mode)");
    System.out.println("Config: " + args);
    System.out.println();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        interrupted = true;
        if (!reportPrinted) {
            printReportFile();
        }
    }, "ShutdownHook"));

    FileLineSource source = new FileLineSource(args.getInputDir());
    java.util.List<File> files = source.listLogFiles();

    if (files.isEmpty()) {
        System.out.println("Warning: No .log files found in " + args.getInputDir());
        return;
    }

    System.out.println("Found " + files.size() + " .log file(s)");
    System.out.println();

    double globalFirstTs = Double.MAX_VALUE;
    double globalLastTs = 0;
    double durationSec = 0;

    for (int i = 0; i < files.size() && !interrupted; i++) {
        File file = files.get(i);
        String fileName = file.getName();
        System.out.println("=== File: " + fileName + " (" + (i + 1) + "/" + files.size() + ") ===");

        java.util.Map<String, Long> writesBefore = aggregator.getWriteCountSnapshot();

        try {
            double[] ts = source.readFile(file, new MonitorLineHandler() {
                @Override
                public void onLine(String line) {
                    processLine(line);
                }
                @Override
                public void onError(Exception e) {
                    System.err.println("[File] Error: " + e.getMessage());
                }
            });

            if (ts[0] >= 0) {
                if (ts[0] < globalFirstTs) globalFirstTs = ts[0];
                if (ts[1] > globalLastTs) globalLastTs = ts[1];

                java.util.Map<String, Long> writesAfter = aggregator.getWriteCountSnapshot();
                double fileDuration = ts[1] - ts[0];
                printer.printPerFileSummary(fileName, writesBefore, writesAfter,
                        fileDuration, args.getTopN(), System.out);
            }
        } catch (java.io.IOException e) {
            System.err.println("[File] Error reading " + fileName + ": " + e.getMessage());
        }
    }

    durationSec = (globalLastTs > globalFirstTs) ? (globalLastTs - globalFirstTs) : 0;
    this.durationSec = durationSec;

    if (!reportPrinted) {
        printReportFile();
    }
}

private volatile double durationSec;
```

- [ ] **Step 3: Update processLine() for file mode**

Replace the `processLine()` method:

```java
private void processLine(String line) {
    ParsedCommand cmd = CommandParser.parse(line);
    if (cmd == null) {
        return;
    }

    String pattern = clusterer.cluster(cmd.getKey());

    if (cmd.isWriteCommand()) {
        aggregator.recordWrite(pattern);
        aggregator.setRepresentativeKeyIfAbsent(pattern, cmd.getKey());

        if (args.getSource() == Args.Source.LIVE) {
            // Live mode: TTL from inline + delayed sampling
            if (cmd.getTtlMillis() != null) {
                aggregator.addTtlSample(pattern, cmd.getTtlMillis());
                aggregator.markTtlFromCommand(pattern);
            } else {
                ttlSampler.scheduleDelayedTtl(cmd.getKey(), pattern);
            }

            // Memory sampling via async queue
            PatternStats stats = aggregator.getStats(pattern);
            if (stats != null && stats.getMemorySampleCount() < args.getSamplesPerPattern()) {
                sampleQueue.offer(new SampleTask(cmd.getKey(),
                        memory -> aggregator.addMemorySample(pattern, memory)));
            }
        } else {
            // File mode: TTL from command args only, no delayed sampling
            if (cmd.getTtlMillis() != null) {
                aggregator.addTtlSample(pattern, cmd.getTtlMillis());
                aggregator.markTtlFromCommand(pattern);
            }

            // Memory from value string length
            if (cmd.getValueSize() >= 0) {
                aggregator.addMemorySample(pattern, cmd.getValueSize());
            }
        }
    } else if (cmd.getTtlMillis() != null) {
        aggregator.addTtlSample(pattern, cmd.getTtlMillis());
        aggregator.markTtlFromCommand(pattern);
    }
}
```

- [ ] **Step 4: Add printReportFile() method**

```java
private void printReportFile() {
    if (reportPrinted) {
        return;
    }
    reportPrinted = true;

    // Move patterns with no TTL to the noTtlStore (same logic as live mode)
    for (PatternStats stats : aggregator.getAllStats()) {
        if (!stats.getTtlSamples().isEmpty() && stats.getAvgTtlSeconds() <= 0) {
            long memoryBytes = (long) stats.getAvgMemoryBytes();
            String repKey = stats.getRepresentativeKey() != null
                    ? stats.getRepresentativeKey() : stats.getPattern();
            noTtlStore.offer(repKey, stats.getPattern(),
                    memoryBytes, "TTL");
        }
    }

    PrintStream out = System.out;
    if (args.getOutput() == OutputFormat.JSON) {
        printer.printJsonFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
    } else {
        printer.printConsoleFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
    }

    if (interrupted) {
        out.println("[Interrupted by user — partial report shown]");
    }
}
```

- [ ] **Step 5: Verify compilation**

Run: `mvn compile -pl redis-monitor-analyzer`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "feat: add file-mode dispatch to MemoryIncrementAnalyzer"
```

---

### Task 7: Integration test for file mode

**Files:**
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java`

- [ ] **Step 1: Write the integration test**

Add to `MemoryIncrementAnalyzerIntegrationTest.java`:

```java
@Test
public void testFileModeWithTempLogFile() throws Exception {
    // Create a temp directory with a .log file
    java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory("file_mode_test_");
    java.io.File logFile = new java.io.File(tempDir.toFile(), "test.log");

    try (java.io.FileWriter fw = new java.io.FileWriter(logFile)) {
        // Simulate MONITOR output with known commands
        fw.write("100.000 [0 10.0.0.53:33414] \"SETEX\" \"order:100:detail\" \"3600\" \"data_value_1\"\n");
        fw.write("100.500 [0 10.0.0.53:33414] \"SETEX\" \"order:101:detail\" \"3600\" \"data_value_2\"\n");
        fw.write("101.000 [0 10.0.0.53:33414] \"SET\" \"user:1:profile\" \"profile_data_here\" \"EX\" \"7200\"\n");
        fw.write("101.500 [0 10.0.0.101:58684] \"HSET\" \"cache:items\" \"field_a\" \"value_a\"\n");
        fw.write("102.000 [0 10.0.0.101:58684] \"SADD\" \"tags:active\" \"tag1\" \"tag2\" \"tag3\"\n");
        fw.write("102.500 [0 10.0.0.101:58684] \"SET\" \"log:events\" \"log_data_no_ttl\"\n");
        fw.write("103.000 [0 10.0.0.101:58684] \"EXPIRE\" \"log:events\" \"1800\"\n");
    }

    Args args = Args.parse(new String[]{
            "--source=file",
            "--input-dir=" + tempDir.toFile().getAbsolutePath(),
            "--upgrade-threshold=10",
            "--top-n=20",
            "--output=console"
    });

    MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    java.io.PrintStream captured = new java.io.PrintStream(baos);
    java.io.PrintStream original = System.out;
    System.setOut(captured);

    try {
        analyzer.run();
    } finally {
        System.setOut(original);
    }

    String output = baos.toString();

    // Verify file mode header
    assertTrue("Should show 'File Mode'", output.contains("File Mode"));
    assertTrue("Should show 'Source:'", output.contains("Source:"));

    // Verify per-file summary
    assertTrue("Should contain per-file header",
            output.contains("─── File: test.log"));

    // Verify cross-file summary
    // order:*, user:*, cache:*, tags:*, log:* should all be clustered
    assertTrue("Should contain pattern from SETEX",
            output.contains("order:*"));

    // Verify memory estimation footnote
    assertTrue("Should contain memory estimation footnote",
            output.contains("value string length"));

    // Verify TTL footnote
    assertTrue("Should contain TTL footnote",
            output.contains("inline"));

    // Clean up
    logFile.delete();
    tempDir.toFile().delete();
}

@Test
public void testFileModeJsonOutput() throws Exception {
    java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory("file_mode_json_");
    java.io.File logFile = new java.io.File(tempDir.toFile(), "test.log");

    try (java.io.FileWriter fw = new java.io.FileWriter(logFile)) {
        fw.write("100.000 [0 10.0.0.1:1234] \"SETEX\" \"order:100\" \"3600\" \"hello\"\n");
        fw.write("101.000 [0 10.0.0.1:1234] \"SET\" \"user:1\" \"world\" \"EX\" \"7200\"\n");
    }

    Args args = Args.parse(new String[]{
            "--source=file",
            "--input-dir=" + tempDir.toFile().getAbsolutePath(),
            "--output=json"
    });

    MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    java.io.PrintStream captured = new java.io.PrintStream(baos);
    java.io.PrintStream original = System.out;
    System.setOut(captured);

    try {
        analyzer.run();
    } finally {
        System.setOut(original);
    }

    String output = baos.toString();

    assertTrue("JSON should contain meta", output.contains("\"meta\""));
    assertTrue("JSON should contain patterns", output.contains("\"patterns\""));
    assertTrue("JSON should contain sourceType", output.contains("\"sourceType\": \"file\""));
    assertTrue("JSON should contain memoryEstimation",
            output.contains("\"memoryEstimation\": \"value_string_length\""));

    logFile.delete();
    tempDir.toFile().delete();
}

@Test
public void testFileModeEmptyDirectory() throws Exception {
    java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory("file_mode_empty_");

    Args args = Args.parse(new String[]{
            "--source=file",
            "--input-dir=" + tempDir.toFile().getAbsolutePath()
    });

    MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    java.io.PrintStream captured = new java.io.PrintStream(baos);
    java.io.PrintStream original = System.out;
    System.setOut(captured);

    try {
        analyzer.run();
    } finally {
        System.setOut(original);
    }

    String output = baos.toString();
    assertTrue("Should warn about no .log files",
            output.contains("No .log files found"));
    assertEquals(0, analyzer.getAggregator().getTotalWriteCount());

    tempDir.toFile().delete();
}
```

- [ ] **Step 2: Run the integration test**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest#testFileModeWithTempLogFile+testFileModeJsonOutput+testFileModeEmptyDirectory`
Expected: PASS — all 3 file-mode integration tests pass

- [ ] **Step 3: Run all tests to verify no regressions**

Run: `mvn test -pl redis-monitor-analyzer`
Expected: PASS — all tests pass (note: live integration tests require Redis at 10.43.28.185:6379; skip with `-DskipTests` if Redis is unavailable, then file-mode and unit tests still pass with `-Dtest=ArgsTest,CommandParserTest,FileLineSourceTest,PatternStatsAggregatorTest,ReportPrinterTest,MemoryIncrementAnalyzerIntegrationTest#testFileMode*`)

- [ ] **Step 4: Commit**

```bash
git add redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java
git commit -m "test: add file-mode integration tests"
```

---

## Execution Order

Tasks must run in sequence (each depends on the previous):

1. **Task 1** → Args: Source enum, --source, --input-dir
2. **Task 2** → CommandParser: valueSize computation
3. **Task 3** → PatternStatsAggregator: getWriteCountSnapshot
4. **Task 4** → FileLineSource (new file)
5. **Task 5** → ReportPrinter: file-mode output
6. **Task 6** → MemoryIncrementAnalyzer: mode dispatch
7. **Task 7** → Integration tests

Total: 7 tasks, each 2-5 minutes of implementation work.
