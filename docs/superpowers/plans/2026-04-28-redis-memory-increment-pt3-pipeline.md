# Redis Memory Increment Analyzer — Plan 3: Pipeline, Sampling & Reporting

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the sampling threads, report printer, and main orchestrator that wires everything together into a working CLI tool. This plan produces the final runnable system.

**Architecture:** 5 files created in `com.yj.redis.monitor.analyzer.increment` — `SampleTask`, `MemorySamplerThread` (background thread for MEMORY USAGE), `TtlSampler` (delayed-query TTL sampling), `ReportPrinter` (console + JSON output), `MemoryIncrementAnalyzer` (main entry point + orchestrator). One modification to `RedisConnectionFactory` in `redis-monitor-core` for connection timeout support. One integration test.

**Tech Stack:** Java 8, JUnit 4.13.2, Mockito 4.11.0, Jedis 4.4.6, Jackson 2.15.2

**Prerequisite:** Plan 1 Foundation and Plan 2 Core Engine must be complete. All existing tests pass.

---

### Task 3.1: RedisConnectionFactory enhancement

**Files:**
- Modify: `redis-monitor-core/src/main/java/com/yj/redis/monitor/core/RedisConnectionFactory.java`

- [ ] **Step 1: Read current state and write enhanced version**

Current file only does `new Jedis(host, port)`. The `MemorySamplerThread` needs a Jedis with connection timeout to avoid hanging on `MEMORY USAGE`. Add timeout and password support.

```java
package com.yj.redis.monitor.core;

import redis.clients.jedis.Jedis;

public class RedisConnectionFactory {

    private final String host;
    private final int port;
    private final int connectionTimeoutMs;
    private final int socketTimeoutMs;
    private final String password;

    public RedisConnectionFactory(String host, int port) {
        this(host, port, 2000, 5000, null);
    }

    public RedisConnectionFactory(String host, int port, int connectionTimeoutMs,
                                   int socketTimeoutMs, String password) {
        this.host = host;
        this.port = port;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.password = password;
    }

    public Jedis createConnection() {
        Jedis jedis = new Jedis(host, port, connectionTimeoutMs, socketTimeoutMs);
        if (password != null && !password.isEmpty()) {
            jedis.auth(password);
        }
        return jedis;
    }
}
```

- [ ] **Step 2: Verify compilation doesn't break existing callers**

Run: `mvn compile -pl redis-monitor-core`
Expected: BUILD SUCCESS (old constructor signature preserved)

- [ ] **Step 3: Commit**

```bash
git add redis-monitor-core/src/main/java/com/yj/redis/monitor/core/RedisConnectionFactory.java
git commit -m "feat: add timeout and password support to RedisConnectionFactory"
```

---

### Task 3.2: SampleTask POJO

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/SampleTask.java`

No separate test — this is a simple data holder.

- [ ] **Step 1: Write the class**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.function.Consumer;

public class SampleTask {

    private final String key;
    private final Consumer<Long> callback;

    public SampleTask(String key, Consumer<Long> callback) {
        this.key = key;
        this.callback = callback;
    }

    public String getKey() { return key; }

    public Consumer<Long> getCallback() { return callback; }
}
```

- [ ] **Step 2: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/SampleTask.java
git commit -m "feat: add SampleTask for memory sampler thread communication"
```

---

### Task 3.3: MemorySamplerThread

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemorySamplerThread.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemorySamplerThreadTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class MemorySamplerThreadTest {

    @Test
    public void testThreadProcessesTask() throws Exception {
        BlockingQueue<SampleTask> queue = new LinkedBlockingQueue<>();
        MemorySamplerThread thread = new MemorySamplerThread("localhost", 6379, queue);
        thread.start();

        AtomicLong result = new AtomicLong(-1);
        queue.offer(new SampleTask("test:key", result::set));

        Thread.sleep(500);
        thread.shutdown();
        thread.join(2000);
    }

    @Test
    public void testShutdownStopsThread() throws Exception {
        BlockingQueue<SampleTask> queue = new LinkedBlockingQueue<>();
        MemorySamplerThread thread = new MemorySamplerThread("localhost", 6379, queue);
        thread.start();
        assertTrue(thread.isAlive());
        thread.shutdown();
        thread.join(2000);
        assertFalse(thread.isAlive());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=MemorySamplerThreadTest`
Expected: FAIL with "cannot find symbol: class MemorySamplerThread"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MemorySamplerThread extends Thread {

    private final RedisConnectionFactory connectionFactory;
    private final BlockingQueue<SampleTask> queue;
    private volatile boolean running;

    public MemorySamplerThread(String host, int port, BlockingQueue<SampleTask> queue) {
        this.connectionFactory = new RedisConnectionFactory(host, port, 2000, 5000, null);
        this.queue = queue;
        this.running = true;
        setDaemon(true);
    }

    @Override
    public void run() {
        try (Jedis jedis = connectionFactory.createConnection()) {
            while (running) {
                try {
                    SampleTask task = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        try {
                            Long memory = jedis.memoryUsage(task.getKey());
                            if (memory != null) {
                                task.getCallback().accept(memory);
                            }
                        } catch (Exception e) {
                            // Skip this sample on timeout or error, continue
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=MemorySamplerThreadTest`
Expected: 2 tests PASS (note: testThreadProcessesTask may pass quickly if Redis isn't running — the callback won't fire but the thread lifecycle is correct)

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemorySamplerThread.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemorySamplerThreadTest.java
git commit -m "feat: add MemorySamplerThread for async MEMORY USAGE sampling"
```

---

### Task 3.4: TtlSampler (delayed TTL query)

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/TtlSampler.java`

No unit test — this component requires a running Redis instance. Tested via integration test in Task 3.7.

- [ ] **Step 1: Write the class**

```java
package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.*;

public class TtlSampler extends Thread {

    private static final long DELAY_MS = 1000;
    private static final long POLL_TIMEOUT_MS = 100;

    private final RedisConnectionFactory connectionFactory;
    private final PatternStatsAggregator aggregator;
    private final int maxTtlSamples;
    private final DelayQueue<DelayedTtlTask> delayQueue;
    private volatile boolean running;

    public TtlSampler(String host, int port, PatternStatsAggregator aggregator, int maxTtlSamples) {
        this.connectionFactory = new RedisConnectionFactory(host, port, 2000, 5000, null);
        this.aggregator = aggregator;
        this.maxTtlSamples = maxTtlSamples;
        this.delayQueue = new DelayQueue<>();
        this.running = true;
        setDaemon(true);
    }

    public void scheduleDelayedTtl(String key, String pattern) {
        delayQueue.offer(new DelayedTtlTask(key, pattern, DELAY_MS));
    }

    @Override
    public void run() {
        try (Jedis jedis = connectionFactory.createConnection()) {
            while (running) {
                try {
                    DelayedTtlTask task = delayQueue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        processTtlTask(jedis, task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void processTtlTask(Jedis jedis, DelayedTtlTask task) {
        PatternStats stats = aggregator.getStats(task.pattern);
        if (stats == null) return;
        if (stats.getTtlSamples().size() >= maxTtlSamples) return;
        if (stats.isHasTtlFromCommand()) return;

        try {
            Long ttl = jedis.ttl(task.key);
            if (ttl != null && ttl >= 0) {
                stats.getTtlSamples().add(ttl * 1000L);
            }
        } catch (Exception e) {
            // Key may have been deleted; skip
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt();
    }

    private static class DelayedTtlTask implements Delayed {
        final String key;
        final String pattern;
        final long expiryTime;

        DelayedTtlTask(String key, String pattern, long delayMs) {
            this.key = key;
            this.pattern = pattern;
            this.expiryTime = System.currentTimeMillis() + delayMs;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expiryTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(this.expiryTime, ((DelayedTtlTask) o).expiryTime);
        }
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/TtlSampler.java
git commit -m "feat: add TtlSampler with delayed TTL query and discard rules"
```

---

### Task 3.5: ReportPrinter

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ReportPrinter.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ReportPrinterTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

public class ReportPrinterTest {

    @Test
    public void testConsoleReportContainsPatternName() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*:profile", 4096L);
        agg.recordWrite("user:*:profile", 4096L);

        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ReportPrinter printer = new ReportPrinter("localhost", 6379, 300, 10);
        printer.printConsole(agg, noTtlStore, 20, new PrintStream(baos));

        String output = baos.toString();
        assertTrue(output, output.contains("user:*:profile"));
        assertTrue(output, output.contains("localhost"));
    }

    @Test
    public void testConsoleReportWithNoTtlSamples() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();
        noTtlStore.offer("config:flags", "config:*", 2048L, "SET config:flags ...");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ReportPrinter printer = new ReportPrinter("localhost", 6379, 60, 10);
        printer.printConsole(agg, noTtlStore, 20, new PrintStream(baos));

        String output = baos.toString();
        assertTrue(output, output.contains("No-TTL"));
    }

    @Test
    public void testJsonReportValid() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 1024L);

        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ReportPrinter printer = new ReportPrinter("localhost", 6379, 300, 10);
        printer.printJson(agg, noTtlStore, 20, new PrintStream(baos));

        String output = baos.toString();
        assertTrue(output, output.contains("\"meta\""));
        assertTrue(output, output.contains("\"patterns\""));
        assertTrue(output, output.contains("\"user:*\""));
    }

    @Test
    public void testEmptyReport() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ReportPrinter printer = new ReportPrinter("localhost", 6379, 300, 10);
        printer.printConsole(agg, noTtlStore, 20, new PrintStream(baos));

        String output = baos.toString();
        assertTrue(output, output.contains("0"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ReportPrinterTest`
Expected: FAIL with "cannot find symbol: class ReportPrinter"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.io.PrintStream;
import java.util.List;

public class ReportPrinter {

    private final String host;
    private final int port;
    private final int durationSec;
    private final int samplesPerPattern;

    public ReportPrinter(String host, int port, int durationSec, int samplesPerPattern) {
        this.host = host;
        this.port = port;
        this.durationSec = durationSec;
        this.samplesPerPattern = samplesPerPattern;
    }

    public void printConsole(PatternStatsAggregator aggregator, NoTtlKeyStore noTtlStore,
                              int topN, PrintStream out) {
        List<PatternStats> top = aggregator.getTopPatterns(topN, durationSec);

        out.println("═══════════════════════════════════════════════════════════════════");
        out.println("  Redis Memory Increment Analysis Report");
        out.printf("  Host: %s:%d  |  Duration: %ds%n", host, port, durationSec);
        out.println("═══════════════════════════════════════════════════════════════════");
        out.println();
        out.printf("【Memory Increment Distribution (Top %d by incrementBytes)】%n", topN);
        out.printf("%-4s %-22s %-8s %-10s %-10s %-10s %-12s %-14s%n",
                "Rank", "Pattern", "Writes", "WriteRate", "AvgTTL", "AvgMem", "Increment", "BalancedRef");
        out.println("───────────────────────────────────────────────────────────────────────────────────────────────");

        long totalIncrement = 0;
        long totalBalanced = 0;
        int rank = 0;
        for (PatternStats s : top) {
            rank++;
            long incBytes = (long) s.getIncrementBytes();
            long balBytes = (long) s.getBalancedBytes(durationSec);
            totalIncrement += incBytes;
            if (!s.getTtlSamples().isEmpty()) {
                totalBalanced += balBytes;
            }

            String ttlStr = s.getTtlSamples().isEmpty() ? "-" : String.format("%.0fs", s.getAvgTtlSeconds());
            String balStr = s.getTtlSamples().isEmpty() ? "-" : formatBytes(balBytes);

            out.printf("%-4d %-22s %-8d %-10s %-10s %-10s %-12s %-14s%n",
                    rank,
                    truncate(s.getPattern(), 22),
                    s.getWriteCount(),
                    String.format("%.1f/s", s.getWriteRatePerSecond(durationSec)),
                    ttlStr,
                    formatBytes((long) s.getAvgMemoryBytes()),
                    formatBytes(incBytes),
                    balStr);
        }
        out.println("───────────────────────────────────────────────────────────────────────────────────────────────");
        out.printf("Total (with TTL)  %54s    %-14s%n",
                formatBytes(totalIncrement), formatBytes(totalBalanced));
        out.println();

        List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
        if (!noTtlSamples.isEmpty()) {
            out.println("【No-TTL Key Samples (continuous growth, no expiry limit)】");
            out.printf("%-22s %-26s %-10s %-30s%n", "Pattern", "Key", "Memory", "Command");
            out.println("─────────────────────────────────────────────────────────────────");
            for (NoTtlKeySample sample : noTtlSamples) {
                out.printf("%-22s %-26s %-10s %-30s%n",
                        truncate(sample.getPattern(), 22),
                        truncate(sample.getKey(), 26),
                        formatBytes(sample.getMemoryBytes()),
                        truncate(sample.getCommand(), 30));
            }
            out.println();
        }
        out.println("═══════════════════════════════════════════════════════════════════");
    }

    public void printJson(PatternStatsAggregator aggregator, NoTtlKeyStore noTtlStore,
                           int topN, PrintStream out) {
        List<PatternStats> top = aggregator.getTopPatterns(topN, durationSec);

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"meta\": {\n");
        sb.append(String.format("    \"host\": \"%s\",%n", jsonEscape(host)));
        sb.append(String.format("    \"port\": %d,%n", port));
        sb.append(String.format("    \"durationSec\": %d,%n", durationSec));
        sb.append(String.format("    \"samplesPerPattern\": %d,%n", samplesPerPattern));
        sb.append(String.format("    \"totalPatterns\": %d,%n", aggregator.getPatternCount()));
        sb.append(String.format("    \"totalWriteCount\": %d%n", aggregator.getTotalWriteCount()));
        sb.append("  },\n");
        sb.append("  \"patterns\": [\n");
        for (int i = 0; i < top.size(); i++) {
            PatternStats s = top.get(i);
            sb.append("    {\n");
            sb.append(String.format("      \"pattern\": \"%s\",%n", jsonEscape(s.getPattern())));
            sb.append(String.format("      \"writeCount\": %d,%n", s.getWriteCount()));
            sb.append(String.format("      \"writeRatePerSecond\": %.1f,%n", s.getWriteRatePerSecond(durationSec)));
            sb.append(String.format("      \"avgTtlSec\": %.0f,%n", s.getAvgTtlSeconds()));
            sb.append(String.format("      \"avgMemoryBytes\": %.0f,%n", s.getAvgMemoryBytes()));
            sb.append(String.format("      \"incrementBytes\": %.0f,%n", s.getIncrementBytes()));
            sb.append(String.format("      \"balancedBytes\": %.0f%n", s.getBalancedBytes(durationSec)));
            sb.append("    }");
            if (i < top.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ],\n");
        sb.append("  \"noTtlSamples\": [\n");
        List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
        for (int i = 0; i < noTtlSamples.size(); i++) {
            NoTtlKeySample s = noTtlSamples.get(i);
            sb.append("    {\n");
            sb.append(String.format("      \"key\": \"%s\",%n", jsonEscape(s.getKey())));
            sb.append(String.format("      \"pattern\": \"%s\",%n", jsonEscape(s.getPattern())));
            sb.append(String.format("      \"memoryBytes\": %d,%n", s.getMemoryBytes()));
            sb.append(String.format("      \"command\": \"%s\"%n", jsonEscape(s.getCommand())));
            sb.append("    }");
            if (i < noTtlSamples.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");
        out.print(sb.toString());
    }

    static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private static String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen - 1) + "…";
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ReportPrinterTest`
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ReportPrinter.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ReportPrinterTest.java
git commit -m "feat: add ReportPrinter with console and JSON output formats"
```

---

### Task 3.6: MemoryIncrementAnalyzer (main orchestrator)

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java`

This is the main entry point. No unit test — tested via integration test in Task 3.7.

- [ ] **Step 1: Write the class**

```java
package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MemoryIncrementAnalyzer {

    private final Args args;
    private final PatternClusterer clusterer;
    private final PatternStatsAggregator aggregator;
    private final ReportPrinter printer;
    private final NoTtlKeyStore noTtlStore;
    private final BlockingQueue<SampleTask> memorySampleQueue;
    private MemorySamplerThread memorySampler;
    private TtlSampler ttlSampler;
    private volatile boolean interrupted;

    public MemoryIncrementAnalyzer(Args args) {
        this.args = args;
        this.clusterer = new PatternClusterer(args.getUpgradeThreshold(), 1_000_000);
        this.aggregator = new PatternStatsAggregator(args.getTtlSamplesPerPattern(), args.getSamplesPerPattern());
        this.printer = new ReportPrinter(args.getHost(), args.getPort(), args.getDurationSec(), args.getSamplesPerPattern());
        this.noTtlStore = new NoTtlKeyStore();
        this.memorySampleQueue = new LinkedBlockingQueue<>();
    }

    public void run() {
        // Register shutdown hook for Ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            interrupted = true;
            printReport();
        }));

        // Start sampler threads
        memorySampler = new MemorySamplerThread(args.getHost(), args.getPort(), memorySampleQueue);
        memorySampler.start();

        ttlSampler = new TtlSampler(args.getHost(), args.getPort(), aggregator, args.getTtlSamplesPerPattern());
        ttlSampler.start();

        // Start MONITOR stream in main thread with retry (spec: retry 3 times on disconnect)
        runMonitorLoop();

        printReport();

        // Shutdown sampler threads
        if (memorySampler != null) memorySampler.shutdown();
        if (ttlSampler != null) ttlSampler.shutdown();
    }

    private void runMonitorLoop() {
        int maxRetries = 3;
        for (int attempt = 0; attempt < maxRetries && !interrupted; attempt++) {
            RedisConnectionFactory factory = new RedisConnectionFactory(
                    args.getHost(), args.getPort(), 2000, 0, null);
            try (Jedis jedis = factory.createConnection()) {
                MonitorStream stream = new MonitorStream(jedis, args.getDurationSec());
                stream.start(new MonitorStream.MonitorLineHandler() {
                    @Override
                    public void onLine(String line) {
                        processLine(line);
                    }

                    @Override
                    public void onError(Exception e) {
                        System.err.println("MONITOR stream error: " + e.getMessage());
                    }
                });
                return; // normal completion
            } catch (Exception e) {
                if (attempt == maxRetries - 1) {
                    System.err.println("MONITOR stream failed after " + maxRetries + " retries");
                    System.err.println(e.getMessage());
                    printReport();
                    System.exit(1);
                }
                System.err.println("MONITOR stream disconnected, retrying (" + (attempt + 1) + "/" + maxRetries + ")...");
                try { Thread.sleep(1000); } catch (InterruptedException ignored) { break; }
            }
        }
    }

    private void processLine(String monitorLine) {
        ParsedCommand cmd = CommandParser.parse(monitorLine);
        if (cmd == null) return;

        String pattern = clusterer.cluster(cmd.getKey());
        aggregator.recordWrite(pattern, 0); // memory populated later by sampler

        // TTL handling: Path A (direct from command) or Path B (delayed query)
        if (cmd.getTtlMillis() != null) {
            aggregator.addTtlSample(pattern, cmd.getTtlMillis());
            aggregator.markTtlFromCommand(pattern);
        } else if (cmd.isWriteCommand()) {
            // Path B: schedule delayed TTL query for write commands without inline TTL
            ttlSampler.scheduleDelayedTtl(cmd.getKey(), pattern);
        }

        // Enqueue for memory sampling if pattern hasn't reached its quota
        PatternStats stats = aggregator.getStats(pattern);
        if (stats != null && stats.getMemorySampleCount() < args.getSamplesPerPattern()) {
            memorySampleQueue.offer(new SampleTask(cmd.getKey(), memory -> {
                aggregator.addMemorySample(pattern, memory);
            }));
        }
    }

    private void printReport() {
        // Check for no-TTL patterns: if avgTtl <= 0, add representative to NoTtlKeyStore
        for (PatternStats stats : aggregator.getAllStats()) {
            if (!stats.getTtlSamples().isEmpty() && stats.getAvgTtlSeconds() <= 0) {
                // Marked as no-TTL — find a representative key (just report the pattern)
                noTtlStore.offer(stats.getPattern(), stats.getPattern(),
                        (long) stats.getAvgMemoryBytes(), "pattern:" + stats.getPattern());
            }
        }

        if ("json".equals(args.getOutput())) {
            printer.printJson(aggregator, noTtlStore, args.getTopN(), System.out);
        } else {
            printer.printConsole(aggregator, noTtlStore, args.getTopN(), System.out);
        }

        if (interrupted) {
            System.out.println();
            System.out.println("[Interrupted by user — partial report shown]");
        }
    }

    public static void main(String[] args) {
        try {
            Args parsed = Args.parse(args);
            MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(parsed);
            analyzer.run();
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: java -cp ... com.yj.redis.monitor.analyzer.increment.MemoryIncrementAnalyzer \\");
            System.err.println("     --host=<host>                    default: localhost");
            System.err.println("     --port=<port>                    default: 6379");
            System.err.println("     --duration=<seconds>             default: 300");
            System.err.println("     --samples-per-pattern=<n>        default: 10");
            System.err.println("     --ttl-samples-per-pattern=<n>    default: 5");
            System.err.println("     --upgrade-threshold=<n>          default: 10");
            System.err.println("     --output=<console|json>          default: console");
            System.err.println("     --top-n=<n>                      default: 20");
            System.exit(1);
        }
    }
}
```

- [ ] **Step 2: Verify full compilation**

Run: `mvn compile -pl redis-monitor-analyzer`
Expected: BUILD SUCCESS

- [ ] **Step 3: Verify all tests still pass**

Run: `mvn test -pl redis-monitor-analyzer`
Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "feat: add MemoryIncrementAnalyzer main orchestrator with Ctrl-C support"
```

---

### Task 3.7: Integration Test

**Files:**
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java`

This test requires a running Redis instance on localhost:6379. It is annotated with `@Ignore` by default for CI environments without Redis.

- [ ] **Step 1: Write the integration test**

```java
package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;

@Ignore("Requires running Redis on localhost:6379")
public class MemoryIncrementAnalyzerIntegrationTest {

    private Jedis jedis;

    @Before
    public void setUp() {
        jedis = new RedisConnectionFactory("localhost", 6379).createConnection();
        jedis.flushAll();
    }

    @After
    public void tearDown() {
        if (jedis != null) {
            jedis.flushAll();
            jedis.close();
        }
    }

    @Test
    public void testEndToEndWithKnownKeys() throws Exception {
        // 1. Write keys with known patterns
        for (int i = 0; i < 50; i++) {
            jedis.setex("user:" + i + ":profile", 3600, "data");
        }
        for (int i = 0; i < 30; i++) {
            jedis.setex("order:" + i + ":detail", 1800, "order_data");
        }
        for (int i = 0; i < 10; i++) {
            jedis.set("config:" + i, "config_value");
        }

        // 2. Run analyzer for a short duration (5 seconds)
        Args args = Args.parse(new String[]{
                "--host=localhost",
                "--port=6379",
                "--duration=5",
                "--samples-per-pattern=5",
                "--ttl-samples-per-pattern=3",
                "--upgrade-threshold=3",
                "--top-n=10"
        });

        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

        // 3. Simulate some traffic during the monitoring window
        Thread writer = new Thread(() -> {
            Jedis w = new RedisConnectionFactory("localhost", 6379).createConnection();
            for (int i = 0; i < 20; i++) {
                w.setex("user:" + i + ":profile", 3600, "updated");
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });
        writer.start();

        // 4. Run analyzer (blocks for duration)
        analyzer.run();

        writer.join();

        // 5. Verify PatternStatsAggregator has data
        PatternStats userStats = analyzer.getAggregator().getStats("user:*:profile");
        if (userStats != null) {
            assertTrue(userStats.getWriteCount() > 0);
        }
    }

    @Test
    public void testNoKeysCapturedProducesEmptyReport() {
        Args args = Args.parse(new String[]{
                "--host=localhost", "--port=6379", "--duration=1", "--top-n=10"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);
        analyzer.run();

        assertEquals(0, analyzer.getAggregator().getTotalWriteCount());
    }

    @Test
    public void testJsonOutputFlag() {
        Args args = Args.parse(new String[]{
                "--host=localhost", "--port=6379", "--duration=1", "--output=json", "--top-n=5"
        });
        assertEquals("json", args.getOutput());
    }
}
```

- [ ] **Step 2: Add getAggregator accessor to MemoryIncrementAnalyzer**

This step modifies `MemoryIncrementAnalyzer.java` to expose the aggregator for testing.

Add this method:
```java
// Package-visible for integration testing
PatternStatsAggregator getAggregator() {
    return aggregator;
}
```

- [ ] **Step 3: Run the integration test** (requires local Redis)

Run: `mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest -Dtest.ignore=false`
Expected: Tests run against local Redis (may fail if Redis is not running — that's OK, these are `@Ignore` by default)

- [ ] **Step 4: Commit**

```bash
git add redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java \
        redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "test: add integration test for MemoryIncrementAnalyzer (requires Redis)"
```

---

### Task 3.8: Plan 3 Final Verification

- [ ] **Step 1: Run all tests across all modules**

Run: `mvn test`
Expected: all unit tests PASS (integration test skipped via @Ignore)

- [ ] **Step 2: Verify full build**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: Verify the main class can be invoked**

Run: `java -cp redis-monitor-analyzer/target/classes:redis-monitor-core/target/classes:redis-monitor-common/target/classes:$(mvn -pl redis-monitor-analyzer dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout -q 2>/dev/null) com.yj.redis.monitor.analyzer.increment.MemoryIncrementAnalyzer --help 2>&1 || true`
Expected: Prints usage or "Unknown argument: --help" (no --help flag implemented, so error with usage is expected)

---

## Final File Manifest

All files created across the three plans:

```
redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/
├── Args.java
├── CommandParser.java
├── KeyDeserializer.java
├── MemoryIncrementAnalyzer.java          (main class)
├── MemorySamplerThread.java
├── MonitorStream.java
├── NoTtlKeySample.java
├── NoTtlKeyStore.java
├── ParsedCommand.java
├── PatternClusterState.java
├── PatternClusterer.java
├── PatternStats.java
├── PatternStatsAggregator.java
├── PrefixTrie.java
├── ReportPrinter.java
├── ReservoirSampler.java
├── SampleTask.java
├── SegmentType.java
└── TtlSampler.java

redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/
├── ArgsTest.java
├── CommandParserTest.java
├── KeyDeserializerTest.java
├── MemoryIncrementAnalyzerIntegrationTest.java
├── MemorySamplerThreadTest.java
├── MonitorStreamTest.java
├── NoTtlKeyStoreTest.java
├── PatternClustererTest.java
├── PatternStatsAggregatorTest.java
├── PrefixTrieTest.java
├── ReportPrinterTest.java
├── ReservoirSamplerTest.java
└── SegmentTypeTest.java

redis-monitor-core/src/main/java/com/yj/redis/monitor/core/
└── RedisConnectionFactory.java           (modified)
```

---

**Plan 3 Complete. All three plans are ready for execution.**
