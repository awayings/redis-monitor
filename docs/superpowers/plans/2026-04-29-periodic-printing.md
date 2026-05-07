# Periodic Intermediate Printing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--print-interval` parameter to print intermediate full reports every N seconds during analysis.

**Architecture:** A single-thread daemon `ScheduledExecutorService` fires periodic print tasks that read the already-thread-safe aggregator. This runs alongside the existing monitor loop (live mode) or file-processing loop (file mode) without changing the core processing logic. Intermediate prints are skipped when no data has been collected yet.

**Tech Stack:** Java 8, JUnit 4, Maven

---

### Task 1: Add `printIntervalSec` to Args

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/Args.java`

- [ ] **Step 1: Add field, key constant, parse logic, getter, toString**

In `Args.java`, make these changes:

**1a. Add `"print-interval"` to VALID_KEYS (line 18):**

```java
private static final Set<String> VALID_KEYS = new HashSet<>(Arrays.asList(
        "host", "port", "duration", "samples-per-pattern",
        "ttl-samples-per-pattern", "upgrade-threshold", "output", "top-n", "password",
        "source", "input-dir", "print-interval"
));
```

**1b. Add field at line 31 (after `inputDir`):**

```java
private final int printIntervalSec;
```

**1c. Add assignment in constructor (after `this.inputDir = builder.inputDir;` at line 44):**

```java
this.printIntervalSec = builder.printIntervalSec;
```

**1d. Add parse case in `parse()` method, inside the switch (after `case "input-dir":` block at line 117):**

```java
case "print-interval":
    builder.printIntervalSec = parseNonNegativeInt("print-interval", value);
    break;
```

**1e. Add `parseNonNegativeInt` helper method after `parsePositiveInt` (after line 140):**

```java
private static int parseNonNegativeInt(String name, String value) {
    try {
        int parsed = Integer.parseInt(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must be non-negative, got: " + value);
        }
        return parsed;
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid " + name + " value: " + value, e);
    }
}
```

**1f. Add getter after `getPassword()` (line 194):**

```java
public int getPrintIntervalSec() {
    return printIntervalSec;
}
```

**1g. Add to `toString()` — in the live-mode branch (line 213), add before the closing `")"`:**

In the live branch:
```java
", printInterval=" + printIntervalSec + "s" +
```

In the file-mode branch (line 203), add similarly before the closing `")"`:
```java
", printInterval=" + printIntervalSec + "s" +
```

**1h. Add to Builder (line 227):**

```java
private int printIntervalSec = 30;
```

- [ ] **Step 2: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/Args.java
git commit -m "feat: add --print-interval argument to Args"
```

---

### Task 2: Write ArgsTest for print-interval parsing

**Files:**
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ArgsTest.java`

- [ ] **Step 1: Write the failing tests**

Add these test methods to `ArgsTest.java` before the closing `}`:

```java
@Test
public void testParsePrintIntervalDefault() {
    Args args = Args.parse(new String[]{});
    assertEquals(30, args.getPrintIntervalSec());
}

@Test
public void testParsePrintIntervalCustom() {
    Args args = Args.parse(new String[]{"--print-interval=10"});
    assertEquals(10, args.getPrintIntervalSec());
}

@Test
public void testParsePrintIntervalZero() {
    Args args = Args.parse(new String[]{"--print-interval=0"});
    assertEquals(0, args.getPrintIntervalSec());
}

@Test(expected = IllegalArgumentException.class)
public void testInvalidPrintIntervalNegativeThrows() {
    Args.parse(new String[]{"--print-interval=-1"});
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
mvn test -pl redis-monitor-analyzer -Dtest=ArgsTest -DfailIfNoTests=false
```
Expected: 4 new tests pass (total ~16 tests, all pass)

- [ ] **Step 3: Commit**

```bash
git add redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ArgsTest.java
git commit -m "test: add print-interval parsing tests"
```

---

### Task 3: Factor out noTtlStore population helper

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java`

- [ ] **Step 1: Extract `populateNoTtlStore()` helper and refactor `printReport()` and `printReportFile()`**

In `MemoryIncrementAnalyzer.java`, add this method after `processLine()` (after line 253):

```java
private void populateNoTtlStore() {
    for (PatternStats stats : aggregator.getAllStats()) {
        if (!stats.getTtlSamples().isEmpty() && stats.getAvgTtlSeconds() <= 0) {
            long memoryBytes = (long) stats.getAvgMemoryBytes();
            String repKey = stats.getRepresentativeKey() != null
                    ? stats.getRepresentativeKey() : stats.getPattern();
            noTtlStore.offer(repKey, stats.getPattern(), memoryBytes, "TTL");
        }
    }
}
```

Then replace `printReport()` (lines 259-286) with:

```java
private void printReport() {
    if (reportPrinted) {
        return;
    }
    reportPrinted = true;

    populateNoTtlStore();

    PrintStream out = System.out;
    if (args.getOutput() == OutputFormat.JSON) {
        printer.printJson(aggregator, noTtlStore, args.getTopN(), out);
    } else {
        printer.printConsole(aggregator, noTtlStore, args.getTopN(), out);
    }

    if (interrupted) {
        out.println("[Interrupted by user -- partial report shown]");
    }
}
```

Replace `printReportFile()` (lines 288-313) with:

```java
private void printReportFile() {
    if (reportPrinted) {
        return;
    }
    reportPrinted = true;

    populateNoTtlStore();

    java.io.PrintStream out = System.out;
    if (args.getOutput() == OutputFormat.JSON) {
        printer.printJsonFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
    } else {
        printer.printConsoleFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
    }

    if (interrupted) {
        out.println("[Interrupted by user -- partial report shown]");
    }
}
```

- [ ] **Step 2: Add imports for `ScheduledExecutorService`, `Executors`, `TimeUnit`**

Add to the import block at the top of the file (after `LinkedBlockingQueue` import, line 12):

```java
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
```

- [ ] **Step 3: Run tests to verify no regression**

```bash
mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest -DfailIfNoTests=false
```
Expected: all existing tests pass

- [ ] **Step 4: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "refactor: extract populateNoTtlStore helper"
```

---

### Task 4: Add periodic printing to live mode

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java`

- [ ] **Step 1: Add new fields and intermediate print methods**

**Add fields** after `private volatile MonitorStream monitorStream;` (line 29):

```java
private ScheduledExecutorService periodicPrinter;
private volatile long startTimeMs;
```

**Add `printIntermediateLive()` method** after `populateNoTtlStore()`:

```java
private void printIntermediateLive() {
    if (aggregator.getTotalWriteCount() == 0) {
        return;
    }
    if (reportPrinted) {
        return;
    }

    populateNoTtlStore();

    PrintStream out = System.out;
    if (args.getOutput() == OutputFormat.JSON) {
        printer.printJson(aggregator, noTtlStore, args.getTopN(), out);
    } else {
        long elapsed = (System.currentTimeMillis() - startTimeMs) / 1000;
        out.println();
        out.println("--- Intermediate Report (elapsed: " + elapsed + "s) ---");
        printer.printConsole(aggregator, noTtlStore, args.getTopN(), out);
    }
}
```

- [ ] **Step 2: Modify `runLiveMode()` to start/stop scheduled printing**

Replace `runLiveMode()` (lines 64-91) with:

```java
private void runLiveMode() {
    PrintStream out = args.getOutput() == OutputFormat.JSON ? System.err : System.out;
    out.println("Redis Memory Increment Analyzer");
    out.println("Config: " + args);
    out.println();

    this.startTimeMs = System.currentTimeMillis();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        interrupted = true;
        if (monitorStream != null) {
            monitorStream.stop();
        }
        if (periodicPrinter != null) {
            periodicPrinter.shutdownNow();
        }
        if (!reportPrinted) {
            printReport();
        }
    }, "ShutdownHook"));

    if (args.getPrintIntervalSec() > 0) {
        periodicPrinter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PeriodicPrinter");
            t.setDaemon(true);
            return t;
        });
        periodicPrinter.scheduleAtFixedRate(
                this::printIntermediateLive,
                args.getPrintIntervalSec(),
                args.getPrintIntervalSec(),
                TimeUnit.SECONDS);
    }

    memorySampler.start();
    ttlSampler.start();

    runMonitorLoop();

    if (periodicPrinter != null) {
        periodicPrinter.shutdown();
        try {
            periodicPrinter.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    if (!reportPrinted) {
        printReport();
    }

    memorySampler.shutdown();
    ttlSampler.shutdown();
}
```

- [ ] **Step 3: Run tests to verify no regression**

```bash
mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest -DfailIfNoTests=false
```
Expected: all existing tests pass

- [ ] **Step 4: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "feat: add periodic intermediate printing to live mode"
```

---

### Task 5: Add periodic printing to file mode

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java`

- [ ] **Step 1: Add `printIntermediateFile()` method**

Add after `printIntermediateLive()`:

```java
private void printIntermediateFile() {
    if (aggregator.getTotalWriteCount() == 0) {
        return;
    }
    if (reportPrinted) {
        return;
    }

    populateNoTtlStore();

    PrintStream out = System.out;
    if (args.getOutput() == OutputFormat.JSON) {
        printer.printJsonFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
    } else {
        long elapsed = (System.currentTimeMillis() - startTimeMs) / 1000;
        out.println();
        out.println("--- Intermediate Report (elapsed: " + elapsed + "s) ---");
        printer.printConsoleFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
    }
}
```

- [ ] **Step 2: Modify `runFileMode()` to start/stop scheduled printing**

Replace `runFileMode()` (lines 93-162) with:

```java
private void runFileMode() {
    PrintStream out = args.getOutput() == OutputFormat.JSON ? System.err : System.out;
    out.println("Redis Memory Increment Analyzer (File Mode)");
    out.println("Config: " + args);
    out.println();

    FileLineSource source = new FileLineSource(args.getInputDir());
    List<File> files = source.listLogFiles();

    if (files.isEmpty()) {
        out.println("Warning: No .log files found in " + args.getInputDir());
        return;
    }

    this.startTimeMs = System.currentTimeMillis();

    // Register shutdown hook after confirming there is work to do
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        interrupted = true;
        if (periodicPrinter != null) {
            periodicPrinter.shutdownNow();
        }
        if (!reportPrinted) {
            printReportFile();
        }
    }, "ShutdownHook"));

    if (args.getPrintIntervalSec() > 0) {
        periodicPrinter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PeriodicPrinter");
            t.setDaemon(true);
            return t;
        });
        periodicPrinter.scheduleAtFixedRate(
                this::printIntermediateFile,
                args.getPrintIntervalSec(),
                args.getPrintIntervalSec(),
                TimeUnit.SECONDS);
    }

    out.println("Found " + files.size() + " .log file(s)");
    out.println();

    // Per-file progress goes to stdout for console, stderr for JSON
    PrintStream perFileOut = args.getOutput() == OutputFormat.JSON ? System.err : System.out;

    double globalFirstTs = Double.MAX_VALUE;
    double globalLastTs = 0;

    for (int i = 0; i < files.size() && !interrupted; i++) {
        File file = files.get(i);
        String fileName = file.getName();
        perFileOut.println("=== File: " + fileName + " (" + (i + 1) + "/" + files.size() + ") ===");

        Map<String, Long> writesBefore = aggregator.getWriteCountSnapshot();

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

                Map<String, Long> writesAfter = aggregator.getWriteCountSnapshot();
                double fileDuration = ts[1] - ts[0];
                printer.printPerFileSummary(fileName, writesBefore, writesAfter,
                        fileDuration, args.getTopN(), perFileOut);
            }
        } catch (IOException e) {
            System.err.println("[File] Error reading " + fileName + ": " + e.getMessage());
        }
    }

    this.durationSec = (globalLastTs > globalFirstTs) ? (globalLastTs - globalFirstTs) : 0;

    if (periodicPrinter != null) {
        periodicPrinter.shutdown();
        try {
            periodicPrinter.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    if (!reportPrinted) {
        printReportFile();
    }
}
```

- [ ] **Step 3: Run all tests to verify no regression**

```bash
mvn test -pl redis-monitor-analyzer -DfailIfNoTests=false
```
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "feat: add periodic intermediate printing to file mode"
```

---

### Task 6: Add `--print-interval` to usage help text

**Files:**
- Modify: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java`

- [ ] **Step 1: Add help line in `printUsage()`**

In `printUsage()` (lines 335-347), add after the `--password` line:

```java
        out.println("  --print-interval=<sec>      Seconds between intermediate reports (default: 30, 0=off)");
```

Also add file mode source options after the password line:

```java
        out.println("  --password=<password>      Redis password (default: none)");
        out.println("  --source=<live|file>       Data source mode (default: live)");
        out.println("  --input-dir=<path>         Input directory for file mode");
        out.println("  --print-interval=<sec>      Seconds between intermediate reports (default: 30, 0=off)");
```

- [ ] **Step 2: Run tests**

```bash
mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest -DfailIfNoTests=false
```
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzer.java
git commit -m "docs: add --print-interval to usage help text"
```

---

### Task 7: Integration test for periodic printing in live mode

**Files:**
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java`

- [ ] **Step 1: Write the integration test**

Add after `testShortDurationProducesValidReport` (after line 645):

```java
// ========================================================================
// Test 21: Periodic intermediate printing produces intermediate reports
// ========================================================================

@Test
public void testPeriodicIntermediatePrintingLiveMode() throws Exception {
    String prefix = "__it_periodic:";

    for (int i = 0; i < 10; i++) {
        jedis.setex(prefix + i, 60, "data");
    }

    // Use a short print interval (1s) with a longer duration (5s)
    // to ensure intermediate reports appear
    Args args = Args.parse(new String[]{
            "--host=" + HOST, "--port=" + PORT, "--duration=5",
            "--samples-per-pattern=3", "--ttl-samples-per-pattern=3",
            "--upgrade-threshold=10", "--top-n=20", "--output=console",
            "--print-interval=1"
    });

    MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream captured = new PrintStream(baos);
    PrintStream original = System.out;
    System.setOut(captured);

    try {
        Thread writer = new Thread(() -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 4500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 10; i++) {
                    w.setex(prefix + i, 60, "more_data");
                }
                try { Thread.sleep(200); } catch (Exception ignored) {}
            }
            w.close();
        }, "TestWriter");
        writer.start();

        analyzer.run();
        writer.join(5000);
    } finally {
        System.setOut(original);
    }

    String output = baos.toString();

    // Should contain the initial header
    assertTrue("Should contain header", output.contains("Redis Memory Increment"));

    // Should contain at least one intermediate report marker
    assertTrue("Should contain intermediate report",
            output.contains("--- Intermediate Report"));

    // Should contain the final report with Host/Duration info
    assertTrue("Should contain Host:", output.contains("Host:"));
    assertTrue("Should contain Duration:", output.contains("Duration:"));

    // Intermediate reports should appear before the final Host: line
    int firstIntermediate = output.indexOf("--- Intermediate Report");
    int hostIdx = output.indexOf("Host:");
    // Host: appears in both intermediate and final, check there's an
    // intermediate report somewhere in the output
    assertTrue("Intermediate report should be present",
            firstIntermediate >= 0);
}
```

- [ ] **Step 2: Run the test**

```bash
mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest#testPeriodicIntermediatePrintingLiveMode -DfailIfNoTests=false
```
Expected: test passes, intermediate reports visible in stdout

- [ ] **Step 3: Commit**

```bash
git add redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java
git commit -m "test: add integration test for periodic intermediate printing"
```

---

### Task 8: Integration test for periodic printing disabled (print-interval=0)

**Files:**
- Modify: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java`

- [ ] **Step 1: Write the test**

Add after the test from Task 7:

```java
// ========================================================================
// Test 22: print-interval=0 produces no intermediate reports
// ========================================================================

@Test
public void testPrintIntervalZeroNoIntermediateReports() throws Exception {
    String prefix = "__it_nointer:";

    for (int i = 0; i < 5; i++) {
        jedis.setex(prefix + i, 60, "data");
    }

    Args args = Args.parse(new String[]{
            "--host=" + HOST, "--port=" + PORT, "--duration=3",
            "--samples-per-pattern=2", "--ttl-samples-per-pattern=2",
            "--upgrade-threshold=10", "--top-n=20", "--output=console",
            "--print-interval=0"
    });

    MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream captured = new PrintStream(baos);
    PrintStream original = System.out;
    System.setOut(captured);

    try {
        Thread writer = new Thread(() -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 5; i++) {
                    w.setex(prefix + i, 60, "more");
                }
                try { Thread.sleep(200); } catch (Exception ignored) {}
            }
            w.close();
        }, "TestWriter");
        writer.start();

        analyzer.run();
        writer.join(5000);
    } finally {
        System.setOut(original);
    }

    String output = baos.toString();

    // Should NOT contain intermediate report markers
    assertFalse("Should not contain intermediate report when print-interval=0",
            output.contains("--- Intermediate Report"));

    // Should still contain the final report
    assertTrue("Should contain final report header",
            output.contains("Redis Memory Increment"));
}
```

- [ ] **Step 2: Run the test**

```bash
mvn test -pl redis-monitor-analyzer -Dtest=MemoryIncrementAnalyzerIntegrationTest#testPrintIntervalZeroNoIntermediateReports -DfailIfNoTests=false
```
Expected: test passes, no intermediate reports in output

- [ ] **Step 3: Commit**

```bash
git add redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MemoryIncrementAnalyzerIntegrationTest.java
git commit -m "test: verify print-interval=0 suppresses intermediate reports"
```

---

### Task 9: Final verification — full build

- [ ] **Step 1: Run full test suite**

```bash
mvn test -pl redis-monitor-analyzer -DfailIfNoTests=false
```
Expected: all tests pass (ArgsTest ~16 tests, IntegrationTest ~22 tests, plus all unit tests)

- [ ] **Step 2: Full build**

```bash
mvn clean package -DskipTests
```
Expected: BUILD SUCCESS
