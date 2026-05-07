package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.*;

public class MemoryIncrementAnalyzerIntegrationTest {

    private static final String HOST = "10.43.28.185";
    private static final int PORT = 6379;

    private Jedis jedis;

    @Before
    public void setUp() {
        jedis = new RedisConnectionFactory(HOST, PORT).createConnection();
    }

    @After
    public void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }

    // ---- Helper ----

    /**
     * Runs the analyzer for the given duration while a writer thread
     * continuously writes keys in the background.
     */
    private PatternStatsAggregator runAnalyzerWithWriter(Args args, Runnable writerTask) throws Exception {
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

        Thread writer = new Thread(writerTask, "TestWriter");
        writer.start();

        analyzer.run();
        writer.join(5000);

        return analyzer.getAggregator();
    }

    private static Args shortArgs(String... extra) {
        String[] base = {
                "--host=" + HOST, "--port=" + PORT, "--duration=3",
                "--samples-per-pattern=3", "--ttl-samples-per-pattern=3",
                "--upgrade-threshold=3", "--top-n=20"
        };
        if (extra.length == 0) {
            return Args.parse(base);
        }
        String[] combined = new String[base.length + extra.length];
        System.arraycopy(base, 0, combined, 0, base.length);
        System.arraycopy(extra, 0, combined, base.length, extra.length);
        return Args.parse(combined);
    }

    // ========================================================================
    // Test 1: Write count accuracy
    // ========================================================================

    @Test
    public void testWriteCountAccuracy() throws Exception {
        String prefix = "__it_wc:";
        int preWrites = 100;

        for (int i = 0; i < preWrites; i++) {
            jedis.setex(prefix + i, 60, "test");
        }

        Args args = shortArgs("--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 20; i++) {
                    w.setex(prefix + i, 60, "updated");
                }
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });

        assertTrue("Should capture writes", agg.getTotalWriteCount() > 0);
        assertTrue("Should have at least one pattern", agg.getPatternCount() > 0);

        // Verify pattern matching our prefix exists
        boolean found = false;
        for (PatternStats stats : agg.getAllStats()) {
            if (stats.getPattern().contains("__it_wc")) {
                found = true;
                assertTrue("Pattern should have writes", stats.getWriteCount() > 0);
                break;
            }
        }
        assertTrue("Should find pattern containing test prefix", found);
    }

    // ========================================================================
    // Test 2: Pattern clustering — colon-separated keys
    // ========================================================================

    @Test
    public void testPatternClusteringWithColonKeys() throws Exception {
        String prefix = "__it_cl:";

        // Pre-write keys with same structure but different middle segments
        for (int i = 0; i < 20; i++) {
            jedis.setex(prefix + i, 60, "data");
        }

        Args args = shortArgs("--upgrade-threshold=3", "--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            int idx = 20;
            while (System.currentTimeMillis() < deadline) {
                w.setex(prefix + idx, 60, "data");
                idx++;
                try { Thread.sleep(50); } catch (Exception ignored) {}
            }
            w.close();
        });

        // With upgradeThreshold=3, the numeric segment should upgrade to *
        boolean hasWildcard = false;
        for (PatternStats stats : agg.getAllStats()) {
            String pattern = stats.getPattern();
            if (pattern.contains("__it_cl") && pattern.contains("*")) {
                hasWildcard = true;
                break;
            }
        }
        assertTrue("Middle segment should be wildcard after upgrade", hasWildcard);
    }

    // ========================================================================
    // Test 3: TTL from inline SETEX command
    // ========================================================================

    @Test
    public void testTtlFromInlineSetexCommand() throws Exception {
        String prefix = "__it_ttl:";

        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i, 120, "data");
        }

        Args args = shortArgs("--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 10; i++) {
                    w.setex(prefix + i, 120, "updated_data");
                }
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });

        assertTrue("Should capture writes", agg.getTotalWriteCount() > 0);
    }

    // ========================================================================
    // Test 4: No-TTL pattern detection (plain SET without TTL)
    // ========================================================================

    @Test
    public void testNoTtlPatternDetection() throws Exception {
        String prefix = "__it_notl:";

        for (int i = 0; i < 10; i++) {
            jedis.set(prefix + i, "persistent_data");
        }

        Args args = shortArgs("--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 10; i++) {
                    w.set(prefix + i, "updated_persistent");
                }
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });

        // At least one pattern should be captured with writes > 0
        boolean foundPattern = false;
        for (PatternStats stats : agg.getAllStats()) {
            if (stats.getPattern().contains("__it_notl")) {
                foundPattern = true;
                break;
            }
        }
        assertTrue("Should find pattern for no-TTL keys", foundPattern);
    }

    // ========================================================================
    // Test 5: Multiple command types
    // ========================================================================

    @Test
    public void testMultipleCommandTypes() throws Exception {
        String prefix = "__it_multi:";

        // Pre-populate keys for each type
        jedis.setex(prefix + "set:0", 60, "v");
        jedis.hset(prefix + "hset:0", "f", "v");
        jedis.sadd(prefix + "sadd:0", "m");

        Args args = shortArgs("--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                w.setex(prefix + "set:0", 60, "v");
                w.hset(prefix + "hset:0", "f", "v");
                w.sadd(prefix + "sadd:0", "m");
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });

        assertTrue("Should capture writes from multiple command types",
                agg.getTotalWriteCount() > 0);
    }

    // ========================================================================
    // Test 6: Top-N filtering
    // ========================================================================

    @Test
    public void testTopNFiltering() throws Exception {
        // Write to 6 distinct patterns
        for (int p = 0; p < 6; p++) {
            String key = "__it_topn:a" + p + ":k";
            jedis.setex(key, 60, "data");
        }

        Args args = shortArgs("--top-n=3", "--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                for (int p = 0; p < 6; p++) {
                    w.setex("__it_topn:a" + p + ":k", 60, "updated");
                }
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });

        List<PatternStats> top = agg.getTopPatterns(3, 3);
        assertTrue("Top-N should not exceed requested count", top.size() <= 3);
    }

    // ========================================================================
    // Test 7: No-colon key clustering via PrefixTrie
    // ========================================================================

    @Test
    public void testNoColonKeyClustering() throws Exception {
        String prefix = "__it_nc_";

        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i, 60, "data");
        }

        Args args = shortArgs("--upgrade-threshold=3", "--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            int idx = 10;
            while (System.currentTimeMillis() < deadline) {
                w.setex(prefix + idx, 60, "data");
                idx++;
                try { Thread.sleep(50); } catch (Exception ignored) {}
            }
            w.close();
        });

        assertTrue("Should capture no-colon keys", agg.getTotalWriteCount() > 0);
    }

    // ========================================================================
    // Test 8: Pattern upgrade threshold triggers wildcard
    // ========================================================================

    @Test
    public void testPatternUpgradeThreshold() throws Exception {
        String prefix = "__it_upg:";

        // Write keys with distinct middle segments to trigger upgrade
        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i + ":data", 60, "val");
        }

        Args args = shortArgs("--upgrade-threshold=3", "--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            int idx = 10;
            while (System.currentTimeMillis() < deadline) {
                w.setex(prefix + idx + ":data", 60, "val");
                idx++;
                try { Thread.sleep(50); } catch (Exception ignored) {}
            }
            w.close();
        });

        // With upgradeThreshold=3 and many distinct values, the numeric
        // segment should upgrade to *
        boolean hasWildcard = false;
        for (PatternStats stats : agg.getAllStats()) {
            if (stats.getPattern().contains("__it_upg") && stats.getPattern().contains("*")) {
                hasWildcard = true;
                break;
            }
        }
        assertTrue("Pattern should contain wildcard after upgrade threshold reached", hasWildcard);
    }

    // ========================================================================
    // Test 9: Console report contains expected sections
    // ========================================================================

    @Test
    public void testConsoleReportContainsExpectedSections() throws Exception {
        String prefix = "__it_rpt:";

        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i, 60, "data");
        }

        Args args = shortArgs("--duration=3", "--output=console");
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
                    for (int i = 0; i < 10; i++) {
                        w.setex(prefix + i, 60, "more");
                    }
                    try { Thread.sleep(100); } catch (Exception ignored) {}
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
        assertTrue("Report should contain 'Redis Memory Increment' header",
                output.contains("Redis Memory Increment"));
        assertTrue("Report should contain 'Host:'",
                output.contains("Host:"));
        assertTrue("Report should contain 'Duration:'",
                output.contains("Duration:"));
        assertTrue("Report should contain 'Total patterns:'",
                output.contains("Total patterns:"));
    }

    // ========================================================================
    // Test 10: JSON output produces valid structure
    // ========================================================================

    @Test
    public void testJsonOutputProducesValidStructure() throws Exception {
        String prefix = "__it_json:";

        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i, 60, "data");
        }

        Args args = shortArgs("--duration=3", "--output=json");
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
                    for (int i = 0; i < 10; i++) {
                        w.setex(prefix + i, 60, "more");
                    }
                    try { Thread.sleep(100); } catch (Exception ignored) {}
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
        assertTrue("JSON should contain meta", output.contains("\"meta\""));
        assertTrue("JSON should contain patterns", output.contains("\"patterns\""));
        assertTrue("JSON should contain host", output.contains("\"host\""));
        assertTrue("JSON should contain durationSec", output.contains("\"durationSec\""));
        assertTrue("JSON should contain totalWriteCount", output.contains("\"totalWriteCount\""));
        assertTrue("JSON should contain noTtlSamples", output.contains("\"noTtlSamples\""));
    }

    // ========================================================================
    // Test 11: No keys captured produces empty report
    // ========================================================================

    @Test
    public void testNoKeysCapturedProducesEmptyReport() {
        Args args = Args.parse(new String[]{
                "--host=" + HOST, "--port=" + PORT, "--duration=1", "--top-n=10"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);
        analyzer.run();
        assertEquals(0, analyzer.getAggregator().getTotalWriteCount());
    }

    // ========================================================================
    // Test 12: Memory increment formula correctness
    // ========================================================================

    @Test
    public void testMemoryIncrementFormulaCorrectness() throws Exception {
        String prefix = "__it_mem:";

        // Write keys with a known value to control memory usage
        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i, 60, "0123456789"); // 10 bytes
        }

        Args args = shortArgs("--samples-per-pattern=5", "--duration=4");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 3500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 10; i++) {
                    w.setex(prefix + i, 60, "0123456789");
                }
                try { Thread.sleep(200); } catch (Exception ignored) {}
            }
            w.close();
        });

        // Find patterns for our test prefix
        for (PatternStats stats : agg.getAllStats()) {
            if (stats.getPattern().contains("__it_mem")) {
                assertTrue("Write count should be positive", stats.getWriteCount() > 0);

                if (stats.getMemorySampleCount() > 0) {
                    // incrementBytes = writeCount * avgMemoryBytes
                    double computed = stats.getWriteCount() * stats.getAvgMemoryBytes();
                    assertEquals("incrementBytes should equal writeCount * avgMemoryBytes",
                            computed, stats.getIncrementBytes(), 0.01);

                    double avgMem = stats.getAvgMemoryBytes();
                    assertTrue("avgMemoryBytes should be positive", avgMem > 0);
                }
            }
        }
    }

    // ========================================================================
    // Test 13: Memory samples collected per pattern
    // ========================================================================

    @Test
    public void testMemorySamplesCollected() throws Exception {
        String prefix = "__it_ms:";

        for (int i = 0; i < 5; i++) {
            jedis.setex(prefix + i, 60, "sample_data_here");
        }

        Args args = shortArgs("--samples-per-pattern=3", "--duration=4");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 3500;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < 5; i++) {
                    w.setex(prefix + i, 60, "sample_data_here");
                }
                try { Thread.sleep(200); } catch (Exception ignored) {}
            }
            w.close();
        });

        // At least one pattern should have memory samples
        boolean hasSamples = false;
        for (PatternStats stats : agg.getAllStats()) {
            if (stats.getPattern().contains("__it_ms") && stats.getMemorySampleCount() > 0) {
                hasSamples = true;
                break;
            }
        }
        assertTrue("At least one pattern should have memory samples collected", hasSamples);
    }

    // ========================================================================
    // Test 14: Multiple pattern segments clustering
    // ========================================================================

    @Test
    public void testMultiSegmentPatternClustering() throws Exception {
        // Three-segment keys: __it_msc:user:0:sessions
        String prefix = "__it_msc";

        for (int i = 0; i < 15; i++) {
            jedis.setex(prefix + ":user:" + i + ":sessions", 60, "session_data");
        }

        Args args = shortArgs("--upgrade-threshold=3", "--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            int idx = 15;
            while (System.currentTimeMillis() < deadline) {
                w.setex(prefix + ":user:" + idx + ":sessions", 60, "session_data");
                idx++;
                try { Thread.sleep(50); } catch (Exception ignored) {}
            }
            w.close();
        });

        assertTrue("Should capture three-segment keys", agg.getTotalWriteCount() > 0);
    }

    // ========================================================================
    // Test 15: HSET and SADD command coverage
    // ========================================================================

    @Test
    public void testHsetAndSaddCoverage() throws Exception {
        String hsetKey = "__it_hset:data";
        String saddKey = "__it_sadd:items";

        jedis.hset(hsetKey, "f1", "v1");
        jedis.sadd(saddKey, "m1", "m2");

        Args args = shortArgs("--duration=3");
        PatternStatsAggregator agg = runAnalyzerWithWriter(args, () -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            long deadline = System.currentTimeMillis() + 2500;
            while (System.currentTimeMillis() < deadline) {
                w.hset(hsetKey, "f1", "v1");
                w.sadd(saddKey, "m3");
                try { Thread.sleep(100); } catch (Exception ignored) {}
            }
            w.close();
        });

        assertTrue("Should capture HSET writes", agg.getTotalWriteCount() > 0);
    }

    // ========================================================================
    // Test 16: Args JSON output flag
    // ========================================================================

    @Test
    public void testJsonOutputFlag() {
        Args args = Args.parse(new String[]{
                "--host=" + HOST, "--port=" + PORT, "--duration=1",
                "--output=json", "--top-n=5"
        });
        assertEquals(OutputFormat.JSON, args.getOutput());
    }

    // ========================================================================
    // Test 17: Partial report on Ctrl-C (simulated via short duration)
    // ========================================================================

    @Test
    public void testShortDurationProducesValidReport() throws Exception {
        String prefix = "__it_short:";

        for (int i = 0; i < 5; i++) {
            jedis.setex(prefix + i, 30, "d");
        }

        Args args = Args.parse(new String[]{
                "--host=" + HOST, "--port=" + PORT, "--duration=2",
                "--samples-per-pattern=2", "--ttl-samples-per-pattern=2",
                "--upgrade-threshold=10", "--top-n=20", "--output=console"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream captured = new PrintStream(baos);
        PrintStream original = System.out;
        System.setOut(captured);

        try {
            Thread writer = new Thread(() -> {
                Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
                long deadline = System.currentTimeMillis() + 1500;
                while (System.currentTimeMillis() < deadline) {
                    for (int i = 0; i < 5; i++) {
                        w.setex(prefix + i, 30, "d");
                    }
                    try { Thread.sleep(100); } catch (Exception ignored) {}
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
        assertTrue("Short duration should still produce a report",
                output.contains("Redis Memory Increment"));
    }

    // ========================================================================
    // Test 18: File mode with temp log file
    // ========================================================================

    @Test
    public void testFileModeWithTempLogFile() throws Exception {
        java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory("file_mode_test_");
        java.io.File logFile = new java.io.File(tempDir.toFile(), "test.log");

        try (java.io.FileWriter fw = new java.io.FileWriter(logFile)) {
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

        assertTrue("Should show 'File Mode'", output.contains("File Mode"));
        assertTrue("Should show 'Source:'", output.contains("Source:"));
        assertTrue("Should contain per-file header", output.contains("--- File: test.log"));
        assertTrue("Should contain memory estimation footnote", output.contains("value string length"));
        assertTrue("Should contain TTL footnote", output.contains("inline"));
        assertTrue("Should have captured writes", analyzer.getAggregator().getTotalWriteCount() > 0);

        logFile.delete();
        tempDir.toFile().delete();
    }

    // ========================================================================
    // Test 19: File mode JSON output
    // ========================================================================

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
        assertTrue("JSON should contain memoryEstimation", output.contains("\"memoryEstimation\": \"value_string_length\""));

        logFile.delete();
        tempDir.toFile().delete();
    }

    // ========================================================================
    // Test 20: File mode empty directory
    // ========================================================================

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
        assertTrue("Should warn about no .log files", output.contains("No .log files found"));
        assertEquals(0, analyzer.getAggregator().getTotalWriteCount());

        tempDir.toFile().delete();
    }

    // ========================================================================
    // Test 21: Periodic intermediate printing produces intermediate reports
    // ========================================================================

    @Test
    public void testPeriodicIntermediatePrintingLiveMode() throws Exception {
        String prefix = "__it_periodic:";

        for (int i = 0; i < 10; i++) {
            jedis.setex(prefix + i, 60, "data");
        }

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

        assertTrue("Should contain intermediate report",
                output.contains("--- Intermediate Report"));
        assertTrue("Should contain final report header",
                output.contains("Redis Memory Increment"));
    }

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

        assertFalse("Should not contain intermediate report when print-interval=0",
                output.contains("--- Intermediate Report"));
        assertTrue("Should contain final report header",
                output.contains("Redis Memory Increment"));
    }
}
