# Redis Memory Increment Analyzer — Plan 2: Core Engine

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the core engine — pattern clustering, command parsing, MONITOR stream reading, stats aggregation, and CLI argument parsing. These components wire together the data structures from Plan 1.

**Architecture:** 6 files created in `com.yj.redis.monitor.analyzer.increment` — `PatternClusterer` (incremental prefix clustering with keyToPattern LRU cache), `PatternStatsAggregator` (central in-memory store), `CommandParser` (MONITOR line parser with TTL extraction), `MonitorStream` (Jedis MONITOR wrapper), `Args` (CLI argument POJO). One modification to `RedisConnectionFactory` in `redis-monitor-core` for timeout/password support.

**Tech Stack:** Java 8, JUnit 4.13.2, Mockito 4.11.0, Jedis 4.4.6

**Prerequisite:** Plan 1 Foundation must be complete. All Plan 1 sources compile and tests pass.

---

### Task 2.1: PatternClusterer

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternClusterer.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PatternClustererTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.Map;
import static org.junit.Assert.*;

public class PatternClustererTest {

    @Test
    public void testFirstKeyCreatesFixedPattern() {
        PatternClusterer clusterer = new PatternClusterer(10, 10);
        String pattern = clusterer.cluster("user:123:profile");
        assertEquals("user:123:profile", pattern);
    }

    @Test
    public void testSecondMatchingKeyReturnsSamePattern() {
        PatternClusterer clusterer = new PatternClusterer(10, 10);
        String p1 = clusterer.cluster("user:1001:profile");
        String p2 = clusterer.cluster("user:1002:profile");
        assertEquals(p1, p2);
        assertTrue(p2.contains("user:") && p2.contains(":profile"));
    }

    @Test
    public void testDifferentPrefixesCreateDifferentPatterns() {
        PatternClusterer clusterer = new PatternClusterer(10, 10);
        String p1 = clusterer.cluster("user:123:data");
        String p2 = clusterer.cluster("order:456:info");
        assertNotEquals(p1, p2);
    }

    @Test
    public void testUpgradeThresholdTriggersWildcard() {
        PatternClusterer clusterer = new PatternClusterer(3, 10);
        clusterer.cluster("log:001");
        clusterer.cluster("log:002");
        clusterer.cluster("log:003");
        String pattern = clusterer.cluster("log:004");
        assertTrue(pattern, pattern.contains("*"));
    }

    @Test
    public void testNoColonKeyGoesToPrefixTrie() {
        PatternClusterer clusterer = new PatternClusterer(10, 2);
        String p1 = clusterer.cluster("user_profile_1001");
        String p2 = clusterer.cluster("user_profile_1002");
        String p3 = clusterer.cluster("other_key");
        assertNotNull(p1);
        assertNotNull(p2);
        assertNotNull(p3);
    }

    @Test
    public void testCacheReturnsSamePatternForSameKey() {
        PatternClusterer clusterer = new PatternClusterer(10, 10);
        String p1 = clusterer.cluster("cache:key1");
        String p2 = clusterer.cluster("cache:key1");
        assertEquals(p1, p2);
    }

    @Test
    public void testDifferentSegmentCountDifferentPattern() {
        PatternClusterer clusterer = new PatternClusterer(10, 10);
        String p1 = clusterer.cluster("a:b:c");
        String p2 = clusterer.cluster("a:b:c:d");
        assertNotEquals(p1, p2);
    }

    @Test
    public void testExtractPatternsAtReportTime() {
        PatternClusterer clusterer = new PatternClusterer(10, 3);
        clusterer.cluster("user:1001:profile");
        clusterer.cluster("user:1002:profile");
        clusterer.cluster("user:1003:profile");
        clusterer.cluster("user:1004:settings");
        Map<String, Integer> result = clusterer.getClusterSizes();
        assertFalse(result.isEmpty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PatternClustererTest`
Expected: FAIL with "cannot find symbol: class PatternClusterer"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.*;

public class PatternClusterer {

    private final int upgradeThreshold;
    private final List<PatternClusterState> clusters;
    private final Map<String, String> keyToPattern;
    private final PrefixTrie prefixTrie;

    public PatternClusterer(int upgradeThreshold, int maxCacheSize) {
        this.upgradeThreshold = upgradeThreshold;
        this.clusters = new ArrayList<>();
        this.keyToPattern = new LinkedHashMap<String, String>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > maxCacheSize;
            }
        };
        this.prefixTrie = new PrefixTrie(upgradeThreshold);
    }

    public String cluster(String rawKey) {
        // 1. Check cache
        String cached = keyToPattern.get(rawKey);
        if (cached != null) {
            PatternClusterState state = findClusterByPattern(cached);
            if (state != null) {
                state.incrementCount();
                return cached;
            }
        }

        // 2. Handle no-colon keys via PrefixTrie
        if (!rawKey.contains(":")) {
            return clusterNoColon(rawKey);
        }

        // 3. Split by colon
        String[] segments = rawKey.split(":");

        // 4. Find exact match (FIXED exact, variable type-match)
        PatternClusterState match = findExactMatch(segments);
        if (match != null) {
            match.incrementCount();
            recordDistinctValues(match, segments);
            keyToPattern.put(rawKey, match.getCurrentPattern());
            return match.getCurrentPattern();
        }

        // 5. Try near-match: same segment count, all non-FIXED match,
        //    but some FIXED differ — record distinct values, upgrade if threshold reached
        PatternClusterState nearMatch = findNearMatch(segments);
        if (nearMatch != null) {
            nearMatch.incrementCount();
            keyToPattern.put(rawKey, nearMatch.getCurrentPattern());
            return nearMatch.getCurrentPattern();
        }

        // 6. Create new cluster with all segments FIXED
        List<SegmentType> fixedTypes = new ArrayList<>();
        List<String> fixedValues = new ArrayList<>();
        for (String seg : segments) {
            fixedTypes.add(SegmentType.FIXED);
            fixedValues.add(seg);
        }
        String pattern = String.join(":", segments);
        PatternClusterState newState = new PatternClusterState(pattern, fixedTypes, fixedValues);
        clusters.add(newState);
        keyToPattern.put(rawKey, pattern);
        return pattern;
    }

    public Map<String, Integer> getClusterSizes() {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (PatternClusterState state : clusters) {
            result.put(state.getCurrentPattern(), state.getKeyCount());
        }
        Map<String, Integer> triePatterns = prefixTrie.extractPatterns();
        for (Map.Entry<String, Integer> entry : triePatterns.entrySet()) {
            result.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
        return result;
    }

    private String clusterNoColon(String rawKey) {
        boolean inserted = prefixTrie.insert(rawKey);
        if (!inserted) {
            keyToPattern.put(rawKey, "*");
            return "*";
        }
        keyToPattern.put(rawKey, rawKey);
        return rawKey;
    }

    private PatternClusterState findExactMatch(String[] segments) {
        for (PatternClusterState state : clusters) {
            if (state.getSegmentCount() != segments.length) continue;
            boolean matched = true;
            for (int i = 0; i < segments.length; i++) {
                SegmentType clusterType = state.getSegmentType(i);
                if (clusterType == SegmentType.FIXED) {
                    if (!state.getFixedValue(i).equals(segments[i])) {
                        matched = false;
                        break;
                    }
                } else {
                    if (!clusterType.matches(segments[i])) {
                        matched = false;
                        break;
                    }
                }
            }
            if (matched) return state;
        }
        return null;
    }

    private PatternClusterState findNearMatch(String[] segments) {
        for (PatternClusterState state : clusters) {
            if (state.getSegmentCount() != segments.length) continue;
            int mismatchedIdx = -1;
            boolean viable = true;
            for (int i = 0; i < segments.length; i++) {
                SegmentType clusterType = state.getSegmentType(i);
                if (clusterType == SegmentType.FIXED) {
                    if (!state.getFixedValue(i).equals(segments[i])) {
                        if (mismatchedIdx >= 0) {
                            viable = false; // more than one mismatch
                            break;
                        }
                        mismatchedIdx = i;
                    }
                } else {
                    if (!clusterType.matches(segments[i])) {
                        viable = false;
                        break;
                    }
                }
            }
            if (viable && mismatchedIdx >= 0) {
                // Record new distinct value; upgrade if threshold reached
                boolean upgraded = state.recordDistinctValue(mismatchedIdx, segments[mismatchedIdx], upgradeThreshold);
                if (upgraded) {
                    // Merging: after upgrade, check if other clusters now have same pattern → merge them
                    mergeDuplicatePatterns();
                }
                return state;
            }
        }
        return null;
    }

    private void recordDistinctValues(PatternClusterState state, String[] segments) {
        boolean anyUpgraded = false;
        for (int i = 0; i < segments.length; i++) {
            if (state.getSegmentType(i) == SegmentType.FIXED) {
                if (!state.getFixedValue(i).equals(segments[i])) {
                    if (state.recordDistinctValue(i, segments[i], upgradeThreshold)) {
                        anyUpgraded = true;
                    }
                }
            }
        }
        if (anyUpgraded) {
            mergeDuplicatePatterns();
        }
    }

    private void mergeDuplicatePatterns() {
        Map<String, PatternClusterState> merged = new LinkedHashMap<>();
        for (PatternClusterState state : clusters) {
            String pattern = state.getCurrentPattern();
            PatternClusterState existing = merged.get(pattern);
            if (existing != null) {
                existing.addKeyCount(state.getKeyCount());
            } else {
                merged.put(pattern, state);
            }
        }
        clusters.clear();
        clusters.addAll(merged.values());
    }

    private PatternClusterState findClusterByPattern(String pattern) {
        for (PatternClusterState state : clusters) {
            if (state.getCurrentPattern().equals(pattern)) return state;
        }
        return null;
    }
}
```

**Note on pattern merging:** When a segment upgrades from FIXED to STRING, two previously-distinct clusters may now share the same pattern (e.g., `user:123:profile` and `user:456:profile` both become `user:*:profile`). `mergeDuplicatePatterns()` consolidates these by keeping the first cluster and summing their keyCounts. This runs after every upgrade.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PatternClustererTest`
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternClusterer.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PatternClustererTest.java
git commit -m "feat: add PatternClusterer with incremental clustering and LRU cache"
```

---

### Task 2.2: PatternStatsAggregator

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregator.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregatorTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class PatternStatsAggregatorTest {

    @Test
    public void testRecordWriteCreatesNewPatternStats() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 1024L);
        PatternStats stats = agg.getStats("user:*");
        assertNotNull(stats);
        assertEquals(1, stats.getWriteCount());
    }

    @Test
    public void testMultipleWritesIncrementCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 512L);
        agg.recordWrite("user:*", 1024L);
        agg.recordWrite("user:*", 2048L);
        PatternStats stats = agg.getStats("user:*");
        assertEquals(3, stats.getWriteCount());
    }

    @Test
    public void testDistinctPatterns() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 100L);
        agg.recordWrite("order:*", 200L);
        assertNotNull(agg.getStats("user:*"));
        assertNotNull(agg.getStats("order:*"));
    }

    @Test
    public void testAddTtlSample() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 512L);
        agg.addTtlSample("user:*", 3600000L);
        PatternStats stats = agg.getStats("user:*");
        assertFalse(stats.getTtlSamples().isEmpty());
    }

    @Test
    public void testAddMemorySample() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 512L);
        agg.addMemorySample("user:*", 4096L);
        PatternStats stats = agg.getStats("user:*");
        assertEquals(1, stats.getMemorySampleCount());
    }

    @Test
    public void testMarkHasTtlFromCommand() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 512L);
        agg.markTtlFromCommand("user:*");
        PatternStats stats = agg.getStats("user:*");
        assertTrue(stats.isHasTtlFromCommand());
    }

    @Test
    public void testGetTopPatterns() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("user:*", 4096L);
        agg.recordWrite("user:*", 4096L);
        agg.recordWrite("user:*", 4096L);
        agg.recordWrite("order:*", 1024L);
        agg.recordWrite("order:*", 1024L);
        agg.recordWrite("log:*", 512L);

        List<PatternStats> top = agg.getTopPatterns(2, 10.0);
        assertEquals(2, top.size());
        assertEquals("user:*", top.get(0).getPattern());
        assertEquals("order:*", top.get(1).getPattern());
    }

    @Test
    public void testGetTotalWriteCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(5, 10);
        agg.recordWrite("a:*", 100L);
        agg.recordWrite("b:*", 200L);
        agg.recordWrite("a:*", 300L);
        assertEquals(3, agg.getTotalWriteCount());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PatternStatsAggregatorTest`
Expected: FAIL with "cannot find symbol: class PatternStatsAggregator"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PatternStatsAggregator {

    private final Map<String, PatternStats> statsMap;
    private final int ttlSampleCapacity;
    private final int memorySampleCapacity;

    public PatternStatsAggregator(int ttlSampleCapacity, int memorySampleCapacity) {
        this.statsMap = new ConcurrentHashMap<>();
        this.ttlSampleCapacity = ttlSampleCapacity;
        this.memorySampleCapacity = memorySampleCapacity;
    }

    public void recordWrite(String pattern, long memoryBytes) {
        PatternStats stats = statsMap.computeIfAbsent(pattern,
                k -> new PatternStats(pattern, ttlSampleCapacity, memorySampleCapacity));
        stats.incrementWriteCount();
    }

    public void addTtlSample(String pattern, long ttlMillis) {
        PatternStats stats = statsMap.get(pattern);
        if (stats != null) {
            stats.getTtlSamples().add(ttlMillis);
        }
    }

    public void addMemorySample(String pattern, long memoryBytes) {
        PatternStats stats = statsMap.get(pattern);
        if (stats != null) {
            stats.getMemorySamples().add(memoryBytes);
        }
    }

    public void markTtlFromCommand(String pattern) {
        PatternStats stats = statsMap.get(pattern);
        if (stats != null) {
            stats.setHasTtlFromCommand(true);
        }
    }

    public PatternStats getStats(String pattern) {
        return statsMap.get(pattern);
    }

    public Collection<PatternStats> getAllStats() {
        return new ArrayList<>(statsMap.values());
    }

    public List<PatternStats> getTopPatterns(int topN, double durationSec) {
        return statsMap.values().stream()
                .sorted((a, b) -> Double.compare(
                        b.getIncrementBytes(), a.getIncrementBytes()))
                .limit(topN)
                .collect(Collectors.toList());
    }

    public long getTotalWriteCount() {
        return statsMap.values().stream()
                .mapToLong(PatternStats::getWriteCount)
                .sum();
    }

    public int getPatternCount() {
        return statsMap.size();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PatternStatsAggregatorTest`
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregator.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PatternStatsAggregatorTest.java
git commit -m "feat: add PatternStatsAggregator for central in-memory stats"
```

---

### Task 2.3: CommandParser

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/CommandParser.java`
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ParsedCommand.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/CommandParserTest.java`

- [ ] **Step 1: Write ParsedCommand POJO and the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

public class ParsedCommand {

    private final String commandName;
    private final String key;
    private final Long ttlMillis;
    private final String rawLine;

    public ParsedCommand(String commandName, String key, Long ttlMillis, String rawLine) {
        this.commandName = commandName;
        this.key = key;
        this.ttlMillis = ttlMillis;
        this.rawLine = rawLine;
    }

    public String getCommandName() { return commandName; }
    public String getKey() { return key; }
    public Long getTtlMillis() { return ttlMillis; }
    public String getRawLine() { return rawLine; }
    public boolean isWriteCommand() { return ttlMillis == null || ttlMillis >= 0; }
}
```

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class CommandParserTest {

    @Test
    public void testParseSet() {
        // MONITOR format: "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"value1\""
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertNull(cmd.getTtlMillis());
    }

    @Test
    public void testParseSetex() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETEX\" \"key1\" \"3600\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SETEX", cmd.getCommandName());
        assertEquals(Long.valueOf(3600000L), cmd.getTtlMillis());
    }

    @Test
    public void testParseSetWithEx() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"value1\" \"EX\" \"1800\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals(Long.valueOf(1800000L), cmd.getTtlMillis());
    }

    @Test
    public void testParseExpire() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"EXPIRE\" \"key1\" \"600\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("EXPIRE", cmd.getCommandName());
        assertEquals(Long.valueOf(600000L), cmd.getTtlMillis());
    }

    @Test
    public void testParseHset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"HSET\" \"hashkey\" \"field1\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("HSET", cmd.getCommandName());
        assertEquals("hashkey", cmd.getKey());
        assertNull(cmd.getTtlMillis());
    }

    @Test
    public void testParseSadd() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SADD\" \"setkey\" \"member1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SADD", cmd.getCommandName());
        assertEquals("setkey", cmd.getKey());
    }

    @Test
    public void testParseNonWriteCommand() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"GET\" \"key1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNull(cmd);
    }

    @Test
    public void testParseMalformedLine() {
        ParsedCommand cmd = CommandParser.parse("garbage line without proper format");
        assertNull(cmd);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=CommandParserTest`
Expected: FAIL with "cannot find symbol: class CommandParser"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.*;

public final class CommandParser {

    private static final Set<String> WRITE_COMMANDS = new HashSet<>(Arrays.asList(
            "SET", "SETNX", "GETSET", "MSET", "SETEX", "PSETEX",
            "HSET", "HMSET", "HSETNX", "SADD", "ZADD", "RESTORE"
    ));

    private static final Set<String> TTL_COMMANDS = new HashSet<>(Arrays.asList(
            "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"
    ));

    private static final Set<String> INLINE_TTL_COMMANDS = new HashSet<>(Arrays.asList(
            "SETEX", "PSETEX"
    ));

    private CommandParser() {}

    public static ParsedCommand parse(String monitorLine) {
        try {
            List<String> tokens = tokenize(monitorLine);
            if (tokens.isEmpty()) return null;

            String commandName = tokens.get(0).toUpperCase();

            if (TTL_COMMANDS.contains(commandName)) {
                return parseTtlCommand(commandName, tokens, monitorLine);
            }
            if (!WRITE_COMMANDS.contains(commandName)) {
                return null;
            }
            return parseWriteCommand(commandName, tokens, monitorLine);
        } catch (Exception e) {
            return null;
        }
    }

    private static ParsedCommand parseWriteCommand(String cmd, List<String> args, String raw) {
        if (args.size() < 2) return null;
        String key = args.get(1);

        Long ttlMillis = null;
        if (INLINE_TTL_COMMANDS.contains(cmd)) {
            if (args.size() >= 3) {
                ttlMillis = parseTtlArg(cmd, args.get(2));
            }
        } else if ("SET".equals(cmd)) {
            for (int i = 3; i < args.size() - 1; i++) {
                String opt = args.get(i).toUpperCase();
                if (("EX".equals(opt) || "PX".equals(opt)) && i + 1 < args.size()) {
                    long val = Long.parseLong(args.get(i + 1));
                    ttlMillis = "PX".equals(opt) ? val : val * 1000;
                    break;
                }
            }
        } else if ("RESTORE".equals(cmd) && args.size() >= 4) {
            ttlMillis = parseTtlArg(cmd, args.get(3));
        }

        return new ParsedCommand(cmd, key, ttlMillis, raw);
    }

    private static ParsedCommand parseTtlCommand(String cmd, List<String> args, String raw) {
        if (args.size() < 3) return null;
        String key = args.get(1);
        long ttlMillis = parseTtlArg(cmd, args.get(2));
        return new ParsedCommand(cmd, key, ttlMillis, raw);
    }

    private static long parseTtlArg(String cmd, String arg) {
        long val = Long.parseLong(arg);
        switch (cmd) {
            case "SETEX":
            case "EXPIRE":
                return val * 1000;
            case "PSETEX":
            case "PEXPIRE":
                return val;
            case "EXPIREAT":
                return val * 1000 - System.currentTimeMillis();
            case "PEXPIREAT":
                return val - System.currentTimeMillis();
            default:
                return val * 1000;
        }
    }

    private static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        int idx = line.indexOf(']');
        if (idx < 0) return tokens;
        String payload = line.substring(idx + 1).trim();
        boolean inQuote = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < payload.length(); i++) {
            char c = payload.charAt(i);
            if (c == '"') {
                if (inQuote) {
                    tokens.add(sb.toString());
                    sb.setLength(0);
                }
                inQuote = !inQuote;
            } else if (inQuote) {
                sb.append(c);
            }
        }
        return tokens;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=CommandParserTest`
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ParsedCommand.java \
        redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/CommandParser.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/CommandParserTest.java
git commit -m "feat: add CommandParser for MONITOR output line parsing and TTL extraction"
```

---

### Task 2.4: MonitorStream

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MonitorStream.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MonitorStreamTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MonitorStreamTest {

    @Test
    public void testMonitorStreamCreated() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        assertNotNull(stream);
        verify(jedis).monitor();
    }

    @Test
    public void testIsRunningInitiallyTrue() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        assertTrue(stream.isRunning());
    }

    @Test
    public void testStopChangesRunningState() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        stream.stop();
        assertFalse(stream.isRunning());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=MonitorStreamTest`
Expected: FAIL with "cannot find symbol: class MonitorStream"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MonitorStream implements AutoCloseable {

    private final Jedis jedis;
    private final int durationSec;
    private volatile boolean running;
    private BufferedReader reader;

    public MonitorStream(Jedis jedis, int durationSec) {
        this.jedis = jedis;
        this.durationSec = durationSec;
        this.running = true;
    }

    public void start(MonitorLineHandler handler) {
        try {
            jedis.monitor();
            reader = new BufferedReader(new InputStreamReader(jedis.getClient().getInputStream()));
            long deadline = System.currentTimeMillis() + durationSec * 1000L;
            String line;
            while (running && System.currentTimeMillis() < deadline) {
                line = reader.readLine();
                if (line != null) {
                    handler.onLine(line);
                }
            }
        } catch (Exception e) {
            if (running) {
                handler.onError(e);
            }
        } finally {
            running = false;
            close();
        }
    }

    public void stop() {
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        try {
            if (reader != null) reader.close();
        } catch (Exception ignored) {}
        try {
            if (jedis != null) jedis.close();
        } catch (Exception ignored) {}
    }

    public interface MonitorLineHandler {
        void onLine(String line);
        void onError(Exception e);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=MonitorStreamTest`
Expected: all 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/MonitorStream.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/MonitorStreamTest.java
git commit -m "feat: add MonitorStream for Jedis MONITOR command streaming"
```

---

### Task 2.5: Args (CLI argument parsing)

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/Args.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ArgsTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class ArgsTest {

    @Test
    public void testParseDefaults() {
        Args args = Args.parse(new String[0]);
        assertEquals("localhost", args.getHost());
        assertEquals(6379, args.getPort());
        assertEquals(300, args.getDurationSec());
        assertEquals(10, args.getSamplesPerPattern());
        assertEquals(5, args.getTtlSamplesPerPattern());
        assertEquals(10, args.getUpgradeThreshold());
        assertEquals("console", args.getOutput());
        assertEquals(20, args.getTopN());
    }

    @Test
    public void testParseHostAndPort() {
        Args args = Args.parse(new String[]{
                "--host=10.0.0.1", "--port=6380"
        });
        assertEquals("10.0.0.1", args.getHost());
        assertEquals(6380, args.getPort());
    }

    @Test
    public void testParseDuration() {
        Args args = Args.parse(new String[]{"--duration=60"});
        assertEquals(60, args.getDurationSec());
    }

    @Test
    public void testParseSamplesPerPattern() {
        Args args = Args.parse(new String[]{"--samples-per-pattern=20"});
        assertEquals(20, args.getSamplesPerPattern());
    }

    @Test
    public void testParseOutputJson() {
        Args args = Args.parse(new String[]{"--output=json"});
        assertEquals("json", args.getOutput());
    }

    @Test
    public void testParseTopN() {
        Args args = Args.parse(new String[]{"--top-n=5"});
        assertEquals(5, args.getTopN());
    }

    @Test
    public void testParseAllArgs() {
        Args args = Args.parse(new String[]{
                "--host=redis.example.com",
                "--port=6380",
                "--duration=600",
                "--samples-per-pattern=15",
                "--ttl-samples-per-pattern=8",
                "--upgrade-threshold=5",
                "--output=json",
                "--top-n=10"
        });
        assertEquals("redis.example.com", args.getHost());
        assertEquals(6380, args.getPort());
        assertEquals(600, args.getDurationSec());
        assertEquals(15, args.getSamplesPerPattern());
        assertEquals(8, args.getTtlSamplesPerPattern());
        assertEquals(5, args.getUpgradeThreshold());
        assertEquals("json", args.getOutput());
        assertEquals(10, args.getTopN());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDurationThrows() {
        Args.parse(new String[]{"--duration=-1"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownArgThrows() {
        Args.parse(new String[]{"--unknown=value"});
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ArgsTest`
Expected: FAIL with "cannot find symbol: class Args"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.HashMap;
import java.util.Map;

public class Args {

    private String host = "localhost";
    private int port = 6379;
    private int durationSec = 300;
    private int samplesPerPattern = 10;
    private int ttlSamplesPerPattern = 5;
    private int upgradeThreshold = 10;
    private String output = "console";
    private int topN = 20;

    private Args() {}

    public static Args parse(String[] args) {
        Args result = new Args();
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String kv = arg.substring(2);
                int eq = kv.indexOf('=');
                if (eq < 0) {
                    map.put(kv, "true");
                } else {
                    map.put(kv.substring(0, eq), kv.substring(eq + 1));
                }
            }
        }
        for (Map.Entry<String, String> e : map.entrySet()) {
            switch (e.getKey()) {
                case "host":
                    result.host = e.getValue();
                    break;
                case "port":
                    result.port = Integer.parseInt(e.getValue());
                    break;
                case "duration":
                    result.durationSec = Integer.parseInt(e.getValue());
                    if (result.durationSec <= 0) throw new IllegalArgumentException("duration must be > 0");
                    break;
                case "samples-per-pattern":
                    result.samplesPerPattern = Integer.parseInt(e.getValue());
                    break;
                case "ttl-samples-per-pattern":
                    result.ttlSamplesPerPattern = Integer.parseInt(e.getValue());
                    break;
                case "upgrade-threshold":
                    result.upgradeThreshold = Integer.parseInt(e.getValue());
                    break;
                case "output":
                    result.output = e.getValue();
                    if (!"console".equals(result.output) && !"json".equals(result.output)) {
                        throw new IllegalArgumentException("output must be 'console' or 'json'");
                    }
                    break;
                case "top-n":
                    result.topN = Integer.parseInt(e.getValue());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: --" + e.getKey());
            }
        }
        return result;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public int getDurationSec() { return durationSec; }
    public int getSamplesPerPattern() { return samplesPerPattern; }
    public int getTtlSamplesPerPattern() { return ttlSamplesPerPattern; }
    public int getUpgradeThreshold() { return upgradeThreshold; }
    public String getOutput() { return output; }
    public int getTopN() { return topN; }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ArgsTest`
Expected: all 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/Args.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ArgsTest.java
git commit -m "feat: add Args CLI argument parser with defaults and validation"
```

---

### Task 2.6: Plan 2 Final Verification

- [ ] **Step 1: Run all Plan 1 + Plan 2 tests together**

Run: `mvn test -pl redis-monitor-analyzer`
Expected: all tests from both plans PASS

- [ ] **Step 2: Verify full compilation**

Run: `mvn compile -pl redis-monitor-analyzer`
Expected: BUILD SUCCESS

---

**Plan 2 Complete.** Proceed to Plan 3: Pipeline, Sampling & Reporting.
