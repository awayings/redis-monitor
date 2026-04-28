# Redis Memory Increment Analyzer — Plan 1: Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the foundational data structures and sampling primitives that Plan 2 and Plan 3 depend on. All components are pure Java with no Redis or threading dependencies.

**Architecture:** 7 files created in `com.yj.redis.monitor.analyzer.increment` — `ReservoirSampler<T>` (generic reservoir sampler), `SegmentType` (enum with regex detection), `PatternClusterState` (POJO), `PrefixTrie` (no-colon key clustering), `NoTtlKeySample` + `NoTtlKeyStore` (FIFO sliding window), `KeyDeserializer` (Jdk→UTF-8→hex fallback), `PatternStats` (per-pattern stats POJO). All tests in parallel package under `src/test/`.

**Tech Stack:** Java 8, JUnit 4.13.2, Mockito 4.11.0 (no Spring, no Redis)

**Prerequisite:** Plan 1 must be complete before Plan 2 or Plan 3 can start.

---

### Task 1.1: ReservoirSampler<T>

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ReservoirSampler.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ReservoirSamplerTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class ReservoirSamplerTest {

    @Test
    public void testEmptySamplerMeanIsZero() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(5);
        assertEquals(0.0, sampler.mean(), 0.001);
        assertEquals(0, sampler.size());
    }

    @Test
    public void testSamplerLimitsToCapacity() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(3);
        sampler.add(10L);
        sampler.add(20L);
        sampler.add(30L);
        sampler.add(40L);
        assertEquals(3, sampler.size());
    }

    @Test
    public void testMeanWithFullReservoir() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(3);
        sampler.add(10L);
        sampler.add(20L);
        sampler.add(30L);
        double expected = (10.0 + 20.0 + 30.0) / 3.0;
        assertEquals(expected, sampler.mean(), 0.001);
    }

    @Test
    public void testMeanWithPartialReservoir() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(5);
        sampler.add(100L);
        sampler.add(200L);
        assertEquals(150.0, sampler.mean(), 0.001);
    }

    @Test
    public void testIsEmpty() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(3);
        assertTrue(sampler.isEmpty());
        sampler.add(1L);
        assertFalse(sampler.isEmpty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ReservoirSamplerTest`
Expected: FAIL with "cannot find symbol: class ReservoirSampler"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ReservoirSampler<T extends Number> {

    private final int capacity;
    private final List<T> samples;
    private int totalSeen;

    public ReservoirSampler(int capacity) {
        this.capacity = capacity;
        this.samples = new ArrayList<>(capacity);
        this.totalSeen = 0;
    }

    public void add(T value) {
        totalSeen++;
        if (samples.size() < capacity) {
            samples.add(value);
        } else {
            int idx = ThreadLocalRandom.current().nextInt(totalSeen);
            if (idx < capacity) {
                samples.set(idx, value);
            }
        }
    }

    public int size() {
        return samples.size();
    }

    public boolean isEmpty() {
        return samples.isEmpty();
    }

    public double mean() {
        if (samples.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (T sample : samples) {
            sum += sample.doubleValue();
        }
        return sum / samples.size();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=ReservoirSamplerTest`
Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/ReservoirSampler.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/ReservoirSamplerTest.java
git commit -m "feat: add ReservoirSampler for fixed-size statistical sampling"
```

---

### Task 1.2: SegmentType enum

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/SegmentType.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/SegmentTypeTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class SegmentTypeTest {

    @Test
    public void testDetectNumber() {
        assertEquals(SegmentType.NUMBER, SegmentType.detect("12345"));
        assertEquals(SegmentType.NUMBER, SegmentType.detect("0"));
        assertEquals(SegmentType.NUMBER, SegmentType.detect("00042"));
    }

    @Test
    public void testDetectUuid() {
        assertEquals(SegmentType.UUID, SegmentType.detect("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals(SegmentType.UUID, SegmentType.detect("550e8400e29b41d4a716446655440000"));
    }

    @Test
    public void testDetectDate() {
        assertEquals(SegmentType.DATE, SegmentType.detect("2024-01-15"));
        assertEquals(SegmentType.DATE, SegmentType.detect("2024/01/15"));
        assertEquals(SegmentType.DATE, SegmentType.detect("20240115"));
    }

    @Test
    public void testDetectHex() {
        assertEquals(SegmentType.HEX, SegmentType.detect("deadbeefcafebabe01234567"));
        assertEquals(SegmentType.HEX, SegmentType.detect("a1b2c3d4e5f6a7b8c9d0e1f2"));
    }

    @Test
    public void testDetectFixed() {
        assertEquals(SegmentType.FIXED, SegmentType.detect("users"));
        assertEquals(SegmentType.FIXED, SegmentType.detect("abc"));
        assertEquals(SegmentType.FIXED, SegmentType.detect("user_data_v2"));
    }

    @Test
    public void testMatchNumber() {
        assertTrue(SegmentType.NUMBER.matches("42"));
        assertFalse(SegmentType.NUMBER.matches("abc"));
    }

    @Test
    public void testMatchUuid() {
        assertTrue(SegmentType.UUID.matches("550e8400-e29b-41d4-a716-446655440000"));
        assertFalse(SegmentType.UUID.matches("not-a-uuid"));
    }

    @Test
    public void testMatchFixed() {
        assertTrue(SegmentType.FIXED.matches("anything"));
        assertTrue(SegmentType.FIXED.matches(""));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=SegmentTypeTest`
Expected: FAIL with "cannot find symbol: class SegmentType"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.regex.Pattern;

public enum SegmentType {
    NUMBER("^\\d+$"),
    UUID("^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$"),
    DATE("^\\d{4}[-/]?\\d{2}[-/]?\\d{2}$"),
    HEX("^[0-9a-fA-F]{16,}$"),
    STRING(null),
    FIXED(null);

    private final Pattern pattern;

    SegmentType(String regex) {
        this.pattern = regex != null ? Pattern.compile(regex) : null;
    }

    public static SegmentType detect(String segment) {
        if (NUMBER.pattern != null && NUMBER.pattern.matcher(segment).matches()) {
            return NUMBER;
        }
        if (UUID.pattern != null && UUID.pattern.matcher(segment).matches()) {
            return UUID;
        }
        if (DATE.pattern != null && DATE.pattern.matcher(segment).matches()) {
            return DATE;
        }
        if (HEX.pattern != null && HEX.pattern.matcher(segment).matches()) {
            return HEX;
        }
        return FIXED;
    }

    public boolean matches(String segment) {
        if (this == FIXED || this == STRING) {
            return true;
        }
        return pattern != null && pattern.matcher(segment).matches();
    }

    public boolean isVariable() {
        return this != FIXED;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=SegmentTypeTest`
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/SegmentType.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/SegmentTypeTest.java
git commit -m "feat: add SegmentType enum with regex-based key segment detection"
```

---

### Task 1.3: PatternClusterState POJO

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternClusterState.java`

No separate test file — this is a pure data holder tested indirectly via PatternClusterer in Plan 2.

- [ ] **Step 1: Write the class**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.*;

public class PatternClusterState {

    private String currentPattern;
    private int keyCount;
    private final List<SegmentType> segmentTypes;
    private final List<String> fixedValues;
    private final List<Set<String>> distinctValues;

    public PatternClusterState(String pattern, List<SegmentType> segmentTypes, List<String> fixedValues) {
        this.currentPattern = pattern;
        this.segmentTypes = new ArrayList<>(segmentTypes);
        this.keyCount = 1;
        this.fixedValues = new ArrayList<>(fixedValues);
        this.distinctValues = new ArrayList<>(segmentTypes.size());
        for (int i = 0; i < segmentTypes.size(); i++) {
            if (segmentTypes.get(i) == SegmentType.FIXED && i < fixedValues.size()) {
                Set<String> set = new HashSet<>();
                set.add(fixedValues.get(i));
                distinctValues.add(set);
            } else {
                distinctValues.add(new HashSet<>());
            }
        }
    }

    public String getCurrentPattern() { return currentPattern; }

    public int getKeyCount() { return keyCount; }

    public int getSegmentCount() { return segmentTypes.size(); }

    public SegmentType getSegmentType(int index) { return segmentTypes.get(index); }

    public String getFixedValue(int index) { return fixedValues.get(index); }

    public int getDistinctCount(int index) { return distinctValues.get(index).size(); }

    public void incrementCount() { keyCount++; }

    public void addKeyCount(int n) { keyCount += n; }

    /**
     * Records a new value for a FIXED segment. Returns true if the segment
     * has seen enough distinct values to trigger an upgrade.
     */
    public boolean recordDistinctValue(int index, String value, int upgradeThreshold) {
        if (segmentTypes.get(index) != SegmentType.FIXED) return false;
        distinctValues.get(index).add(value);
        if (distinctValues.get(index).size() >= upgradeThreshold) {
            segmentTypes.set(index, SegmentType.STRING);
            fixedValues.set(index, null);
            rebuildPattern();
            return true;
        }
        return false;
    }

    private void rebuildPattern() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segmentTypes.size(); i++) {
            if (i > 0) sb.append(':');
            SegmentType type = segmentTypes.get(i);
            if (type == SegmentType.FIXED) {
                sb.append(fixedValues.get(i));
            } else {
                sb.append('*');
            }
        }
        currentPattern = sb.toString();
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternClusterState.java
git commit -m "feat: add PatternClusterState POJO for pattern cluster tracking"
```

---

### Task 1.4: PrefixTrie (no-colon key clustering)

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PrefixTrie.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PrefixTrieTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.Map;
import static org.junit.Assert.*;

public class PrefixTrieTest {

    @Test
    public void testInsertAndExtractSinglePath() {
        PrefixTrie trie = new PrefixTrie(2);
        trie.insert("user_profile_1001");
        trie.insert("user_profile_1002");
        trie.insert("user_profile_1003");

        Map<String, Integer> patterns = trie.extractPatterns();
        assertFalse(patterns.isEmpty());
    }

    @Test
    public void testInsertRejectsKeyTooLong() {
        PrefixTrie trie = new PrefixTrie(10);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 201; i++) sb.append('x');
        assertFalse(trie.insert(sb.toString()));
    }

    @Test
    public void testInsertDuplicateKeys() {
        PrefixTrie trie = new PrefixTrie(5);
        assertTrue(trie.insert("key1"));
        assertTrue(trie.insert("key1"));
        assertEquals(1, trie.getUniqueKeyCount());
    }

    @Test
    public void testExtractBranchPattern() {
        PrefixTrie trie = new PrefixTrie(2);
        trie.insert("user_profile_1001");
        trie.insert("user_profile_1002");
        trie.insert("user_profile_2001");

        Map<String, Integer> patterns = trie.extractPatterns();
        assertTrue(patterns.containsKey("user_profile_*"));
    }

    @Test
    public void testExtractWithHighThreshold() {
        PrefixTrie trie = new PrefixTrie(100);
        trie.insert("key_a");
        trie.insert("key_b");
        trie.insert("key_c");

        Map<String, Integer> patterns = trie.extractPatterns();
        assertTrue(patterns.containsKey("*"));
    }

    @Test
    public void testInsertUpToMaxUniqueKeys() {
        PrefixTrie trie = new PrefixTrie(5);
        assertTrue(trie.insert("key1"));
        assertTrue(trie.insert("key2"));
        assertTrue(trie.insert("key3"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PrefixTrieTest`
Expected: FAIL with "cannot find symbol: class PrefixTrie"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.*;

public class PrefixTrie {

    static final int MAX_KEY_LENGTH = 200;
    static final int MAX_UNIQUE_KEYS = 100_000;
    static final int MAX_NODES = 500_000;

    private final TrieNode root;
    private int totalNodes;
    private final Set<String> seenKeys;
    private final int upgradeThreshold;

    public PrefixTrie(int upgradeThreshold) {
        this.root = new TrieNode();
        this.totalNodes = 1;
        this.seenKeys = new HashSet<>();
        this.upgradeThreshold = upgradeThreshold;
    }

    static class TrieNode {
        Map<Character, TrieNode> children = new HashMap<>();
        int count = 0;
    }

    public boolean insert(String key) {
        if (key.length() > MAX_KEY_LENGTH) return false;
        if (seenKeys.size() >= MAX_UNIQUE_KEYS && !seenKeys.contains(key)) return false;
        if (!seenKeys.add(key)) {
            TrieNode node = root;
            for (char c : key.toCharArray()) {
                node = node.children.get(c);
                node.count++;
            }
            return true;
        }
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
        return true;
    }

    public int getUniqueKeyCount() {
        return seenKeys.size();
    }

    public Map<String, Integer> extractPatterns() {
        Map<String, Integer> result = new LinkedHashMap<>();
        int uncoveredCount = 0;
        for (Map.Entry<Character, TrieNode> entry : root.children.entrySet()) {
            uncoveredCount += extractNode(entry.getKey(), entry.getValue(), "", result);
        }
        if (uncoveredCount > 0) {
            result.put("*", uncoveredCount);
        }
        return result;
    }

    private int extractNode(char c, TrieNode node, String prefix, Map<String, Integer> result) {
        String newPrefix = prefix + c;
        if (node.count < upgradeThreshold) {
            return node.count;
        }
        int childrenAboveThreshold = 0;
        for (TrieNode child : node.children.values()) {
            if (child.count >= upgradeThreshold) childrenAboveThreshold++;
        }
        if (childrenAboveThreshold >= 2 || node.children.isEmpty()) {
            result.put(newPrefix + "*", node.count);
            return 0;
        }
        if (childrenAboveThreshold == 0) {
            result.put(newPrefix + "*", node.count);
            return 0;
        }
        int uncovered = 0;
        for (Map.Entry<Character, TrieNode> entry : node.children.entrySet()) {
            uncovered += extractNode(entry.getKey(), entry.getValue(), newPrefix, result);
        }
        return uncovered;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=PrefixTrieTest`
Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PrefixTrie.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/PrefixTrieTest.java
git commit -m "feat: add PrefixTrie for no-colon key clustering with memory guards"
```

---

### Task 1.5: NoTtlKeySample + NoTtlKeyStore

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/NoTtlKeySample.java`
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/NoTtlKeyStore.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/NoTtlKeyStoreTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class NoTtlKeyStoreTest {

    @Test
    public void testStoreAndRetrieve() {
        NoTtlKeyStore store = new NoTtlKeyStore();
        store.offer("key1", "pattern:*", 1024L, "SET key1 val");
        store.offer("key2", "other:*", 2048L, "HSET key2 f v");

        List<NoTtlKeySample> samples = store.getSamples();
        assertEquals(2, samples.size());
        assertEquals("key1", samples.get(0).getKey());
        assertEquals("pattern:*", samples.get(0).getPattern());
        assertEquals(1024L, samples.get(0).getMemoryBytes());
        assertEquals("key2", samples.get(1).getKey());
    }

    @Test
    public void testFifoEvictionAtMaxSize() {
        NoTtlKeyStore store = new NoTtlKeyStore();
        for (int i = 0; i < 7; i++) {
            store.offer("key" + i, "p:*", 100L, "CMD");
        }
        List<NoTtlKeySample> samples = store.getSamples();
        assertEquals(5, samples.size());
        assertEquals("key2", samples.get(0).getKey());
        assertEquals("key6", samples.get(4).getKey());
    }

    @Test
    public void testEmptyStore() {
        NoTtlKeyStore store = new NoTtlKeyStore();
        assertTrue(store.getSamples().isEmpty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=NoTtlKeyStoreTest`
Expected: FAIL with "cannot find symbol: class NoTtlKeyStore"

- [ ] **Step 3: Write NoTtlKeySample POJO**

```java
package com.yj.redis.monitor.analyzer.increment;

public class NoTtlKeySample {

    private final String key;
    private final String pattern;
    private final long memoryBytes;
    private final String command;

    public NoTtlKeySample(String key, String pattern, long memoryBytes, String command) {
        this.key = key;
        this.pattern = pattern;
        this.memoryBytes = memoryBytes;
        this.command = command;
    }

    public String getKey() { return key; }
    public String getPattern() { return pattern; }
    public long getMemoryBytes() { return memoryBytes; }
    public String getCommand() { return command; }
}
```

- [ ] **Step 4: Write NoTtlKeyStore**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class NoTtlKeyStore {

    private static final int MAX_SIZE = 5;
    private final Queue<NoTtlKeySample> samples = new ArrayDeque<>();

    public void offer(String key, String pattern, long memoryBytes, String command) {
        if (samples.size() >= MAX_SIZE) {
            samples.poll();
        }
        samples.offer(new NoTtlKeySample(key, pattern, memoryBytes, command));
    }

    public List<NoTtlKeySample> getSamples() {
        return new ArrayList<>(samples);
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=NoTtlKeyStoreTest`
Expected: all 3 tests PASS

- [ ] **Step 6: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/NoTtlKeySample.java \
        redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/NoTtlKeyStore.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/NoTtlKeyStoreTest.java
git commit -m "feat: add NoTtlKeyStore for tracking no-expiry key examples"
```

---

### Task 1.6: KeyDeserializer

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/KeyDeserializer.java`
- Create: `redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/KeyDeserializerTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class KeyDeserializerTest {

    @Test
    public void testDeserializePlainString() {
        KeyDeserializer deserializer = new KeyDeserializer(false);
        String result = deserializer.deserialize("user:123".getBytes());
        assertEquals("user:123", result);
    }

    @Test
    public void testDeserializeNonUtf8Bytes() {
        KeyDeserializer deserializer = new KeyDeserializer(false);
        byte[] bytes = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};
        String result = deserializer.deserialize(bytes);
        assertEquals("fffefd", result);
    }

    @Test
    public void testDeserializeEmptyBytes() {
        KeyDeserializer deserializer = new KeyDeserializer(false);
        String result = deserializer.deserialize(new byte[0]);
        assertEquals("", result);
    }

    @Test
    public void testJdkFirstThenStringFallback() {
        KeyDeserializer deserializer = new KeyDeserializer(true);
        byte[] plainBytes = "simple:key".getBytes();
        String result = deserializer.deserialize(plainBytes);
        assertNotNull(result);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=KeyDeserializerTest`
Expected: FAIL with "cannot find symbol: class KeyDeserializer"

- [ ] **Step 3: Write minimal implementation**

```java
package com.yj.redis.monitor.analyzer.increment;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;

public class KeyDeserializer {

    private final boolean jdkFirst;

    public KeyDeserializer(boolean jdkFirst) {
        this.jdkFirst = jdkFirst;
    }

    public String deserialize(byte[] keyBytes) {
        if (jdkFirst) {
            try {
                return deserializeJdk(keyBytes);
            } catch (Exception e) {
                return deserializeString(keyBytes);
            }
        } else {
            return deserializeString(keyBytes);
        }
    }

    private String deserializeJdk(byte[] bytes) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        ois.close();
        if (obj instanceof String) {
            return (String) obj;
        }
        throw new IllegalArgumentException("Deserialized object is not String: " + obj.getClass());
    }

    private String deserializeString(byte[] bytes) {
        try {
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return bytesToHex(bytes);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl redis-monitor-analyzer -Dtest=KeyDeserializerTest`
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/KeyDeserializer.java \
        redis-monitor-analyzer/src/test/java/com/yj/redis/monitor/analyzer/increment/KeyDeserializerTest.java
git commit -m "feat: add KeyDeserializer with Jdk→UTF-8→hex fallback chain"
```

---

### Task 1.7: PatternStats POJO

**Files:**
- Create: `redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternStats.java`

No separate test — this is a pure data class, computed fields tested via PatternStatsAggregator in Plan 2.

- [ ] **Step 1: Write the class**

```java
package com.yj.redis.monitor.analyzer.increment;

public class PatternStats {

    private final String pattern;
    private long writeCount;
    private final ReservoirSampler<Long> ttlSamples;
    private final ReservoirSampler<Long> memorySamples;
    private boolean hasTtlFromCommand;

    public PatternStats(String pattern, int ttlSampleCapacity, int memorySampleCapacity) {
        this.pattern = pattern;
        this.writeCount = 0;
        this.ttlSamples = new ReservoirSampler<>(ttlSampleCapacity);
        this.memorySamples = new ReservoirSampler<>(memorySampleCapacity);
        this.hasTtlFromCommand = false;
    }

    public String getPattern() { return pattern; }

    public long getWriteCount() { return writeCount; }

    public void incrementWriteCount() { writeCount++; }

    public ReservoirSampler<Long> getTtlSamples() { return ttlSamples; }

    public ReservoirSampler<Long> getMemorySamples() { return memorySamples; }

    public boolean isHasTtlFromCommand() { return hasTtlFromCommand; }

    public void setHasTtlFromCommand(boolean hasTtlFromCommand) {
        this.hasTtlFromCommand = hasTtlFromCommand;
    }

    public double getWriteRatePerSecond(double durationSec) {
        if (durationSec <= 0) return 0.0;
        return writeCount / durationSec;
    }

    public double getAvgTtlSeconds() {
        return ttlSamples.mean();
    }

    public double getAvgMemoryBytes() {
        return memorySamples.mean();
    }

    public double getIncrementBytes() {
        return writeCount * getAvgMemoryBytes();
    }

    public double getBalancedBytes(double durationSec) {
        if (ttlSamples.isEmpty()) return 0.0;
        return getWriteRatePerSecond(durationSec) * getAvgTtlSeconds() * getAvgMemoryBytes();
    }

    public int getMemorySampleCount() {
        return memorySamples.size();
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add redis-monitor-analyzer/src/main/java/com/yj/redis/monitor/analyzer/increment/PatternStats.java
git commit -m "feat: add PatternStats POJO with computed increment and balanced bytes"
```

---

### Task 1.8: Plan 1 Final Verification

- [ ] **Step 1: Run all Plan 1 tests together**

Run: `mvn test -pl redis-monitor-analyzer -Dtest="ReservoirSamplerTest,SegmentTypeTest,PrefixTrieTest,NoTtlKeyStoreTest,KeyDeserializerTest"`
Expected: all tests PASS

- [ ] **Step 2: Verify compilation of all Plan 1 sources**

Run: `mvn compile -pl redis-monitor-analyzer`
Expected: BUILD SUCCESS

---

**Plan 1 Complete.** Proceed to Plan 2: Core Engine.
