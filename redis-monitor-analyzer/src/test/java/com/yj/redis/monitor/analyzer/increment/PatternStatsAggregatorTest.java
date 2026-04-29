package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class PatternStatsAggregatorTest {

    @Test
    public void testRecordWriteCreatesNewPatternStats() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("user:*");
        PatternStats stats = agg.getStats("user:*");
        assertNotNull(stats);
        assertEquals(1, stats.getWriteCount());
    }

    @Test
    public void testMultipleWritesIncrementCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("user:*");
        agg.recordWrite("user:*");
        agg.recordWrite("user:*");
        PatternStats stats = agg.getStats("user:*");
        assertEquals(3, stats.getWriteCount());
    }

    @Test
    public void testDistinctPatterns() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("user:*");
        agg.recordWrite("order:*");
        assertNotNull(agg.getStats("user:*"));
        assertNotNull(agg.getStats("order:*"));
    }

    @Test
    public void testAddTtlSample() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("key:*");
        agg.addTtlSample("key:*", 3600000L);
        PatternStats stats = agg.getStats("key:*");
        assertFalse(stats.getTtlSamples().isEmpty());
    }

    @Test
    public void testAddMemorySample() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 5);
        agg.recordWrite("key:*");
        agg.addMemorySample("key:*", 200L);
        PatternStats stats = agg.getStats("key:*");
        assertEquals(1, stats.getMemorySampleCount());
    }

    @Test
    public void testMarkHasTtlFromCommand() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("key:*");
        agg.markTtlFromCommand("key:*");
        assertTrue(agg.getStats("key:*").isHasTtlFromCommand());
    }

    @Test
    public void testGetTopPatterns() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("pattern:a");
        agg.recordWrite("pattern:a");
        agg.recordWrite("pattern:a");
        agg.addMemorySample("pattern:a", 200L);

        agg.recordWrite("pattern:b");
        agg.recordWrite("pattern:b");
        agg.addMemorySample("pattern:b", 100L);

        agg.recordWrite("pattern:c");
        agg.addMemorySample("pattern:c", 50L);

        List<PatternStats> top = agg.getTopPatterns(2, 60);
        assertEquals(2, top.size());
        // pattern:a has writeCount=3 * avgMemoryBytes=200 = 600 incrementBytes
        // pattern:b has writeCount=2 * avgMemoryBytes=100 = 200 incrementBytes
        // So top 2 should be pattern:a then pattern:b
        assertEquals("pattern:a", top.get(0).getPattern());
        assertEquals("pattern:b", top.get(1).getPattern());
    }

    @Test
    public void testGetTotalWriteCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("a:*");
        agg.recordWrite("b:*");
        agg.recordWrite("a:*");
        assertEquals(3, agg.getTotalWriteCount());
    }

    @Test
    public void testGetPatternCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        assertEquals(0, agg.getPatternCount());
        agg.recordWrite("x:*");
        agg.recordWrite("y:*");
        assertEquals(2, agg.getPatternCount());
    }

    @Test
    public void testGetAllStats() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("a:*");
        agg.recordWrite("b:*");
        assertEquals(2, agg.getAllStats().size());
    }

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

        assertEquals(1L, snap1.get("a:*").longValue());
        assertEquals(2L, snap2.get("a:*").longValue());
    }
}
