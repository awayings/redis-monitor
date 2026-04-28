package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class PatternStatsAggregatorTest {

    @Test
    public void testRecordWriteCreatesNewPatternStats() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("user:*", 100L);
        PatternStats stats = agg.getStats("user:*");
        assertNotNull(stats);
        assertEquals(1, stats.getWriteCount());
    }

    @Test
    public void testMultipleWritesIncrementCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("user:*", 100L);
        agg.recordWrite("user:*", 200L);
        agg.recordWrite("user:*", 300L);
        PatternStats stats = agg.getStats("user:*");
        assertEquals(3, stats.getWriteCount());
    }

    @Test
    public void testDistinctPatterns() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("user:*", 100L);
        agg.recordWrite("order:*", 200L);
        assertNotNull(agg.getStats("user:*"));
        assertNotNull(agg.getStats("order:*"));
    }

    @Test
    public void testAddTtlSample() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("key:*", 50L);
        agg.addTtlSample("key:*", 3600000L);
        PatternStats stats = agg.getStats("key:*");
        assertFalse(stats.getTtlSamples().isEmpty());
    }

    @Test
    public void testAddMemorySample() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 5);
        agg.recordWrite("key:*", 200L);
        agg.addMemorySample("key:*", 200L);
        PatternStats stats = agg.getStats("key:*");
        assertEquals(1, stats.getMemorySampleCount());
    }

    @Test
    public void testMarkHasTtlFromCommand() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("key:*", 50L);
        agg.markTtlFromCommand("key:*");
        assertTrue(agg.getStats("key:*").isHasTtlFromCommand());
    }

    @Test
    public void testGetTopPatterns() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("pattern:a", 200L);
        agg.recordWrite("pattern:a", 200L);
        agg.recordWrite("pattern:a", 200L);
        agg.addMemorySample("pattern:a", 200L);

        agg.recordWrite("pattern:b", 100L);
        agg.recordWrite("pattern:b", 100L);
        agg.addMemorySample("pattern:b", 100L);

        agg.recordWrite("pattern:c", 50L);
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
        agg.recordWrite("a:*", 100L);
        agg.recordWrite("b:*", 200L);
        agg.recordWrite("a:*", 300L);
        assertEquals(3, agg.getTotalWriteCount());
    }

    @Test
    public void testGetPatternCount() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        assertEquals(0, agg.getPatternCount());
        agg.recordWrite("x:*", 50L);
        agg.recordWrite("y:*", 50L);
        assertEquals(2, agg.getPatternCount());
    }

    @Test
    public void testGetAllStats() {
        PatternStatsAggregator agg = new PatternStatsAggregator(10, 10);
        agg.recordWrite("a:*", 50L);
        agg.recordWrite("b:*", 50L);
        assertEquals(2, agg.getAllStats().size());
    }
}
