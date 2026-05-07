package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import java.util.Map;
import static org.junit.Assert.*;

public class PatternClustererTest {

    @Test
    public void testFirstKeyCreatesFixedPattern() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String pattern = clusterer.cluster("user:123:profile");
        assertEquals("user:123:profile", pattern);
        Map<String, Integer> sizes = clusterer.getClusterSizes();
        assertEquals(1, sizes.get("user:123:profile").intValue());
    }

    @Test
    public void testSecondMatchingKeyReturnsSamePattern() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String p1 = clusterer.cluster("user:1001:profile");
        String p2 = clusterer.cluster("user:1002:profile");
        assertEquals(p1, p2);
    }

    @Test
    public void testDifferentPrefixesCreateDifferentPatterns() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String p1 = clusterer.cluster("user:123:data");
        String p2 = clusterer.cluster("order:456:info");
        assertNotEquals(p1, p2);
    }

    @Test
    public void testUpgradeThresholdTriggersWildcard() {
        PatternClusterer clusterer = new PatternClusterer(3, 100);
        clusterer.cluster("log:001");
        clusterer.cluster("log:002");
        clusterer.cluster("log:003");
        clusterer.cluster("log:004");
        Map<String, Integer> sizes = clusterer.getClusterSizes();
        boolean hasWildcard = sizes.keySet().stream().anyMatch(p -> p.contains("*"));
        assertTrue("Expected at least one pattern to contain '*'", hasWildcard);
    }

    @Test
    public void testUnderscoreDelimitedKeysUseSegmentClustering() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String p1 = clusterer.cluster("user_profile_1001");
        String p2 = clusterer.cluster("user_profile_1002");
        String p3 = clusterer.cluster("other_key");
        // user_profile_1001 and user_profile_1002 differ at only one segment -> near match -> same cluster
        assertEquals(p1, p2);
        // other_key has different segment count -> separate cluster
        assertNotEquals(p1, p3);
    }

    @Test
    public void testUnderscoreTwoMismatchCreatesSeparateClusters() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String p1 = clusterer.cluster("SYNC_CUSTOM_TOP_GOODS_16802");
        String p2 = clusterer.cluster("SYNC_CUSTOM_LOW_GOODS_16771");
        // TOP!=LOW and 16802!=16771 -> mismatchCount=2 -> separate clusters
        assertNotEquals(p1, p2);
        // Both should NOT be merged into a single SYNC_CUSTOM_* pattern
        Map<String, Integer> sizes = clusterer.getClusterSizes();
        assertFalse("Should not merge TOP and LOW into a single pattern",
                sizes.containsKey("SYNC_CUSTOM_*"));
    }

    @Test
    public void testUnderscoreUpgradeTriggersWildcard() {
        PatternClusterer clusterer = new PatternClusterer(3, 100);
        clusterer.cluster("SYNC_CUSTOM_TOP_GOODS_16802");
        clusterer.cluster("SYNC_CUSTOM_TOP_GOODS_16803");
        clusterer.cluster("SYNC_CUSTOM_TOP_GOODS_16804");
        Map<String, Integer> sizes = clusterer.getClusterSizes();
        boolean hasTopGoods = sizes.keySet().stream()
                .anyMatch(p -> p.equals("SYNC_CUSTOM_TOP_GOODS_*"));
        assertTrue("Expected SYNC_CUSTOM_TOP_GOODS_* after upgrade threshold", hasTopGoods);
    }

    @Test
    public void testTrulyNoDelimiterGoesToPrefixTrie() {
        PatternClusterer clusterer = new PatternClusterer(3, 100);
        String p1 = clusterer.cluster("abcdef");
        String p2 = clusterer.cluster("abcdef");
        String p3 = clusterer.cluster("xyz");
        // Same key returns same pattern from cache
        assertEquals(p1, p2);
        // Keys without : or _ still use prefix trie (not segment clustering)
        assertNotNull(p1);
        assertNotNull(p3);
    }

    @Test
    public void testCacheReturnsSamePatternForSameKey() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String p1 = clusterer.cluster("cache:test:key");
        String p2 = clusterer.cluster("cache:test:key");
        assertEquals(p1, p2);
    }

    @Test
    public void testDifferentSegmentCountDifferentPattern() {
        PatternClusterer clusterer = new PatternClusterer(10, 100);
        String p1 = clusterer.cluster("a:b:c");
        String p2 = clusterer.cluster("a:b:c:d");
        assertNotEquals(p1, p2);
    }

    @Test
    public void testExtractPatternsAtReportTime() {
        PatternClusterer clusterer = new PatternClusterer(3, 100);
        clusterer.cluster("test:001");
        clusterer.cluster("test:002");
        clusterer.cluster("test:003");
        clusterer.cluster("test:004");
        assertFalse(clusterer.getClusterSizes().isEmpty());
    }
}
