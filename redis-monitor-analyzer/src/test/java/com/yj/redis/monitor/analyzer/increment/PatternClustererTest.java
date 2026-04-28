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
    public void testNoColonKeyGoesToPrefixTrie() {
        PatternClusterer clusterer = new PatternClusterer(3, 100);
        String p1 = clusterer.cluster("user_profile_1001");
        String p2 = clusterer.cluster("user_profile_1002");
        String p3 = clusterer.cluster("other_key");
        assertNotNull(p1);
        assertNotNull(p2);
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
