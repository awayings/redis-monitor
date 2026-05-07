package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PatternClusterer {

    private final int upgradeThreshold;
    private final int maxCacheSize;
    private final List<PatternClusterState> clusters;
    private final LinkedHashMap<String, String> keyToPattern;
    private final PrefixTrie prefixTrie;

    public PatternClusterer(int upgradeThreshold, int maxCacheSize) {
        this.upgradeThreshold = upgradeThreshold;
        this.maxCacheSize = maxCacheSize;
        this.clusters = new ArrayList<>();
        this.keyToPattern = new LinkedHashMap<String, String>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > maxCacheSize;
            }
        };
        this.prefixTrie = new PrefixTrie(upgradeThreshold);
    }

    /**
     * Clusters the given raw key into a pattern.
     *
     * @param rawKey the raw Redis key
     * @return the pattern for this key
     */
    public String cluster(String rawKey) {
        // 1. Detect delimiter (needed for cache path branching)
        String delimiter = detectDelimiter(rawKey);

        // 2. Check keyToPattern cache
        String cachedPattern = keyToPattern.get(rawKey);
        if (cachedPattern != null) {
            if (delimiter != null) {
                for (PatternClusterState state : clusters) {
                    if (state.getCurrentPattern().equals(cachedPattern)) {
                        state.incrementCount();
                        break;
                    }
                }
            }
            return cachedPattern;
        }

        // 3. No delimiter -> prefix trie
        if (delimiter == null) {
            return clusterNoColon(rawKey);
        }

        // 4. Split by delimiter into segments
        String[] segments = rawKey.split(delimiter);

        // 5. Try exact match
        PatternClusterState exact = findExactMatch(segments, delimiter);
        if (exact != null) {
            exact.incrementCount();
            recordDistinctValues(exact, segments);
            String pattern = exact.getCurrentPattern();
            keyToPattern.put(rawKey, pattern);
            return pattern;
        }

        // 6. Try near match
        PatternClusterState near = findNearMatch(segments, delimiter);
        if (near != null) {
            String pattern = near.getCurrentPattern();
            keyToPattern.put(rawKey, pattern);
            return pattern;
        }

        // 7. Create new cluster with ALL segments as FIXED
        List<SegmentType> segmentTypes = new ArrayList<>(segments.length);
        List<String> fixedValues = new ArrayList<>(segments.length);
        for (String seg : segments) {
            segmentTypes.add(SegmentType.FIXED);
            fixedValues.add(seg);
        }

        String pattern = String.join(delimiter, segments);
        PatternClusterState newState = new PatternClusterState(pattern, segmentTypes, fixedValues, delimiter);
        newState.incrementCount();
        clusters.add(newState);
        keyToPattern.put(rawKey, pattern);
        return pattern;
    }

    private static String detectDelimiter(String rawKey) {
        if (rawKey.contains(":")) return ":";
        if (rawKey.contains("_")) return "_";
        return null;
    }

    /**
     * Handles keys without colons by inserting into the prefix trie.
     */
    private String clusterNoColon(String rawKey) {
        if (prefixTrie.insert(rawKey)) {
            keyToPattern.put(rawKey, rawKey);
            return rawKey;
        }
        keyToPattern.put(rawKey, "*");
        return "*";
    }

    /**
     * Finds a cluster where all positions match exactly (FIXED positions match
     * their fixed value, variable positions match their type regex).
     */
    private PatternClusterState findExactMatch(String[] segments, String delimiter) {
        for (PatternClusterState state : clusters) {
            if (!state.getDelimiter().equals(delimiter)) {
                continue;
            }
            if (state.getSegmentCount() != segments.length) {
                continue;
            }

            boolean match = true;
            for (int i = 0; i < segments.length; i++) {
                if (state.getSegmentType(i) == SegmentType.FIXED) {
                    if (!state.getFixedValue(i).equals(segments[i])) {
                        match = false;
                        break;
                    }
                } else {
                    if (!state.getSegmentType(i).matches(segments[i])) {
                        match = false;
                        break;
                    }
                }
            }

            if (match) {
                return state;
            }
        }
        return null;
    }

    /**
     * Finds a cluster where all variable positions match their type regex and
     * exactly one FIXED position differs. Records the distinct value at the
     * mismatched position and triggers upgrade + dedup if threshold is reached.
     */
    private PatternClusterState findNearMatch(String[] segments, String delimiter) {
        for (PatternClusterState state : clusters) {
            if (!state.getDelimiter().equals(delimiter)) {
                continue;
            }
            if (state.getSegmentCount() != segments.length) {
                continue;
            }

            int mismatchCount = 0;
            int mismatchIndex = -1;
            boolean allVarMatch = true;

            for (int i = 0; i < segments.length; i++) {
                if (state.getSegmentType(i) == SegmentType.FIXED) {
                    if (!state.getFixedValue(i).equals(segments[i])) {
                        mismatchCount++;
                        mismatchIndex = i;
                    }
                } else {
                    if (!state.getSegmentType(i).matches(segments[i])) {
                        allVarMatch = false;
                        break;
                    }
                }
            }

            if (mismatchCount == 1 && allVarMatch) {
                String segmentValue = segments[mismatchIndex];
                boolean upgraded = state.recordDistinctValue(mismatchIndex, segmentValue, upgradeThreshold);
                if (upgraded) {
                    mergeDuplicatePatterns();
                }
                state.incrementCount();
                return state;
            }
        }
        return null;
    }

    /**
     * For each FIXED position where the segment differs from the fixed value,
     * records the segment as a new distinct value. If any position triggers an
     * upgrade, merges duplicate patterns.
     */
    private void recordDistinctValues(PatternClusterState state, String[] segments) {
        boolean anyUpgraded = false;
        for (int i = 0; i < segments.length; i++) {
            if (state.getSegmentType(i) == SegmentType.FIXED
                    && !state.getFixedValue(i).equals(segments[i])) {
                if (state.recordDistinctValue(i, segments[i], upgradeThreshold)) {
                    anyUpgraded = true;
                }
            }
        }
        if (anyUpgraded) {
            mergeDuplicatePatterns();
        }
    }

    /**
     * Deduplicates clusters by pattern name, merging key counts of clusters
     * that share the same pattern after an upgrade.
     */
    private void mergeDuplicatePatterns() {
        LinkedHashMap<String, PatternClusterState> deduped = new LinkedHashMap<>();
        for (PatternClusterState state : clusters) {
            String pattern = state.getCurrentPattern();
            PatternClusterState existing = deduped.get(pattern);
            if (existing != null) {
                existing.addKeyCount(state.getKeyCount());
            } else {
                deduped.put(pattern, state);
            }
        }
        clusters.clear();
        clusters.addAll(deduped.values());
    }

    /**
     * Returns the current cluster sizes: patterns and their key counts from
     * both colon-based clusters and the prefix trie (for no-colon keys).
     */
    public Map<String, Integer> getClusterSizes() {
        Map<String, Integer> sizes = new LinkedHashMap<>();
        for (PatternClusterState state : clusters) {
            sizes.put(state.getCurrentPattern(), state.getKeyCount());
        }
        for (Map.Entry<String, Integer> entry : prefixTrie.extractPatterns().entrySet()) {
            sizes.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
        return sizes;
    }
}
