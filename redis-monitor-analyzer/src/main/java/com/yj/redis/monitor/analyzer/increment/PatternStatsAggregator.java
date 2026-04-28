package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PatternStatsAggregator {

    private final ConcurrentHashMap<String, PatternStats> statsMap;
    private final int ttlSampleCapacity;
    private final int memorySampleCapacity;

    public PatternStatsAggregator(int ttlSampleCapacity, int memorySampleCapacity) {
        this.statsMap = new ConcurrentHashMap<>();
        this.ttlSampleCapacity = ttlSampleCapacity;
        this.memorySampleCapacity = memorySampleCapacity;
    }

    /**
     * Records a write operation for the given pattern with memory usage in bytes.
     * Creates a new PatternStats entry if one does not exist.
     */
    public void recordWrite(String pattern, long memoryBytes) {
        PatternStats stats = statsMap.computeIfAbsent(pattern,
                p -> new PatternStats(p, ttlSampleCapacity, memorySampleCapacity));
        stats.incrementWriteCount();
    }

    /**
     * Adds a TTL sample (in milliseconds) for the given pattern.
     */
    public void addTtlSample(String pattern, long ttlMillis) {
        PatternStats stats = statsMap.get(pattern);
        if (stats != null) {
            stats.getTtlSamples().add(ttlMillis);
        }
    }

    /**
     * Adds a memory sample (in bytes) for the given pattern.
     */
    public void addMemorySample(String pattern, long memoryBytes) {
        PatternStats stats = statsMap.computeIfAbsent(pattern,
                p -> new PatternStats(p, ttlSampleCapacity, memorySampleCapacity));
        stats.getMemorySamples().add(memoryBytes);
    }

    /**
     * Marks that the given pattern has TTL information from an explicit command.
     */
    public void markTtlFromCommand(String pattern) {
        PatternStats stats = statsMap.get(pattern);
        if (stats != null) {
            stats.setHasTtlFromCommand(true);
        }
    }

    /**
     * Returns the stats for a given pattern, or null if not found.
     */
    public PatternStats getStats(String pattern) {
        return statsMap.get(pattern);
    }

    /**
     * Returns a snapshot list of all stats.
     */
    public List<PatternStats> getAllStats() {
        return new ArrayList<>(statsMap.values());
    }

    /**
     * Returns the top N patterns sorted by estimated increment bytes descending.
     *
     * @param topN       the maximum number of patterns to return
     * @param durationSec the duration in seconds over which writes were observed
     * @return the top N patterns
     */
    public List<PatternStats> getTopPatterns(int topN, long durationSec) {
        return statsMap.values().stream()
                .sorted(Comparator.comparingDouble(PatternStats::getIncrementBytes).reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    /**
     * Returns the total write count across all patterns.
     */
    public long getTotalWriteCount() {
        return statsMap.values().stream()
                .mapToLong(PatternStats::getWriteCount)
                .sum();
    }

    /**
     * Returns the number of distinct patterns tracked.
     */
    public int getPatternCount() {
        return statsMap.size();
    }
}
