package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.List;

public class PatternStats {

    private static final int MAX_SAMPLE_KEYS = 10;

    private final String pattern;
    private long writeCount;
    private final ReservoirSampler<Long> ttlSamples;
    private final ReservoirSampler<Long> memorySamples;
    private boolean hasTtlFromCommand;
    private String representativeKey;
    private final List<String> sampleKeys = new ArrayList<>();

    public PatternStats(String pattern, int ttlSampleCapacity, int memorySampleCapacity) {
        this.pattern = pattern;
        this.writeCount = 0;
        this.ttlSamples = new ReservoirSampler<>(ttlSampleCapacity);
        this.memorySamples = new ReservoirSampler<>(memorySampleCapacity);
        this.hasTtlFromCommand = false;
    }

    public String getPattern() {
        return pattern;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public ReservoirSampler<Long> getTtlSamples() {
        return ttlSamples;
    }

    public ReservoirSampler<Long> getMemorySamples() {
        return memorySamples;
    }

    public boolean isHasTtlFromCommand() {
        return hasTtlFromCommand;
    }

    public void setHasTtlFromCommand(boolean hasTtlFromCommand) {
        this.hasTtlFromCommand = hasTtlFromCommand;
    }

    public void incrementWriteCount() {
        writeCount++;
    }

    /**
     * Computes the write rate per second.
     *
     * @param durationSec the duration in seconds over which writes were observed
     * @return write rate (writes per second), or 0 if durationSec is not positive
     */
    public double getWriteRatePerSecond(double durationSec) {
        if (durationSec <= 0) {
            return 0.0;
        }
        return writeCount / durationSec;
    }

    /**
     * @return average TTL in seconds (TTL samples are stored in milliseconds)
     */
    public double getAvgTtlSeconds() {
        return ttlSamples.mean() / 1000.0;
    }

    /**
     * @return average memory bytes per sample
     */
    public double getAvgMemoryBytes() {
        return memorySamples.mean();
    }

    /**
     * @return total estimated memory increment (writeCount * avgMemoryBytes)
     */
    public double getIncrementBytes() {
        return writeCount * getAvgMemoryBytes();
    }

    /**
     * Returns a time-balanced estimate of memory increment per second.
     * Formula: writeRate * avgTtlSeconds * avgMemoryBytes.
     * Returns 0 if no TTL samples have been collected.
     *
     * @param durationSec the duration in seconds over which writes were observed
     * @return balanced bytes estimate
     */
    public double getBalancedBytes(double durationSec) {
        if (ttlSamples.isEmpty()) {
            return 0.0;
        }
        return getWriteRatePerSecond(durationSec) * getAvgTtlSeconds() * getAvgMemoryBytes();
    }

    /**
     * @return number of memory samples collected
     */
    public int getMemorySampleCount() {
        return memorySamples.size();
    }

    public String getRepresentativeKey() {
        return representativeKey;
    }

    public void setRepresentativeKey(String representativeKey) {
        this.representativeKey = representativeKey;
    }

    public void addSampleKey(String key) {
        if (sampleKeys.size() < MAX_SAMPLE_KEYS) {
            sampleKeys.add(key);
        }
    }

    public List<String> getSampleKeys() {
        return sampleKeys;
    }
}
