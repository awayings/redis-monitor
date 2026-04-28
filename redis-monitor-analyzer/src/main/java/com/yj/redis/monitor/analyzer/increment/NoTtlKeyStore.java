package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class NoTtlKeyStore {

    private static final int MAX_SIZE = 5;

    private final Queue<NoTtlKeySample> samples = new ArrayDeque<>(MAX_SIZE);

    /**
     * Offers a new sample. If the store is at capacity, the oldest sample is evicted.
     */
    public void offer(String key, String pattern, long memoryBytes, String command) {
        NoTtlKeySample sample = new NoTtlKeySample(key, pattern, memoryBytes, command);
        if (samples.size() >= MAX_SIZE) {
            samples.poll();
        }
        samples.offer(sample);
    }

    /**
     * Returns a copy of the current samples list (oldest first).
     */
    public List<NoTtlKeySample> getSamples() {
        return new ArrayList<>(samples);
    }
}
