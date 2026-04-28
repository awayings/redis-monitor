package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ReservoirSampler<T extends Number> {

    private final int capacity;
    private final List<T> samples;
    private int totalSeen;

    /**
     * @param capacity maximum number of samples to retain, must be positive
     * @throws IllegalArgumentException if capacity is not positive
     */
    public ReservoirSampler(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive, got: " + capacity);
        }
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

    /**
     * @return arithmetic mean of stored samples, or 0.0 if the sampler is empty
     */
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
