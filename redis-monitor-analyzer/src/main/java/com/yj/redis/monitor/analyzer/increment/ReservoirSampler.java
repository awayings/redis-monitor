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
