package com.yj.redis.monitor.analyzer.increment;

import java.util.function.Consumer;

public class SampleTask {

    private final String key;
    private final Consumer<Long> callback;

    public SampleTask(String key, Consumer<Long> callback) {
        this.key = key;
        this.callback = callback;
    }

    public String getKey() {
        return key;
    }

    public Consumer<Long> getCallback() {
        return callback;
    }
}
