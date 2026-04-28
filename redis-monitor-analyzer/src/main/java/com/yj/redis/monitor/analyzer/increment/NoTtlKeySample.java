package com.yj.redis.monitor.analyzer.increment;

public class NoTtlKeySample {

    private final String key;
    private final String pattern;
    private final long memoryBytes;
    private final String command;

    public NoTtlKeySample(String key, String pattern, long memoryBytes, String command) {
        this.key = key;
        this.pattern = pattern;
        this.memoryBytes = memoryBytes;
        this.command = command;
    }

    public String getKey() {
        return key;
    }

    public String getPattern() {
        return pattern;
    }

    public long getMemoryBytes() {
        return memoryBytes;
    }

    public String getCommand() {
        return command;
    }
}
