package com.yj.redis.monitor.analyzer.increment;

public class ParsedCommand {

    private final String commandName;
    private final String key;
    private final Long ttlMillis;
    private final String rawLine;
    private final boolean isWrite;
    private final long valueSize;

    public ParsedCommand(String commandName, String key, Long ttlMillis, String rawLine, boolean isWrite, long valueSize) {
        this.commandName = commandName;
        this.key = key;
        this.ttlMillis = ttlMillis;
        this.rawLine = rawLine;
        this.isWrite = isWrite;
        this.valueSize = valueSize;
    }

    public String getCommandName() {
        return commandName;
    }

    public String getKey() {
        return key;
    }

    public Long getTtlMillis() {
        return ttlMillis;
    }

    public String getRawLine() {
        return rawLine;
    }

    public boolean isWriteCommand() {
        return isWrite;
    }

    public long getValueSize() {
        return valueSize;
    }
}
