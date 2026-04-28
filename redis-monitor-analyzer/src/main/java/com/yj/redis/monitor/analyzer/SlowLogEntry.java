package com.yj.redis.monitor.analyzer;

public class SlowLogEntry {

    private long id;
    private long timestamp;
    private long durationMicros;
    private String command;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getDurationMicros() {
        return durationMicros;
    }

    public void setDurationMicros(long durationMicros) {
        this.durationMicros = durationMicros;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }
}
