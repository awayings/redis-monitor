package com.yj.redis.monitor.analyzer.increment;

public interface MonitorLineHandler {

    /**
     * Called when a new MONITOR output line is received.
     */
    void onLine(String line);

    /**
     * Called when an error occurs during MONITOR streaming.
     */
    void onError(Exception e);
}
