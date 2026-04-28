package com.yj.redis.monitor.analyzer.increment;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;

public class MonitorStream implements AutoCloseable {

    private final Jedis jedis;
    private final int durationSec;
    private volatile boolean running;

    public MonitorStream(Jedis jedis, int durationSec) {
        this.jedis = jedis;
        this.durationSec = durationSec;
        this.running = true;
    }

    /**
     * Starts MONITOR streaming, blocking the calling thread until the duration
     * expires or stop() is called. Lines are delivered to the handler callback.
     */
    public void start(MonitorLineHandler handler) {
        long deadline = System.currentTimeMillis() + durationSec * 1000L;

        try {
            jedis.monitor(new JedisMonitor() {
                @Override
                public void onCommand(String command) {
                    if (!running || System.currentTimeMillis() > deadline) {
                        disconnect();
                        return;
                    }
                    handler.onLine(command);
                }
            });
        } catch (Exception e) {
            if (running) {
                handler.onError(e);
            }
        } finally {
            running = false;
        }
    }

    public void stop() {
        running = false;
        disconnect();
    }

    public boolean isRunning() {
        return running;
    }

    private void disconnect() {
        try {
            Connection conn = jedis.getConnection();
            if (conn != null) {
                conn.close();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void close() {
        running = false;
        disconnect();
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception ignored) {
        }
    }
}
