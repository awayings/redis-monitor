package com.yj.redis.monitor.analyzer.increment;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;

public class MonitorStream implements AutoCloseable {

    private final Jedis jedis;
    private final int durationSec;
    private volatile boolean running;
    private volatile boolean started;

    public MonitorStream(Jedis jedis, int durationSec) {
        this.jedis = jedis;
        this.durationSec = durationSec;
        this.running = true;
        this.started = false;
    }

    /**
     * Starts MONITOR streaming. Runs in a daemon thread and returns immediately.
     * Lines are delivered to the provided handler callback.
     *
     * @param handler callback for received lines and errors
     */
    public void start(MonitorLineHandler handler) {
        if (started) {
            return;
        }
        started = true;

        long deadline = System.currentTimeMillis() + durationSec * 1000L;

        Thread monitorThread = new Thread(() -> {
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
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    /**
     * Signals the MONITOR loop to stop.
     */
    public void stop() {
        running = false;
    }

    /**
     * Returns whether the stream is currently running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Closes the underlying Jedis connection and releases resources.
     */
    @Override
    public void close() {
        running = false;
        disconnect();
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                // suppress
            }
        }
    }

    /**
     * Disconnects the underlying connection to break the blocking MONITOR loop.
     */
    private void disconnect() {
        try {
            Connection conn = jedis.getConnection();
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            // suppress
        }
    }
}
