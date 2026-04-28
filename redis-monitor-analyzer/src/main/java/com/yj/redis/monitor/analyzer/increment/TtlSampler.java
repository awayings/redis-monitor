package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class TtlSampler extends Thread {

    private final String host;
    private final int port;
    private final PatternStatsAggregator aggregator;
    private final int maxTtlSamples;
    private final DelayQueue<DelayedTtlTask> delayQueue;
    private volatile boolean running;

    public TtlSampler(String host, int port, PatternStatsAggregator aggregator, int maxTtlSamples) {
        super("TtlSampler");
        this.host = host;
        this.port = port;
        this.aggregator = aggregator;
        this.maxTtlSamples = maxTtlSamples;
        this.delayQueue = new DelayQueue<>();
        this.running = true;
        setDaemon(true);
    }

    /**
     * Schedules a delayed TTL query for the given key.
     */
    public void scheduleDelayedTtl(String key, String pattern) {
        delayQueue.offer(new DelayedTtlTask(key, pattern, 1000));
    }

    @Override
    public void run() {
        RedisConnectionFactory factory = new RedisConnectionFactory(host, port, 2000, 5000, null);
        try (Jedis jedis = factory.createConnection()) {
            while (running) {
                try {
                    DelayedTtlTask task = delayQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        processTtlTask(jedis, task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Exception e) {
            // Connection error during setup — thread exits silently
        }
    }

    private void processTtlTask(Jedis jedis, DelayedTtlTask task) {
        PatternStats stats = aggregator.getStats(task.getPattern());
        if (stats == null) {
            return;
        }

        // Skip if reservoir is full
        if (stats.getTtlSamples().size() >= maxTtlSamples) {
            return;
        }

        // Skip if TTL was already obtained from command
        if (stats.isHasTtlFromCommand()) {
            return;
        }

        try {
            long ttl = jedis.ttl(task.getKey());
            // Redis TTL: -1 = persistent (no TTL), -2 = key doesn't exist
            if (ttl >= 0) {
                aggregator.addTtlSample(task.getPattern(), ttl * 1000);
            }
        } catch (Exception e) {
            // Skip on error
        }
    }

    public void shutdown() {
        running = false;
        interrupt();
    }

    /**
     * A delayed task that becomes available after a set delay.
     */
    private static class DelayedTtlTask implements Delayed {

        private final String key;
        private final String pattern;
        private final long expiryTime;

        DelayedTtlTask(String key, String pattern, long delayMillis) {
            this.key = key;
            this.pattern = pattern;
            this.expiryTime = System.currentTimeMillis() + delayMillis;
        }

        String getKey() {
            return key;
        }

        String getPattern() {
            return pattern;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long remaining = expiryTime - System.currentTimeMillis();
            return unit.convert(remaining, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            long diff = this.getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
            if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            }
            return 0;
        }
    }
}
