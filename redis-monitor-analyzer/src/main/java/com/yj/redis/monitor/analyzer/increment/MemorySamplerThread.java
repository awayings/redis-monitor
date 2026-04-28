package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.BlockingQueue;

public class MemorySamplerThread extends Thread {

    private final RedisConnectionFactory factory;
    private final BlockingQueue<SampleTask> queue;
    private volatile boolean running;

    public MemorySamplerThread(RedisConnectionFactory factory, BlockingQueue<SampleTask> queue) {
        super("MemorySampler");
        this.factory = factory;
        this.queue = queue;
        this.running = true;
        setDaemon(true);
    }

    @Override
    public void run() {
        try (Jedis jedis = factory.createConnection()) {
            while (running) {
                try {
                    SampleTask task = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (task != null) {
                        try {
                            Long memory = jedis.memoryUsage(task.getKey());
                            if (memory != null) {
                                task.getCallback().accept(memory);
                            }
                        } catch (Exception e) {
                            // Skip on timeout/error
                        }
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

    public void shutdown() {
        running = false;
        interrupt();
    }
}
