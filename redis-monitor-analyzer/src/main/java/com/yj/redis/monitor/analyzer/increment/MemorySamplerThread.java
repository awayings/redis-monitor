package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.BlockingQueue;

public class MemorySamplerThread extends Thread {

    private final String host;
    private final int port;
    private final String password;
    private final BlockingQueue<SampleTask> queue;
    private volatile boolean running;

    public MemorySamplerThread(String host, int port, BlockingQueue<SampleTask> queue, String password) {
        super("MemorySampler");
        this.host = host;
        this.port = port;
        this.password = password;
        this.queue = queue;
        this.running = true;
        setDaemon(true);
    }

    @Override
    public void run() {
        RedisConnectionFactory factory = new RedisConnectionFactory(host, port, 2000, 5000, password);
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
