package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class MemorySamplerThreadTest {

    @Test
    public void testThreadProcessesTask() throws Exception {
        BlockingQueue<SampleTask> queue = new LinkedBlockingQueue<>();
        MemorySamplerThread thread = new MemorySamplerThread(
                new RedisConnectionFactory("localhost", 6379, null), queue);
        thread.start();
        assertTrue(thread.isAlive());
        thread.shutdown();
        thread.join(5000);
        assertFalse(thread.isAlive());
    }

    @Test
    public void testShutdownStopsThread() throws Exception {
        BlockingQueue<SampleTask> queue = new LinkedBlockingQueue<>();
        MemorySamplerThread thread = new MemorySamplerThread(
                new RedisConnectionFactory("localhost", 6379, null), queue);
        thread.start();
        assertTrue(thread.isAlive());
        thread.shutdown();
        thread.join(5000);
        assertFalse(thread.isAlive());
    }
}
