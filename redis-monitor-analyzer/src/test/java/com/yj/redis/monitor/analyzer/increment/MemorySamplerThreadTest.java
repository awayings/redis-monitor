package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class MemorySamplerThreadTest {

    @Test
    public void testThreadProcessesTask() throws Exception {
        BlockingQueue<SampleTask> queue = new LinkedBlockingQueue<>();
        MemorySamplerThread thread = new MemorySamplerThread("localhost", 6379, queue);
        thread.start();
        assertTrue(thread.isAlive());
        thread.shutdown();
        thread.join(5000);
        assertFalse(thread.isAlive());
    }

    @Test
    public void testShutdownStopsThread() throws Exception {
        BlockingQueue<SampleTask> queue = new LinkedBlockingQueue<>();
        MemorySamplerThread thread = new MemorySamplerThread("localhost", 6379, queue);
        thread.start();
        assertTrue(thread.isAlive());
        thread.shutdown();
        thread.join(5000);
        assertFalse(thread.isAlive());
    }
}
