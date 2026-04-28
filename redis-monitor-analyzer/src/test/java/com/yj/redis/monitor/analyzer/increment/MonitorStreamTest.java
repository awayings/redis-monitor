package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MonitorStreamTest {

    @Test
    public void testMonitorStreamCreated() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        assertNotNull(stream);
    }

    @Test
    public void testIsRunningInitiallyTrue() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        assertTrue(stream.isRunning());
    }

    @Test
    public void testStopChangesRunningState() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        stream.stop();
        assertFalse(stream.isRunning());
    }

    @Test
    public void testCloseSetsRunningFalse() {
        Jedis jedis = mock(Jedis.class);
        MonitorStream stream = new MonitorStream(jedis, 300);
        assertTrue(stream.isRunning());
        stream.close();
        assertFalse(stream.isRunning());
        verify(jedis).close();
    }
}
