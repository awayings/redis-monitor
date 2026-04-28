package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class NoTtlKeyStoreTest {

    @Test
    public void testStoreAndRetrieve() {
        NoTtlKeyStore store = new NoTtlKeyStore();
        store.offer("key1", "pattern1", 1024L, "SET");
        store.offer("key2", "pattern2", 2048L, "GET");

        assertEquals(2, store.getSamples().size());
    }

    @Test
    public void testFifoEvictionAtMaxSize() {
        NoTtlKeyStore store = new NoTtlKeyStore();
        // Offer 7 keys (max is 5)
        for (int i = 1; i <= 7; i++) {
            store.offer("key" + i, "pattern" + i, i * 100L, "CMD" + i);
        }

        assertEquals(5, store.getSamples().size());

        // First should be "key3" (evicted: key1, key2)
        assertEquals("key3", store.getSamples().get(0).getKey());
        // Last should be "key7"
        assertEquals("key7", store.getSamples().get(4).getKey());
    }

    @Test
    public void testEmptyStore() {
        NoTtlKeyStore store = new NoTtlKeyStore();
        assertTrue(store.getSamples().isEmpty());
    }
}
