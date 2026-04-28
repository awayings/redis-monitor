package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class ReservoirSamplerTest {

    @Test
    public void testEmptySamplerMeanIsZero() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(5);
        assertEquals(0.0, sampler.mean(), 0.001);
        assertEquals(0, sampler.size());
    }

    @Test
    public void testSamplerLimitsToCapacity() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(3);
        sampler.add(10L);
        sampler.add(20L);
        sampler.add(30L);
        sampler.add(40L);
        assertEquals(3, sampler.size());
    }

    @Test
    public void testMeanWithFullReservoir() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(3);
        sampler.add(10L);
        sampler.add(20L);
        sampler.add(30L);
        double expected = (10.0 + 20.0 + 30.0) / 3.0;
        assertEquals(expected, sampler.mean(), 0.001);
    }

    @Test
    public void testMeanWithPartialReservoir() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(5);
        sampler.add(100L);
        sampler.add(200L);
        assertEquals(150.0, sampler.mean(), 0.001);
    }

    @Test
    public void testIsEmpty() {
        ReservoirSampler<Long> sampler = new ReservoirSampler<>(3);
        assertTrue(sampler.isEmpty());
        sampler.add(1L);
        assertFalse(sampler.isEmpty());
    }
}
