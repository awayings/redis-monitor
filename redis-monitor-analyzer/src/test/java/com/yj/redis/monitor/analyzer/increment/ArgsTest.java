package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class ArgsTest {

    @Test
    public void testParseDefaults() {
        Args args = Args.parse(new String[]{});
        assertEquals("localhost", args.getHost());
        assertEquals(6379, args.getPort());
        assertEquals(300, args.getDurationSec());
        assertEquals(10, args.getSamplesPerPattern());
        assertEquals(5, args.getTtlSamplesPerPattern());
        assertEquals(10, args.getUpgradeThreshold());
        assertEquals("console", args.getOutput());
        assertEquals(20, args.getTopN());
    }

    @Test
    public void testParseHostAndPort() {
        Args args = Args.parse(new String[]{"--host=10.0.0.1", "--port=6380"});
        assertEquals("10.0.0.1", args.getHost());
        assertEquals(6380, args.getPort());
    }

    @Test
    public void testParseDuration() {
        Args args = Args.parse(new String[]{"--duration=60"});
        assertEquals(60, args.getDurationSec());
    }

    @Test
    public void testParseSamplesPerPattern() {
        Args args = Args.parse(new String[]{"--samples-per-pattern=20"});
        assertEquals(20, args.getSamplesPerPattern());
    }

    @Test
    public void testParseOutputJson() {
        Args args = Args.parse(new String[]{"--output=json"});
        assertEquals("json", args.getOutput());
    }

    @Test
    public void testParseTopN() {
        Args args = Args.parse(new String[]{"--top-n=5"});
        assertEquals(5, args.getTopN());
    }

    @Test
    public void testParseAllArgs() {
        Args args = Args.parse(new String[]{
                "--host=10.0.0.1",
                "--port=6380",
                "--duration=60",
                "--samples-per-pattern=20",
                "--ttl-samples-per-pattern=10",
                "--upgrade-threshold=5",
                "--output=json",
                "--top-n=10"
        });
        assertEquals("10.0.0.1", args.getHost());
        assertEquals(6380, args.getPort());
        assertEquals(60, args.getDurationSec());
        assertEquals(20, args.getSamplesPerPattern());
        assertEquals(10, args.getTtlSamplesPerPattern());
        assertEquals(5, args.getUpgradeThreshold());
        assertEquals("json", args.getOutput());
        assertEquals(10, args.getTopN());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDurationThrows() {
        Args.parse(new String[]{"--duration=-1"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownArgThrows() {
        Args.parse(new String[]{"--unknown=value"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidOutputThrows() {
        Args.parse(new String[]{"--output=xml"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMalformedArgThrows() {
        Args.parse(new String[]{"--bad-format"});
    }

    @Test
    public void testParseTtlSamplesPerPattern() {
        Args args = Args.parse(new String[]{"--ttl-samples-per-pattern=8"});
        assertEquals(8, args.getTtlSamplesPerPattern());
    }

    @Test
    public void testParseUpgradeThreshold() {
        Args args = Args.parse(new String[]{"--upgrade-threshold=15"});
        assertEquals(15, args.getUpgradeThreshold());
    }

    @Test
    public void testInvalidPortThrows() {
        try {
            Args.parse(new String[]{"--port=99999"});
            fail("Expected IllegalArgumentException for invalid port");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
