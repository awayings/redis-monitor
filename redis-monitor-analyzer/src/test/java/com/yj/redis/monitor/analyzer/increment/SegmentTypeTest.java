package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class SegmentTypeTest {

    // --- detect() tests ---

    @Test
    public void testDetectNumber() {
        assertEquals(SegmentType.NUMBER, SegmentType.detect("12345"));
        assertEquals(SegmentType.NUMBER, SegmentType.detect("0"));
        assertEquals(SegmentType.NUMBER, SegmentType.detect("00042"));
    }

    @Test
    public void testDetectUuid() {
        assertEquals(SegmentType.UUID, SegmentType.detect("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals(SegmentType.UUID, SegmentType.detect("550e8400e29b41d4a716446655440000"));
    }

    @Test
    public void testDetectDate() {
        assertEquals(SegmentType.DATE, SegmentType.detect("2024-01-15"));
        assertEquals(SegmentType.DATE, SegmentType.detect("2024/01/15"));
        assertEquals(SegmentType.DATE, SegmentType.detect("20240115"));
    }

    @Test
    public void testDetectHex() {
        assertEquals(SegmentType.HEX, SegmentType.detect("deadbeefcafebabe01234567"));
    }

    @Test
    public void testDetectFixed() {
        assertEquals(SegmentType.FIXED, SegmentType.detect("users"));
        assertEquals(SegmentType.FIXED, SegmentType.detect("abc"));
        assertEquals(SegmentType.FIXED, SegmentType.detect("user_data_v2"));
    }

    // --- matches() tests ---

    @Test
    public void testMatchNumber() {
        assertTrue(SegmentType.NUMBER.matches("12345"));
        assertTrue(SegmentType.NUMBER.matches("0"));
        assertFalse(SegmentType.NUMBER.matches("abc"));
        assertFalse(SegmentType.NUMBER.matches("12a34"));
    }

    @Test
    public void testMatchUuid() {
        assertTrue(SegmentType.UUID.matches("550e8400-e29b-41d4-a716-446655440000"));
        assertTrue(SegmentType.UUID.matches("550e8400e29b41d4a716446655440000"));
        assertFalse(SegmentType.UUID.matches("not-a-uuid"));
        assertFalse(SegmentType.UUID.matches("12345"));
    }

    @Test
    public void testMatchDate() {
        assertTrue(SegmentType.DATE.matches("2024-01-15"));
        assertTrue(SegmentType.DATE.matches("2024/01/15"));
        assertTrue(SegmentType.DATE.matches("20240115"));
        assertFalse(SegmentType.DATE.matches("2024-1-15"));
        assertFalse(SegmentType.DATE.matches("hello"));
    }

    @Test
    public void testMatchHex() {
        assertTrue(SegmentType.HEX.matches("deadbeefcafebabe01234567"));
        assertTrue(SegmentType.HEX.matches("AAAAAAAAAAAAAAAA")); // 16 hex chars
        assertFalse(SegmentType.HEX.matches("abc123")); // only 6 chars
        assertFalse(SegmentType.HEX.matches("xyzxyzxyzxyzxyzz")); // non-hex chars
    }

    @Test
    public void testMatchFixed() {
        assertTrue(SegmentType.FIXED.matches("anything"));
        assertTrue(SegmentType.FIXED.matches(""));
    }

    // --- isVariable() tests ---

    @Test
    public void testIsVariable() {
        assertTrue(SegmentType.NUMBER.isVariable());
        assertTrue(SegmentType.UUID.isVariable());
        assertTrue(SegmentType.DATE.isVariable());
        assertTrue(SegmentType.HEX.isVariable());
        assertTrue(SegmentType.STRING.isVariable());
        assertFalse(SegmentType.FIXED.isVariable());
    }

    // --- Detection priority: NUMBER must match before HEX ---

    @Test
    public void testNumberTakesPriorityOverHex() {
        // "1234567890123456" is 16 digits — matches both NUMBER and HEX.
        // NUMBER should win due to detection order.
        assertEquals(SegmentType.NUMBER, SegmentType.detect("1234567890123456"));
    }

    @Test
    public void testHexDoesNotMatchShortStrings() {
        assertEquals(SegmentType.FIXED, SegmentType.detect("abc123"));
    }
}
