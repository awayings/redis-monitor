package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class CommandParserTest {

    @Test
    public void testParseSet() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertNull(cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseSetex() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETEX\" \"key1\" \"3600\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SETEX", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertEquals(Long.valueOf(3600000L), cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseSetWithEx() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"value1\" \"EX\" \"1800\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertEquals(Long.valueOf(1800000L), cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseExpire() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"EXPIRE\" \"key1\" \"600\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("EXPIRE", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertEquals(Long.valueOf(600000L), cmd.getTtlMillis());
        assertFalse(cmd.isWriteCommand());
    }

    @Test
    public void testParseHset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"HSET\" \"hashkey\" \"field1\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("HSET", cmd.getCommandName());
        assertEquals("hashkey", cmd.getKey());
        assertNull(cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseSadd() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SADD\" \"setkey\" \"member1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SADD", cmd.getCommandName());
        assertEquals("setkey", cmd.getKey());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseNonWriteCommand() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"GET\" \"key1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNull(cmd);
    }

    @Test
    public void testParseMalformedLine() {
        assertNull(CommandParser.parse("garbage"));
    }

    @Test
    public void testParsePsetex() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"PSETEX\" \"key1\" \"5000\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("PSETEX", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertEquals(Long.valueOf(5000L), cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseSetWithPx() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"value1\" \"PX\" \"3000\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertEquals(Long.valueOf(3000L), cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParsePexpire() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"PEXPIRE\" \"key1\" \"4000\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("PEXPIRE", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertEquals(Long.valueOf(4000L), cmd.getTtlMillis());
        assertFalse(cmd.isWriteCommand());
    }

    @Test
    public void testParseExpireat() {
        // EXPIREAT value is an absolute Unix timestamp in seconds
        // We'll just verify it parses and produces a non-negative TTL
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"EXPIREAT\" \"key1\" \"2000000000\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("EXPIREAT", cmd.getCommandName());
        assertEquals("key1", cmd.getKey());
        assertNotNull(cmd.getTtlMillis());
        // For a future timestamp, TTL should be positive
        assertTrue(cmd.getTtlMillis() > 0);
        assertFalse(cmd.isWriteCommand());
    }

    @Test
    public void testParseSetnx() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETNX\" \"lockkey\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SETNX", cmd.getCommandName());
        assertEquals("lockkey", cmd.getKey());
        assertNull(cmd.getTtlMillis());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseMset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"MSET\" \"k1\" \"v1\" \"k2\" \"v2\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("MSET", cmd.getCommandName());
        assertEquals("k1", cmd.getKey());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseZadd() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"ZADD\" \"zsetkey\" \"1.0\" \"member1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("ZADD", cmd.getCommandName());
        assertEquals("zsetkey", cmd.getKey());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseRestore() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"RESTORE\" \"restorekey\" \"0\" \"\\x00\\x01...\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("RESTORE", cmd.getCommandName());
        assertEquals("restorekey", cmd.getKey());
        assertTrue(cmd.isWriteCommand());
    }

    @Test
    public void testParseUnknownCommand() {
        // UNKNOWN is not a tracked command
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"UNKNOWN\" \"key1\"";
        assertNull(CommandParser.parse(line));
    }

    @Test
    public void testParseEmptyLine() {
        assertNull(CommandParser.parse(""));
        assertNull(CommandParser.parse(null));
    }
}
