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

    @Test
    public void testValueSizeForSet() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key1\" \"hello\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // "hello".length()
    }

    @Test
    public void testValueSizeForSetex() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETEX\" \"key1\" \"100\" \"hello\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // "hello".length()
    }

    @Test
    public void testValueSizeForHset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"HSET\" \"hk\" \"f1\" \"v1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(2, cmd.getValueSize()); // token[3] = "v1".length()
    }

    @Test
    public void testValueSizeForSadd() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SADD\" \"sk\" \"m1\" \"m22\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // "m1".length() + "m22".length() = 2+3
    }

    @Test
    public void testValueSizeForZadd() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"ZADD\" \"zk\" \"1.0\" \"m1\" \"2.0\" \"m22\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // "m1".length() + "m22".length() = 2+3
    }

    @Test
    public void testValueSizeForMset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"MSET\" \"k1\" \"v1\" \"k2\" \"v22\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // "v1".length() + "v22".length() = 2+3
    }

    @Test
    public void testValueSizeForSetnx() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SETNX\" \"lockkey\" \"lockval\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(7, cmd.getValueSize()); // "lockval".length()
    }

    @Test
    public void testValueSizeForExpireIsZero() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"EXPIRE\" \"key1\" \"600\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(-1, cmd.getValueSize()); // TTL-only commands have no value
    }

    @Test
    public void testValueSizeForRestore() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"RESTORE\" \"rk\" \"0\" \"serialized_value\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(16, cmd.getValueSize()); // "serialized_value".length()
    }

    @Test
    public void testValueSizeForPsetex() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"PSETEX\" \"key1\" \"5000\" \"value\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // token[3] = "value".length()
    }

    @Test
    public void testValueSizeForGetset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"GETSET\" \"key1\" \"newval\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(6, cmd.getValueSize()); // token[2] = "newval".length()
    }

    @Test
    public void testValueSizeForHmset() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"HMSET\" \"hk\" \"f1\" \"v1\" \"f2\" \"v22\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals(5, cmd.getValueSize()); // token[3]+token[5] = "v1".length() + "v22".length() = 2+3
    }

    @Test
    public void testDecodeEscapesToBytes() {
        byte[] bytes = CommandParser.decodeEscapesToBytes("hello\\x00world");
        assertEquals(11, bytes.length);
        assertEquals('h', bytes[0]);
        assertEquals(0x00, bytes[5]);
        assertEquals('w', bytes[6]);
    }

    @Test
    public void testDecodeEscapesToBytesEscapedQuote() {
        byte[] bytes = CommandParser.decodeEscapesToBytes("key\\\"name");
        assertEquals(8, bytes.length);
        assertEquals('"', bytes[3]);
    }

    @Test
    public void testDeserializeJdkStringKey_device_code() {
        // JDK serialized string: "device_code_send_num_pc-a505.3b696eb1e-7b2c.192c150ec"
        // Header: AC ED 00 05, TC_STRING: 74, length: 00 35 (53 chars)
        String key = "\\xac\\xed\\x00\\x05t\\x005device_code_send_num_pc-a505.3b696eb1e-7b2c.192c150ec";
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \""
                + key + "\" \"value1\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("device_code_send_num_pc-a505.3b696eb1e-7b2c.192c150ec", cmd.getKey());
        assertEquals(6, cmd.getValueSize()); // "value1".length()
    }

    @Test
    public void testDeserializeJdkStringKey_comment_push() {
        // JDK serialized string: "COMMENT_PUSH_2026-04-272026040920334590490028"
        // Header: AC ED 00 05, TC_STRING: 74, length: 00 2D (45 chars)
        String key = "\\xac\\xed\\x00\\x05t\\x00-COMMENT_PUSH_2026-04-272026040920334590490028";
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \""
                + key + "\" \"some_value\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("COMMENT_PUSH_2026-04-272026040920334590490028", cmd.getKey());
    }

    @Test
    public void testTokenizeWithEscapedQuote() {
        // The key contains \" which should not split the token, and should be decoded to "
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key\\\"1\" \"val\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("SET", cmd.getCommandName());
        assertEquals("key\"1", cmd.getKey());
        assertEquals(3, cmd.getValueSize());
    }

    @Test
    public void testTokenizeWithBackslash() {
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"key\\\\path\" \"val\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("key\\path", cmd.getKey());
    }

    @Test
    public void testNonHexKeyUnchanged() {
        // Keys without \x should pass through unchanged
        String line = "1234567890.123456 [0 127.0.0.1:12345] \"SET\" \"normal:key:123\" \"val\"";
        ParsedCommand cmd = CommandParser.parse(line);
        assertNotNull(cmd);
        assertEquals("normal:key:123", cmd.getKey());
    }
}
