package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class KeyDeserializerTest {

    @Test
    public void testDeserializePlainString() {
        KeyDeserializer deserializer = new KeyDeserializer(false);
        String result = deserializer.deserialize("user:123".getBytes());
        assertEquals("user:123", result);
    }

    @Test
    public void testDeserializeNonUtf8Bytes() {
        KeyDeserializer deserializer = new KeyDeserializer(false);
        String result = deserializer.deserialize(new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFD});
        assertEquals("fffefd", result);
    }

    @Test
    public void testDeserializeEmptyBytes() {
        KeyDeserializer deserializer = new KeyDeserializer(false);
        String result = deserializer.deserialize(new byte[0]);
        assertEquals("", result);
    }

    @Test
    public void testJdkFirstThenStringFallback() {
        KeyDeserializer deserializer = new KeyDeserializer(true);
        byte[] plainBytes = "hello123".getBytes();
        String result = deserializer.deserialize(plainBytes);
        assertNotNull(result);
    }
}
