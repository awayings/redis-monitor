package com.yj.redis.monitor.analyzer.increment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import com.google.common.io.BaseEncoding;
import java.nio.charset.StandardCharsets;

public class KeyDeserializer {

    private final boolean jdkFirst;

    public KeyDeserializer(boolean jdkFirst) {
        this.jdkFirst = jdkFirst;
    }

    /**
     * Deserializes a byte array into a String.
     * <p>
     * If jdkFirst is true, attempts JDK deserialization first, then UTF-8 string
     * decoding, then hex encoding as fallback.
     * If jdkFirst is false, attempts UTF-8 string decoding first, then hex encoding.
     *
     * @param bytes the raw key bytes
     * @return the deserialized string (never null)
     */
    public String deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        if (jdkFirst) {
            String result = deserializeJdk(bytes);
            if (result != null) {
                return result;
            }
        }

        String result = deserializeString(bytes);
        if (result != null) {
            return result;
        }

        return bytesToHex(bytes);
    }

    /**
     * Attempts JDK deserialization. Expects the deserialized object to be a String.
     *
     * @return deserialized string, or null if deserialization fails
     */
    private String deserializeJdk(byte[] bytes) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            if (obj instanceof String) {
                return (String) obj;
            }
            return null;
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Attempts to decode bytes as a UTF-8 string.
     * Uses CharsetDecoder with CodingErrorAction.REPORT to detect
     * invalid UTF-8 sequences and fall through to hex encoding.
     *
     * @return decoded string, or null if decoding fails
     */
    private String deserializeString(byte[] bytes) {
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            CharBuffer cb = decoder.decode(bb);
            return cb.toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private String bytesToHex(byte[] bytes) {
        return BaseEncoding.base16().encode(bytes).toLowerCase();
    }
}
