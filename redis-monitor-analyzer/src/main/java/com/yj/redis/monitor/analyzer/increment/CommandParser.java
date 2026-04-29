package com.yj.redis.monitor.analyzer.increment;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CommandParser {

    private static final Set<String> WRITE_COMMANDS = new HashSet<>(Arrays.asList(
            "SET", "SETNX", "GETSET", "MSET", "SETEX", "PSETEX",
            "HSET", "HMSET", "HSETNX", "SADD", "ZADD", "RESTORE"
    ));

    private static final Set<String> TTL_COMMANDS = new HashSet<>(Arrays.asList(
            "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"
    ));

    private CommandParser() {
        // static utility class
    }

    /**
     * Parses a MONITOR output line into a ParsedCommand.
     *
     * @param monitorLine a line from Redis MONITOR output
     * @return ParsedCommand for tracked commands, null otherwise
     */
    public static ParsedCommand parse(String monitorLine) {
        if (monitorLine == null || monitorLine.isEmpty()) {
            return null;
        }

        List<String> tokens = tokenize(monitorLine);
        if (tokens.isEmpty()) {
            return null;
        }

        String cmd = tokens.get(0).toUpperCase();

        if (WRITE_COMMANDS.contains(cmd)) {
            String key = tokens.size() > 1 ? tokens.get(1) : null;
            if (key == null || key.isEmpty()) {
                return null;
            }
            key = deserializeKeyIfNeeded(key);
            Long ttl = extractInlineTtl(cmd, tokens);
            long valueSize = computeValueSize(cmd, tokens);
            return new ParsedCommand(cmd, key, ttl, monitorLine, true, valueSize);
        }

        if (TTL_COMMANDS.contains(cmd)) {
            String key = tokens.size() > 1 ? tokens.get(1) : null;
            if (key == null || key.isEmpty()) {
                return null;
            }
            key = deserializeKeyIfNeeded(key);
            String ttlStr = tokens.size() > 2 ? tokens.get(2) : null;
            if (ttlStr == null || ttlStr.isEmpty()) {
                return null;
            }
            Long ttl = calculateTtlForCommand(cmd, ttlStr);
            if (ttl == null) {
                return null;
            }
            return new ParsedCommand(cmd, key, ttl, monitorLine, false, -1);
        }

        return null;
    }

    /**
     * Tokenizes a MONITOR line by extracting double-quoted tokens after the "]" marker.
     */
    private static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        int idx = line.indexOf(']');
        if (idx < 0) {
            return tokens;
        }

        String after = line.substring(idx + 1);
        int i = 0;
        while (i < after.length()) {
            int quoteStart = after.indexOf('"', i);
            if (quoteStart < 0) {
                break;
            }

            int quoteEnd = quoteStart + 1;
            while (quoteEnd < after.length()) {
                char c = after.charAt(quoteEnd);
                if (c == '\\' && quoteEnd + 1 < after.length()) {
                    // Skip escaped character (e.g. \", \\, \xNN, \n, \r, \t)
                    // For \xNN we skip 4 chars total, handled below
                    quoteEnd += 2;
                } else if (c == '"') {
                    break;
                } else {
                    quoteEnd++;
                }
            }
            if (quoteEnd >= after.length()) {
                break;
            }

            tokens.add(after.substring(quoteStart + 1, quoteEnd));
            i = quoteEnd + 1;
        }

        return tokens;
    }

    private static String deserializeKeyIfNeeded(String key) {
        if (key == null || !key.contains("\\")) {
            return key;
        }
        byte[] bytes = decodeEscapesToBytes(key);
        if (bytes.length == 0) {
            return key;
        }
        if (key.contains("\\x")) {
            return new KeyDeserializer(true).deserialize(bytes);
        }
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Decodes Redis MONITOR escape sequences into raw bytes.
     * Handles: \xNN (hex), \" (double quote), \\ (backslash),
     * \n (newline), \r (carriage return), \t (tab).
     */
    static byte[] decodeEscapesToBytes(String escaped) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int i = 0;
        while (i < escaped.length()) {
            char c = escaped.charAt(i);
            if (c == '\\' && i + 1 < escaped.length()) {
                char next = escaped.charAt(i + 1);
                if (next == 'x' && i + 3 < escaped.length()) {
                    String hex = escaped.substring(i + 2, i + 4);
                    try {
                        baos.write(Integer.parseInt(hex, 16));
                    } catch (NumberFormatException e) {
                        baos.write((byte) c);
                        baos.write((byte) next);
                    }
                    i += 4;
                } else if (next == '"') {
                    baos.write((byte) '"');
                    i += 2;
                } else if (next == '\\') {
                    baos.write((byte) '\\');
                    i += 2;
                } else if (next == 'n') {
                    baos.write((byte) '\n');
                    i += 2;
                } else if (next == 'r') {
                    baos.write((byte) '\r');
                    i += 2;
                } else if (next == 't') {
                    baos.write((byte) '\t');
                    i += 2;
                } else {
                    baos.write((byte) c);
                    i++;
                }
            } else {
                baos.write((byte) c);
                i++;
            }
        }
        return baos.toByteArray();
    }

    /**
     * Extracts inline TTL from write commands that include TTL information.
     */
    private static Long extractInlineTtl(String cmd, List<String> tokens) {
        try {
            if ("SETEX".equals(cmd)) {
                // SETEX key seconds value
                if (tokens.size() >= 3) {
                    long seconds = Long.parseLong(tokens.get(2));
                    return seconds * 1000;
                }
                return null;
            }

            if ("PSETEX".equals(cmd)) {
                // PSETEX key milliseconds value
                if (tokens.size() >= 3) {
                    return Long.parseLong(tokens.get(2));
                }
                return null;
            }

            if ("SET".equals(cmd)) {
                // SET key value [EX seconds|PX milliseconds|EXAT timestamp-sec|PXAT timestamp-ms]
                for (int i = 2; i < tokens.size() - 1; i++) {
                    String arg = tokens.get(i);
                    if ("EX".equals(arg)) {
                        long seconds = Long.parseLong(tokens.get(i + 1));
                        return seconds * 1000;
                    }
                    if ("PX".equals(arg)) {
                        return Long.parseLong(tokens.get(i + 1));
                    }
                    if ("EXAT".equals(arg)) {
                        long absSec = Long.parseLong(tokens.get(i + 1));
                        return Math.max(0, absSec * 1000 - System.currentTimeMillis());
                    }
                    if ("PXAT".equals(arg)) {
                        long absMs = Long.parseLong(tokens.get(i + 1));
                        return Math.max(0, absMs - System.currentTimeMillis());
                    }
                }
            }
        } catch (NumberFormatException e) {
            // Fall through
            System.err.println(e.getMessage());
        }
        return null;
    }

    /**
     * Calculates the TTL in milliseconds for TTL-only commands.
     * For EXPIREAT/PEXPIREAT, the TTL is calculated relative to the current time.
     */
    private static Long calculateTtlForCommand(String cmd, String valueStr) {
        try {
            long value = Long.parseLong(valueStr);
            switch (cmd) {
                case "EXPIRE":
                    return value * 1000;
                case "PEXPIRE":
                    return value;
                case "EXPIREAT":
                    return Math.max(0, value * 1000 - System.currentTimeMillis());
                case "PEXPIREAT":
                    return Math.max(0, value - System.currentTimeMillis());
                default:
                    return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Computes the combined string length of value arguments for write commands.
     * This serves as a memory proxy when MEMORY USAGE is unavailable (file mode).
     *
     * @param cmd    the uppercase command name
     * @param tokens the tokenized arguments (index 0 = command, 1 = key, 2+ = args)
     * @return sum of value argument lengths, or -1 if the command has no values
     */
    private static long computeValueSize(String cmd, List<String> tokens) {
        switch (cmd) {
            case "SET":
            case "SETNX":
            case "GETSET":
                return tokens.size() > 2 ? tokens.get(2).length() : -1;
            case "SETEX":
            case "PSETEX":
                return tokens.size() > 3 ? tokens.get(3).length() : -1;
            case "MSET":
                if (tokens.size() < 3) return -1;
                long msetSize = 0;
                for (int i = 2; i < tokens.size(); i += 2) {
                    msetSize += tokens.get(i).length();
                }
                return msetSize;
            case "HSET":
            case "HSETNX":
                return tokens.size() > 3 ? tokens.get(3).length() : -1;
            case "HMSET":
                if (tokens.size() < 4) return -1;
                long hmsetSize = 0;
                for (int i = 3; i < tokens.size(); i += 2) {
                    hmsetSize += tokens.get(i).length();
                }
                return hmsetSize;
            case "SADD":
                if (tokens.size() < 3) return -1;
                long saddSize = 0;
                for (int i = 2; i < tokens.size(); i++) {
                    saddSize += tokens.get(i).length();
                }
                return saddSize;
            case "ZADD":
                if (tokens.size() < 4) return -1;
                long zaddSize = 0;
                for (int i = 3; i < tokens.size(); i += 2) {
                    zaddSize += tokens.get(i).length();
                }
                return zaddSize;
            case "RESTORE":
                return tokens.size() > 3 ? tokens.get(3).length() : -1;
            default:
                return -1;
        }
    }
}
