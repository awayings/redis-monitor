package com.yj.redis.monitor.analyzer.increment;

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
            Long ttl = extractInlineTtl(cmd, tokens);
            return new ParsedCommand(cmd, key, ttl, monitorLine, true);
        }

        if (TTL_COMMANDS.contains(cmd)) {
            String key = tokens.size() > 1 ? tokens.get(1) : null;
            if (key == null || key.isEmpty()) {
                return null;
            }
            String ttlStr = tokens.size() > 2 ? tokens.get(2) : null;
            if (ttlStr == null || ttlStr.isEmpty()) {
                return null;
            }
            Long ttl = calculateTtlForCommand(cmd, ttlStr);
            if (ttl == null) {
                return null;
            }
            return new ParsedCommand(cmd, key, ttl, monitorLine, false);
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
            while (quoteEnd < after.length() && after.charAt(quoteEnd) != '"') {
                quoteEnd++;
            }
            if (quoteEnd >= after.length()) {
                // Unclosed quote, skip this and any subsequent tokens
                break;
            }

            tokens.add(after.substring(quoteStart + 1, quoteEnd));
            i = quoteEnd + 1;
        }

        return tokens;
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
}
