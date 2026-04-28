package com.yj.redis.monitor.analyzer.increment;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Args {

    private static final Set<String> VALID_KEYS = new HashSet<>(Arrays.asList(
            "host", "port", "duration", "samples-per-pattern",
            "ttl-samples-per-pattern", "upgrade-threshold", "output", "top-n"
    ));

    private static final Set<String> VALID_OUTPUTS = new HashSet<>(Arrays.asList("console", "json"));

    private final String host;
    private final int port;
    private final int durationSec;
    private final int samplesPerPattern;
    private final int ttlSamplesPerPattern;
    private final int upgradeThreshold;
    private final String output;
    private final int topN;

    private Args(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.durationSec = builder.durationSec;
        this.samplesPerPattern = builder.samplesPerPattern;
        this.ttlSamplesPerPattern = builder.ttlSamplesPerPattern;
        this.upgradeThreshold = builder.upgradeThreshold;
        this.output = builder.output;
        this.topN = builder.topN;
    }

    /**
     * Parses command-line arguments in --key=value format.
     *
     * @param args command-line arguments
     * @return populated Args instance
     * @throws IllegalArgumentException if any argument is invalid
     */
    public static Args parse(String[] args) {
        Builder builder = new Builder();

        if (args != null) {
            for (String arg : args) {
                if (!arg.startsWith("--") || !arg.contains("=")) {
                    throw new IllegalArgumentException("Invalid argument format: " + arg);
                }

                String key = arg.substring(2, arg.indexOf('='));
                String value = arg.substring(arg.indexOf('=') + 1);

                if (!VALID_KEYS.contains(key)) {
                    throw new IllegalArgumentException("Unknown argument: --" + key);
                }

                switch (key) {
                    case "host":
                        builder.host = value;
                        break;
                    case "port":
                        builder.port = parsePort(value);
                        break;
                    case "duration":
                        builder.durationSec = parsePositiveInt("duration", value);
                        break;
                    case "samples-per-pattern":
                        builder.samplesPerPattern = parsePositiveInt("samples-per-pattern", value);
                        break;
                    case "ttl-samples-per-pattern":
                        builder.ttlSamplesPerPattern = parsePositiveInt("ttl-samples-per-pattern", value);
                        break;
                    case "upgrade-threshold":
                        builder.upgradeThreshold = parsePositiveInt("upgrade-threshold", value);
                        break;
                    case "output":
                        if (!VALID_OUTPUTS.contains(value)) {
                            throw new IllegalArgumentException("Invalid output value: " + value
                                    + ". Must be one of: " + VALID_OUTPUTS);
                        }
                        builder.output = value;
                        break;
                    case "top-n":
                        builder.topN = parsePositiveInt("top-n", value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: --" + key);
                }
            }
        }

        return new Args(builder);
    }

    private static int parsePositiveInt(String name, String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be positive, got: " + value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid " + name + " value: " + value, e);
        }
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535, got: " + value);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port value: " + value, e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDurationSec() {
        return durationSec;
    }

    public int getSamplesPerPattern() {
        return samplesPerPattern;
    }

    public int getTtlSamplesPerPattern() {
        return ttlSamplesPerPattern;
    }

    public int getUpgradeThreshold() {
        return upgradeThreshold;
    }

    public String getOutput() {
        return output;
    }

    public int getTopN() {
        return topN;
    }

    private static class Builder {
        private String host = "localhost";
        private int port = 6379;
        private int durationSec = 300;
        private int samplesPerPattern = 10;
        private int ttlSamplesPerPattern = 5;
        private int upgradeThreshold = 10;
        private String output = "console";
        private int topN = 20;
    }
}
