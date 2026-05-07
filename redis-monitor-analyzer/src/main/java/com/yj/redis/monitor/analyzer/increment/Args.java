package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.common.MonitorConstants;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Args {

    public enum Source {
        LIVE, FILE
    }

    private static final Set<String> VALID_KEYS = new HashSet<>(Arrays.asList(
            "host", "port", "duration", "samples-per-pattern",
            "ttl-samples-per-pattern", "upgrade-threshold", "output", "top-n", "password",
            "source", "input-dir", "print-interval"
    ));

    private final Source source;
    private final String host;
    private final int port;
    private final int durationSec;
    private final int samplesPerPattern;
    private final int ttlSamplesPerPattern;
    private final int upgradeThreshold;
    private final OutputFormat output;
    private final int topN;
    private final String password;
    private final String inputDir;
    private final int printIntervalSec;

    private Args(Builder builder) {
        this.source = builder.source;
        this.host = builder.host;
        this.port = builder.port;
        this.durationSec = builder.durationSec;
        this.samplesPerPattern = builder.samplesPerPattern;
        this.ttlSamplesPerPattern = builder.ttlSamplesPerPattern;
        this.upgradeThreshold = builder.upgradeThreshold;
        this.output = builder.output;
        this.topN = builder.topN;
        this.password = builder.password;
        this.inputDir = builder.inputDir;
        this.printIntervalSec = builder.printIntervalSec;
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
                if (!arg.startsWith("--")) {
                    throw new IllegalArgumentException("Invalid argument format: " + arg);
                }
                int eqIdx = arg.indexOf('=');
                if (eqIdx < 0) {
                    throw new IllegalArgumentException("Invalid argument format: " + arg);
                }

                String key = arg.substring(2, eqIdx);
                String value = arg.substring(eqIdx + 1);

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
                        try {
                            builder.output = OutputFormat.valueOf(value.toUpperCase());
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException("Invalid output value: " + value
                                    + ". Must be one of: console, json");
                        }
                        break;
                    case "top-n":
                        builder.topN = parsePositiveInt("top-n", value);
                        break;
                    case "password":
                        builder.password = value;
                        break;
                    case "source":
                        try {
                            builder.source = Source.valueOf(value.toUpperCase());
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                    "Invalid source value: " + value + ". Must be: live, file");
                        }
                        break;
                    case "input-dir":
                        builder.inputDir = value;
                        break;
                    case "print-interval":
                        builder.printIntervalSec = parseNonNegativeInt("print-interval", value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: --" + key);
                }
            }
        }

        if (builder.source == Source.FILE && builder.inputDir == null) {
            throw new IllegalArgumentException("--input-dir is required when --source=file");
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

    private static int parseNonNegativeInt(String name, String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(name + " must be non-negative, got: " + value);
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

    public Source getSource() { return source; }

    public String getInputDir() { return inputDir; }

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

    public OutputFormat getOutput() {
        return output;
    }

    public int getTopN() {
        return topN;
    }

    public String getPassword() {
        return password;
    }

    public int getPrintIntervalSec() {
        return printIntervalSec;
    }

    @Override
    public String toString() {
        if (source == Source.FILE) {
            return "source=file, inputDir=" + inputDir +
                    ", duration=" + durationSec + "s" +
                    ", samplesPerPattern=" + samplesPerPattern +
                    ", ttlSamplesPerPattern=" + ttlSamplesPerPattern +
                    ", upgradeThreshold=" + upgradeThreshold +
                    ", topN=" + topN +
                    ", output=" + output.name().toLowerCase() +
                    ", printInterval=" + printIntervalSec + "s";
        }
        return "source=live, host=" + host + ", port=" + port +
                ", duration=" + durationSec + "s" +
                ", samplesPerPattern=" + samplesPerPattern +
                ", ttlSamplesPerPattern=" + ttlSamplesPerPattern +
                ", upgradeThreshold=" + upgradeThreshold +
                ", topN=" + topN +
                ", output=" + output.name().toLowerCase() +
                (password != null ? ", password=***" : "") +
                ", printInterval=" + printIntervalSec + "s";
    }

    private static class Builder {
        private Source source = Source.LIVE;
        private String host = MonitorConstants.DEFAULT_REDIS_HOST;
        private int port = MonitorConstants.DEFAULT_REDIS_PORT;
        private int durationSec = 300;
        private int samplesPerPattern = 10;
        private int ttlSamplesPerPattern = 5;
        private int upgradeThreshold = 10;
        private OutputFormat output = OutputFormat.CONSOLE;
        private int topN = 20;
        private String password = null;
        private String inputDir = null;
        private int printIntervalSec = 30;
    }
}
