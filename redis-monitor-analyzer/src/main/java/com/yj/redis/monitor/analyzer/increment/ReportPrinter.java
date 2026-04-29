package com.yj.redis.monitor.analyzer.increment;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportPrinter {

    private static final long KB = 1024;
    private static final long MB = 1024 * 1024;
    private static final long GB = 1024 * 1024 * 1024;

    private final String host;
    private final int port;
    private final int durationSec;
    private final int samplesPerPattern;
    private final String inputDir;

    public ReportPrinter(String host, int port, int durationSec, int samplesPerPattern) {
        this.host = host;
        this.port = port;
        this.durationSec = durationSec;
        this.samplesPerPattern = samplesPerPattern;
        this.inputDir = null;
    }

    /**
     * Creates a ReportPrinter for file/batch mode, reading from the given input directory.
     */
    public static ReportPrinter forFileMode(String inputDir) {
        return new ReportPrinter(inputDir);
    }

    private ReportPrinter(String inputDir) {
        this.host = null;
        this.port = -1;
        this.durationSec = 0;
        this.samplesPerPattern = 0;
        this.inputDir = inputDir;
    }

    /**
     * Prints a formatted console report to the given output stream.
     */
    public void printConsole(PatternStatsAggregator aggregator, NoTtlKeyStore noTtlStore,
                             int topN, PrintStream out) {
        List<PatternStats> topPatterns = aggregator.getTopPatterns(topN, durationSec);
        long totalWrites = aggregator.getTotalWriteCount();
        int totalPatterns = aggregator.getPatternCount();

        // Header
        out.println("=== Redis Memory Increment Analyzer Report ===");
        out.println("Host: " + host + ":" + port);
        out.println("Duration: " + durationSec + "s");
        out.println("Total patterns: " + totalPatterns + ", Total writes: " + totalWrites);
        out.println();

        // Table header
        out.printf("%-6s %-30s %-8s %-10s %-10s %-12s %-12s %-12s%n",
                "Rank", "Pattern", "Writes", "Write/s", "AvgTTL", "AvgMem", "Increment", "Balanced");
        out.println(String.format("%-6s %-30s %-8s %-10s %-10s %-12s %-12s %-12s",
                "-----", "------", "------", "-------", "------", "------", "---------", "--------")
                .replace(' ', '-'));
        // Replace spaces in the separator line with dashes for a cleaner look
        // Actually let's just use dashed separators
        out.println("------+--------------------------------+--------+----------+----------+------------+------------+------------");

        // Table body
        int rank = 1;
        for (PatternStats stats : topPatterns) {
            String pattern = truncate(stats.getPattern(), 30);
            long writes = stats.getWriteCount();
            String writeRate = String.format("%.2f", stats.getWriteRatePerSecond(durationSec));
            String avgTtl = formatTtlSeconds(stats.getAvgTtlSeconds());
            String avgMem = formatBytes((long) stats.getAvgMemoryBytes());
            String increment = formatBytes((long) stats.getIncrementBytes());
            String balanced = formatBytes((long) stats.getBalancedBytes(durationSec));

            out.printf("%-6d %-30s %-8d %-10s %-10s %-12s %-12s %-12s%n",
                    rank, pattern, writes, writeRate, avgTtl, avgMem, increment, balanced);
            rank++;
        }

        // Total line
        out.println("------+--------------------------------+--------+----------+----------+------------+------------+------------");
        double totalIncrement = 0;
        double totalBalanced = 0;
        for (PatternStats s : topPatterns) {
            totalIncrement += s.getIncrementBytes();
            totalBalanced += s.getBalancedBytes(durationSec);
        }
        String totalIncrementStr = formatBytes((long) totalIncrement);
        String totalBalancedStr = formatBytes((long) totalBalanced);
        out.printf("%-6s %-30s %-8d %-10s %-10s %-12s %-12s %-12s%n",
                "TOTAL", "", totalWrites, "", "", "", totalIncrementStr, totalBalancedStr);
        out.println();

        // No-TTL samples section
        List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
        if (!noTtlSamples.isEmpty()) {
            out.println("--- No-TTL Samples (keys without TTL detected via TTL command) ---");
            out.printf("%-40s %-30s %-12s %-10s%n",
                    "Key", "Pattern", "Memory", "Command");
            out.println("----------------------------------------+------------------------------+------------+----------");
            for (NoTtlKeySample sample : noTtlSamples) {
                out.printf("%-40s %-30s %-12s %-10s%n",
                        truncate(sample.getKey(), 40),
                        truncate(sample.getPattern(), 30),
                        formatBytes(sample.getMemoryBytes()),
                        sample.getCommand());
            }
            out.println();
        }
    }

    /**
     * Prints a JSON report to the given output stream.
     */
    public void printJson(PatternStatsAggregator aggregator, NoTtlKeyStore noTtlStore,
                          int topN, PrintStream out) {
        List<PatternStats> topPatterns = aggregator.getTopPatterns(topN, durationSec);
        long totalWrites = aggregator.getTotalWriteCount();
        int totalPatterns = aggregator.getPatternCount();

        out.println("{");
        // Meta
        out.println("  \"meta\": {");
        out.println("    \"host\": " + jsonEscape(host) + ",");
        out.println("    \"port\": " + port + ",");
        out.println("    \"durationSec\": " + durationSec + ",");
        out.println("    \"samplesPerPattern\": " + samplesPerPattern + ",");
        out.println("    \"totalPatterns\": " + totalPatterns + ",");
        out.println("    \"totalWriteCount\": " + totalWrites);
        out.println("  },");

        // Patterns
        out.println("  \"patterns\": [");
        for (int i = 0; i < topPatterns.size(); i++) {
            PatternStats stats = topPatterns.get(i);
            out.println("    {");
            out.println("      \"pattern\": " + jsonEscape(stats.getPattern()) + ",");
            out.println("      \"writeCount\": " + stats.getWriteCount() + ",");
            out.println("      \"writeRatePerSecond\": " + String.format("%.2f", stats.getWriteRatePerSecond(durationSec)) + ",");
            out.println("      \"avgTtlSec\": " + String.format("%.1f", stats.getAvgTtlSeconds()) + ",");
            out.println("      \"avgMemoryBytes\": " + Math.round(stats.getAvgMemoryBytes()) + ",");
            out.println("      \"incrementBytes\": " + Math.round(stats.getIncrementBytes()) + ",");
            out.println("      \"balancedBytes\": " + Math.round(stats.getBalancedBytes(durationSec)));
            if (i < topPatterns.size() - 1) {
                out.println("    },");
            } else {
                out.println("    }");
            }
        }
        out.println("  ],");

        // No-TTL samples
        List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
        out.println("  \"noTtlSamples\": [");
        for (int i = 0; i < noTtlSamples.size(); i++) {
            NoTtlKeySample sample = noTtlSamples.get(i);
            out.println("    {");
            out.println("      \"key\": " + jsonEscape(sample.getKey()) + ",");
            out.println("      \"pattern\": " + jsonEscape(sample.getPattern()) + ",");
            out.println("      \"memoryBytes\": " + sample.getMemoryBytes() + ",");
            out.println("      \"command\": " + jsonEscape(sample.getCommand()));
            if (i < noTtlSamples.size() - 1) {
                out.println("    },");
            } else {
                out.println("    }");
            }
        }
        out.println("  ]");
        out.println("}");
    }

    // ---- File-mode methods ----

    /**
     * Prints a console report for file/batch mode, reading from parsed log files.
     * Duration is derived from timestamps, not constructor-injected.
     */
    public void printConsoleFile(PatternStatsAggregator aggregator,
                                  NoTtlKeyStore noTtlStore,
                                  int topN, double actualDurationSec,
                                  PrintStream out) {
        List<PatternStats> topPatterns = aggregator.getTopPatterns(topN, (long) actualDurationSec);
        long totalWrites = aggregator.getTotalWriteCount();
        int totalPatterns = aggregator.getPatternCount();

        out.println("=== Redis Memory Increment Analyzer Report ===");
        out.println("Source: " + inputDir);
        out.println("Duration: " + String.format("%.1f", actualDurationSec) + "s (from timestamps)");
        out.println("Total patterns: " + totalPatterns + ", Total writes: " + totalWrites);
        out.println("Memory: estimated from value string length");
        out.println("TTL: from inline + EXPIRE commands only (no live sampling)");
        out.println();

        out.printf("%-6s %-30s %-8s %-10s %-10s %-12s %-12s %-12s%n",
                "Rank", "Pattern", "Writes", "Write/s", "AvgTTL", "AvgMem", "Increment", "Balanced");
        out.println("------+--------------------------------+--------+----------+----------+------------+------------+------------");

        int rank = 1;
        for (PatternStats stats : topPatterns) {
            String pattern = truncate(stats.getPattern(), 30);
            long writes = stats.getWriteCount();
            String writeRate = String.format("%.2f", stats.getWriteRatePerSecond(actualDurationSec));
            String avgTtl = formatTtlSeconds(stats.getAvgTtlSeconds());
            String avgMem = formatBytes((long) stats.getAvgMemoryBytes());
            String increment = formatBytes((long) stats.getIncrementBytes());
            String balanced = formatBytes((long) stats.getBalancedBytes(actualDurationSec));

            out.printf("%-6d %-30s %-8d %-10s %-10s %-12s %-12s %-12s%n",
                    rank, pattern, writes, writeRate, avgTtl, avgMem, increment, balanced);
            rank++;
        }

        out.println("------+--------------------------------+--------+----------+----------+------------+------------+------------");
        double totalIncrement = 0;
        double totalBalanced = 0;
        for (PatternStats s : topPatterns) {
            totalIncrement += s.getIncrementBytes();
            totalBalanced += s.getBalancedBytes(actualDurationSec);
        }
        out.printf("%-6s %-30s %-8d %-10s %-10s %-12s %-12s %-12s%n",
                "TOTAL", "", totalWrites, "", "", "",
                formatBytes((long) totalIncrement), formatBytes((long) totalBalanced));
        out.println();

        // No-TTL samples section
        List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
        if (!noTtlSamples.isEmpty()) {
            out.println("--- No-TTL Samples (keys without TTL detected) ---");
            out.printf("%-40s %-30s %-12s %-10s%n",
                    "Key", "Pattern", "Memory", "Command");
            out.println("----------------------------------------+------------------------------+------------+----------");
            for (NoTtlKeySample sample : noTtlSamples) {
                out.printf("%-40s %-30s %-12s %-10s%n",
                        truncate(sample.getKey(), 40),
                        truncate(sample.getPattern(), 30),
                        formatBytes(sample.getMemoryBytes()),
                        sample.getCommand());
            }
            out.println();
        }
    }

    /**
     * Prints a per-file summary showing the delta in write counts before and after
     * processing a single log file.
     */
    public void printPerFileSummary(String fileName,
                                     Map<String, Long> writesBefore,
                                     Map<String, Long> writesAfter,
                                     double fileDurationSec,
                                     int topN,
                                     PrintStream out) {
        Map<String, Long> deltaWrites = new HashMap<>();
        for (Map.Entry<String, Long> e : writesAfter.entrySet()) {
            long before = writesBefore.getOrDefault(e.getKey(), 0L);
            long delta = e.getValue() - before;
            if (delta > 0) {
                deltaWrites.put(e.getKey(), delta);
            }
        }

        long totalDelta = 0;
        for (long v : deltaWrites.values()) {
            totalDelta += v;
        }

        out.println();
        out.println("--- File: " + fileName + " ("
                + String.format("%.1f", fileDurationSec) + "s, "
                + totalDelta + " writes, "
                + deltaWrites.size() + " patterns) ---");

        if (deltaWrites.isEmpty()) {
            out.println("  (no write commands found)");
            out.println();
            return;
        }

        List<Map.Entry<String, Long>> sorted = new ArrayList<>(deltaWrites.entrySet());
        sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        int limit = Math.min(topN, sorted.size());

        out.println("Top " + limit + " patterns by write count:");
        out.printf("  %-4s %-30s %-8s %-10s%n", "Rank", "Pattern", "Writes", "Write/s");
        out.println("  " + String.format("%-4s %-30s %-8s %-10s", "----", "------", "------", "-------").replace(' ', '-'));
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, Long> e = sorted.get(i);
            String pattern = truncate(e.getKey(), 30);
            long writes = e.getValue();
            String rate = String.format("%.2f", writes / Math.max(fileDurationSec, 0.001));
            out.printf("  %-4d %-30s %-8d %-10s%n", i + 1, pattern, writes, rate);
        }
        out.println();
    }

    /**
     * Prints a JSON report for file/batch mode.
     */
    public void printJsonFile(PatternStatsAggregator aggregator,
                               NoTtlKeyStore noTtlStore,
                               int topN, double actualDurationSec,
                               PrintStream out) {
        List<PatternStats> topPatterns = aggregator.getTopPatterns(topN, (long) actualDurationSec);
        long totalWrites = aggregator.getTotalWriteCount();
        int totalPatterns = aggregator.getPatternCount();

        out.println("{");
        out.println("  \"meta\": {");
        out.println("    \"source\": " + jsonEscape(inputDir) + ",");
        out.println("    \"sourceType\": \"file\",");
        out.println("    \"durationSec\": " + String.format("%.1f", actualDurationSec) + ",");
        out.println("    \"totalPatterns\": " + totalPatterns + ",");
        out.println("    \"totalWriteCount\": " + totalWrites + ",");
        out.println("    \"memoryEstimation\": \"value_string_length\"");
        out.println("  },");

        out.println("  \"patterns\": [");
        for (int i = 0; i < topPatterns.size(); i++) {
            PatternStats stats = topPatterns.get(i);
            out.println("    {");
            out.println("      \"pattern\": " + jsonEscape(stats.getPattern()) + ",");
            out.println("      \"writeCount\": " + stats.getWriteCount() + ",");
            out.println("      \"writeRatePerSecond\": " + String.format("%.2f", stats.getWriteRatePerSecond(actualDurationSec)) + ",");
            out.println("      \"avgTtlSec\": " + String.format("%.1f", stats.getAvgTtlSeconds()) + ",");
            out.println("      \"avgMemoryBytes\": " + Math.round(stats.getAvgMemoryBytes()) + ",");
            out.println("      \"incrementBytes\": " + Math.round(stats.getIncrementBytes()) + ",");
            out.println("      \"balancedBytes\": " + Math.round(stats.getBalancedBytes(actualDurationSec)));
            if (i < topPatterns.size() - 1) {
                out.println("    },");
            } else {
                out.println("    }");
            }
        }
        out.println("  ],");

        List<NoTtlKeySample> noTtlSamples = noTtlStore.getSamples();
        out.println("  \"noTtlSamples\": [");
        for (int i = 0; i < noTtlSamples.size(); i++) {
            NoTtlKeySample sample = noTtlSamples.get(i);
            out.println("    {");
            out.println("      \"key\": " + jsonEscape(sample.getKey()) + ",");
            out.println("      \"pattern\": " + jsonEscape(sample.getPattern()) + ",");
            out.println("      \"memoryBytes\": " + sample.getMemoryBytes() + ",");
            out.println("      \"command\": " + jsonEscape(sample.getCommand()));
            if (i < noTtlSamples.size() - 1) {
                out.println("    },");
            } else {
                out.println("    }");
            }
        }
        out.println("  ]");
        out.println("}");
    }

    // ---- Helper methods ----

    static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "0 B";
        }
        if (bytes < KB) {
            return bytes + " B";
        }
        if (bytes < MB) {
            return String.format("%.2f KB", bytes / (double) KB);
        }
        if (bytes < GB) {
            return String.format("%.2f MB", bytes / (double) MB);
        }
        return String.format("%.2f GB", bytes / (double) GB);
    }

    static String formatTtlSeconds(double seconds) {
        if (seconds <= 0) {
            return "-";
        }
        if (seconds < 60) {
            return String.format("%.0fs", seconds);
        }
        if (seconds < 3600) {
            return String.format("%.0fm", seconds / 60);
        }
        return String.format("%.1fh", seconds / 3600);
    }

    static String truncate(String value, int maxLen) {
        if (value == null) {
            return "";
        }
        if (value.length() <= maxLen) {
            return value;
        }
        return value.substring(0, maxLen - 1) + "…";
    }

    static String jsonEscape(String value) {
        if (value == null) {
            return "\"\"";
        }
        return '"' + StringEscapeUtils.escapeJson(value) + '"';
    }
}
