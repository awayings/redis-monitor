package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MemoryIncrementAnalyzer {

    private final Args args;
    private final PatternClusterer clusterer;
    private final PatternStatsAggregator aggregator;
    private final ReportPrinter printer;
    private final NoTtlKeyStore noTtlStore;
    private final BlockingQueue<SampleTask> sampleQueue;
    private final MemorySamplerThread memorySampler;
    private final TtlSampler ttlSampler;
    private final RedisConnectionFactory factory;

    private volatile boolean interrupted;
    private volatile boolean reportPrinted;
    private volatile double durationSec;
    private volatile MonitorStream monitorStream;

    public MemoryIncrementAnalyzer(Args args) {
        this.args = args;
        this.clusterer = new PatternClusterer(args.getUpgradeThreshold(), 10000);
        this.aggregator = new PatternStatsAggregator(
                args.getTtlSamplesPerPattern(), args.getSamplesPerPattern());
        this.noTtlStore = new NoTtlKeyStore();
        this.interrupted = false;
        this.reportPrinted = false;

        if (args.getSource() == Args.Source.LIVE) {
            this.sampleQueue = new LinkedBlockingQueue<>();
            this.factory = new RedisConnectionFactory(args.getHost(), args.getPort(), args.getPassword());
            this.memorySampler = new MemorySamplerThread(factory, sampleQueue);
            this.ttlSampler = new TtlSampler(factory, aggregator, args.getTtlSamplesPerPattern());
            this.printer = new ReportPrinter(args.getHost(), args.getPort(),
                    args.getDurationSec(), args.getSamplesPerPattern());
        } else {
            this.sampleQueue = null;
            this.factory = null;
            this.memorySampler = null;
            this.ttlSampler = null;
            this.printer = ReportPrinter.forFileMode(args.getInputDir());
        }
    }

    public void run() {
        if (args.getSource() == Args.Source.FILE) {
            runFileMode();
        } else {
            runLiveMode();
        }
    }

    private void runLiveMode() {
        System.out.println("Redis Memory Increment Analyzer");
        System.out.println("Config: " + args);
        System.out.println();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            interrupted = true;
            if (monitorStream != null) {
                monitorStream.stop();
            }
            if (!reportPrinted) {
                printReport();
            }
        }, "ShutdownHook"));

        memorySampler.start();
        ttlSampler.start();

        runMonitorLoop();

        if (!reportPrinted) {
            printReport();
        }

        memorySampler.shutdown();
        ttlSampler.shutdown();
    }

    private void runFileMode() {
        System.out.println("Redis Memory Increment Analyzer (File Mode)");
        System.out.println("Config: " + args);
        System.out.println();

        FileLineSource source = new FileLineSource(args.getInputDir());
        List<File> files = source.listLogFiles();

        if (files.isEmpty()) {
            System.out.println("Warning: No .log files found in " + args.getInputDir());
            return;
        }

        // Register shutdown hook after confirming there is work to do
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            interrupted = true;
            if (!reportPrinted) {
                printReportFile();
            }
        }, "ShutdownHook"));

        System.out.println("Found " + files.size() + " .log file(s)");
        System.out.println();

        // Per-file progress goes to stdout for console, stderr for JSON
        PrintStream perFileOut = args.getOutput() == OutputFormat.JSON ? System.err : System.out;

        double globalFirstTs = Double.MAX_VALUE;
        double globalLastTs = 0;

        for (int i = 0; i < files.size() && !interrupted; i++) {
            File file = files.get(i);
            String fileName = file.getName();
            perFileOut.println("=== File: " + fileName + " (" + (i + 1) + "/" + files.size() + ") ===");

            Map<String, Long> writesBefore = aggregator.getWriteCountSnapshot();

            try {
                double[] ts = source.readFile(file, new MonitorLineHandler() {
                    @Override
                    public void onLine(String line) {
                        processLine(line);
                    }
                    @Override
                    public void onError(Exception e) {
                        System.err.println("[File] Error: " + e.getMessage());
                    }
                });

                if (ts[0] >= 0) {
                    if (ts[0] < globalFirstTs) globalFirstTs = ts[0];
                    if (ts[1] > globalLastTs) globalLastTs = ts[1];

                    Map<String, Long> writesAfter = aggregator.getWriteCountSnapshot();
                    double fileDuration = ts[1] - ts[0];
                    printer.printPerFileSummary(fileName, writesBefore, writesAfter,
                            fileDuration, args.getTopN(), perFileOut);
                }
            } catch (IOException e) {
                System.err.println("[File] Error reading " + fileName + ": " + e.getMessage());
            }
        }

        this.durationSec = (globalLastTs > globalFirstTs) ? (globalLastTs - globalFirstTs) : 0;

        if (!reportPrinted) {
            printReportFile();
        }
    }

    private void runMonitorLoop() {
        int maxAttempts = 3;
        for (int attempt = 0; attempt < maxAttempts && !interrupted; attempt++) {
            try (Jedis jedis = factory.createConnection()) {
                MonitorStream stream = new MonitorStream(jedis, args.getDurationSec());
                this.monitorStream = stream;
                stream.start(new MonitorLineHandler() {
                    @Override
                    public void onLine(String line) {
                        processLine(line);
                    }

                    @Override
                    public void onError(Exception e) {
                        if (!interrupted) {
                            System.err.println("[Monitor] Error: " + e.getMessage());
                        }
                    }
                });
                // Normal completion
                return;
            } catch (Exception e) {
                if (interrupted) {
                    return;
                }
                if (attempt == maxAttempts - 1) {
                    System.err.println("[Monitor] Failed after " + maxAttempts + " attempts: " + e.getMessage());
                    if (!reportPrinted) {
                        printReport();
                    }
                    System.exit(1);
                } else {
                    System.err.println("[Monitor] Error (attempt " + (attempt + 1) + "/"
                            + maxAttempts + "): " + e.getMessage() + " — retrying in 1s...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } finally {
                this.monitorStream = null;
            }
        }
    }

    /**
     * Processes a single MONITOR line.
     */
    private void processLine(String line) {
        ParsedCommand cmd = CommandParser.parse(line);
        if (cmd == null) {
            return;
        }

        String pattern = clusterer.cluster(cmd.getKey());

        if (cmd.isWriteCommand()) {
            aggregator.recordWrite(pattern);
            aggregator.setRepresentativeKeyIfAbsent(pattern, cmd.getKey());

            if (args.getSource() == Args.Source.LIVE) {
                if (cmd.getTtlMillis() != null) {
                    aggregator.addTtlSample(pattern, cmd.getTtlMillis());
                    aggregator.markTtlFromCommand(pattern);
                } else {
                    ttlSampler.scheduleDelayedTtl(cmd.getKey(), pattern);
                }

                PatternStats stats = aggregator.getStats(pattern);
                if (stats != null && stats.getMemorySampleCount() < args.getSamplesPerPattern()) {
                    sampleQueue.offer(new SampleTask(cmd.getKey(),
                            memory -> aggregator.addMemorySample(pattern, memory)));
                }
            } else {
                if (cmd.getTtlMillis() != null) {
                    aggregator.addTtlSample(pattern, cmd.getTtlMillis());
                    aggregator.markTtlFromCommand(pattern);
                }

                if (cmd.getValueSize() >= 0) {
                    aggregator.addMemorySample(pattern, cmd.getValueSize());
                }
            }
        } else if (cmd.getTtlMillis() != null) {
            aggregator.addTtlSample(pattern, cmd.getTtlMillis());
            aggregator.markTtlFromCommand(pattern);
        }
    }

    /**
     * Prints the final report. Can be called from the shutdown hook or after
     * normal completion.
     */
    private void printReport() {
        if (reportPrinted) {
            return;
        }
        reportPrinted = true;

        // Move patterns with no TTL to the noTtlStore
        for (PatternStats stats : aggregator.getAllStats()) {
            if (!stats.getTtlSamples().isEmpty() && stats.getAvgTtlSeconds() <= 0) {
                long memoryBytes = (long) stats.getAvgMemoryBytes();
                String repKey = stats.getRepresentativeKey() != null
                        ? stats.getRepresentativeKey() : stats.getPattern();
                noTtlStore.offer(repKey, stats.getPattern(),
                        memoryBytes, "TTL");
            }
        }

        PrintStream out = System.out;
        if (args.getOutput() == OutputFormat.JSON) {
            printer.printJson(aggregator, noTtlStore, args.getTopN(), out);
        } else {
            printer.printConsole(aggregator, noTtlStore, args.getTopN(), out);
        }

        if (interrupted) {
            out.println("[Interrupted by user — partial report shown]");
        }
    }

    private void printReportFile() {
        if (reportPrinted) {
            return;
        }
        reportPrinted = true;

        for (PatternStats stats : aggregator.getAllStats()) {
            if (!stats.getTtlSamples().isEmpty() && stats.getAvgTtlSeconds() <= 0) {
                long memoryBytes = (long) stats.getAvgMemoryBytes();
                String repKey = stats.getRepresentativeKey() != null
                        ? stats.getRepresentativeKey() : stats.getPattern();
                noTtlStore.offer(repKey, stats.getPattern(), memoryBytes, "TTL");
            }
        }

        java.io.PrintStream out = System.out;
        if (args.getOutput() == OutputFormat.JSON) {
            printer.printJsonFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
        } else {
            printer.printConsoleFile(aggregator, noTtlStore, args.getTopN(), durationSec, out);
        }

        if (interrupted) {
            out.println("[Interrupted by user -- partial report shown]");
        }
    }

    // ---- Integration testing support ----

    PatternStatsAggregator getAggregator() {
        return aggregator;
    }

    // ---- Main entry point ----

    public static void main(String[] args) {
        try {
            Args parsed = Args.parse(args);
            new MemoryIncrementAnalyzer(parsed).run();
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            printUsage(System.err);
            System.exit(1);
        }
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: java ... [options]");
        out.println("Options:");
        out.println("  --host=<host>              Redis host (default: localhost)");
        out.println("  --port=<port>              Redis port (default: 6379)");
        out.println("  --duration=<sec>           Monitoring duration in seconds (default: 300)");
        out.println("  --samples-per-pattern=<n>  Max MEMORY USAGE samples per pattern (default: 10)");
        out.println("  --ttl-samples-per-pattern=<n> Max TTL samples per pattern (default: 5)");
        out.println("  --upgrade-threshold=<n>    Pattern upgrade threshold (default: 10)");
        out.println("  --top-n=<n>                Top N patterns to report (default: 20)");
        out.println("  --output=<console|json>    Output format (default: console)");
        out.println("  --password=<password>      Redis password (default: none)");
    }
}
