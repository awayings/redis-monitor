package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import redis.clients.jedis.Jedis;

import java.io.PrintStream;
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
    private volatile MonitorStream monitorStream;

    public MemoryIncrementAnalyzer(Args args) {
        this.args = args;
        this.clusterer = new PatternClusterer(args.getUpgradeThreshold(), 10000);
        this.aggregator = new PatternStatsAggregator(
                args.getTtlSamplesPerPattern(), args.getSamplesPerPattern());
        this.printer = new ReportPrinter(args.getHost(), args.getPort(),
                args.getDurationSec(), args.getSamplesPerPattern());
        this.noTtlStore = new NoTtlKeyStore();
        this.sampleQueue = new LinkedBlockingQueue<>();
        this.memorySampler = new MemorySamplerThread(args.getHost(), args.getPort(), sampleQueue);
        this.ttlSampler = new TtlSampler(args.getHost(), args.getPort(),
                aggregator, args.getTtlSamplesPerPattern());
        this.factory = new RedisConnectionFactory(args.getHost(), args.getPort(), 2000, 5000, null);
        this.interrupted = false;
        this.reportPrinted = false;
    }

    public void run() {
        // Register shutdown hook for Ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            interrupted = true;
            if (monitorStream != null) {
                monitorStream.stop();
            }
            if (!reportPrinted) {
                printReport();
            }
        }, "ShutdownHook"));

        // Start sampler threads
        memorySampler.start();
        ttlSampler.start();

        // Run MONITOR loop with retries
        runMonitorLoop();

        // Print final report
        if (!reportPrinted) {
            printReport();
        }

        // Shutdown sampler threads
        memorySampler.shutdown();
        ttlSampler.shutdown();
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
        aggregator.recordWrite(pattern, 0);

        if (cmd.getTtlMillis() != null) {
            aggregator.addTtlSample(pattern, cmd.getTtlMillis());
            aggregator.markTtlFromCommand(pattern);
        } else {
            ttlSampler.scheduleDelayedTtl(cmd.getKey(), pattern);
        }

        // Enqueue for memory sampling if pattern hasn't reached quota
        PatternStats stats = aggregator.getStats(pattern);
        if (stats != null && stats.getMemorySampleCount() < args.getSamplesPerPattern()) {
            String key = cmd.getKey();
            sampleQueue.offer(new SampleTask(key, memory -> aggregator.addMemorySample(pattern, memory)));
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
                // Use the first few memory samples for reporting
                long memoryBytes = (long) stats.getAvgMemoryBytes();
                noTtlStore.offer(stats.getPattern(), stats.getPattern(),
                        memoryBytes, "TTL");
            }
        }

        PrintStream out = System.out;
        if ("json".equals(args.getOutput())) {
            printer.printJson(aggregator, noTtlStore, args.getTopN(), out);
        } else {
            printer.printConsole(aggregator, noTtlStore, args.getTopN(), out);
        }

        if (interrupted) {
            out.println("[Interrupted by user — partial report shown]");
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
    }
}
