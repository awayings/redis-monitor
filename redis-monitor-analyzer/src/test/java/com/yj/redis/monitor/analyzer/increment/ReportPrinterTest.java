package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.*;

public class ReportPrinterTest {

    @Test
    public void testConsoleReportContainsPatternName() {
        PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
        aggregator.recordWrite("user:*:profile", 0);
        aggregator.addMemorySample("user:*:profile", 1024);
        aggregator.recordWrite("user:*:profile", 0);

        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();
        ReportPrinter printer = new ReportPrinter("localhost", 6379, 60, 10);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        printer.printConsole(aggregator, noTtlStore, 10, out);
        String output = baos.toString();

        assertTrue("Output should contain pattern name",
                output.contains("user:*:profile"));
        assertTrue("Output should contain host",
                output.contains("localhost"));
    }

    @Test
    public void testConsoleReportWithNoTtlSamples() {
        PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();
        noTtlStore.offer("key:1", "key:*", 512, "SETEX");

        ReportPrinter printer = new ReportPrinter("localhost", 6379, 60, 10);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        printer.printConsole(aggregator, noTtlStore, 10, out);
        String output = baos.toString();

        assertTrue("Output should contain 'No-TTL' section header",
                output.contains("No-TTL"));
    }

    @Test
    public void testJsonReportValid() {
        PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
        aggregator.recordWrite("user:*:profile", 0);
        aggregator.addMemorySample("user:*:profile", 2048);

        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();
        ReportPrinter printer = new ReportPrinter("localhost", 6379, 60, 10);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        printer.printJson(aggregator, noTtlStore, 10, out);
        String output = baos.toString();

        assertTrue("JSON should contain meta key",
                output.contains("\"meta\""));
        assertTrue("JSON should contain patterns key",
                output.contains("\"patterns\""));
        assertTrue("JSON should contain pattern name",
                output.contains("user:*:profile"));
    }

    @Test
    public void testEmptyReport() {
        PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();

        ReportPrinter printer = new ReportPrinter("localhost", 6379, 1, 10);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        printer.printConsole(aggregator, noTtlStore, 10, out);
        String output = baos.toString();

        assertTrue("Empty report should contain 0",
                output.contains("0"));
    }
}
