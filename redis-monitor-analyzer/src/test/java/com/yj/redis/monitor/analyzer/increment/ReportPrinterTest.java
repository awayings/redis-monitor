package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ReportPrinterTest {

    @Test
    public void testConsoleReportContainsPatternName() {
        PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
        aggregator.recordWrite("user:*:profile");
        aggregator.addMemorySample("user:*:profile", 1024);
        aggregator.recordWrite("user:*:profile");

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
        aggregator.recordWrite("user:*:profile");
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

    @Test
    public void testFileModeConsoleReportShowsSource() {
        PatternStatsAggregator aggregator = new PatternStatsAggregator(5, 10);
        aggregator.recordWrite("user:*:profile");
        aggregator.addMemorySample("user:*:profile", 512);
        aggregator.recordWrite("user:*:profile");

        NoTtlKeyStore noTtlStore = new NoTtlKeyStore();
        ReportPrinter printer = ReportPrinter.forFileMode("/tmp/batch_logs");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        printer.printConsoleFile(aggregator, noTtlStore, 10, 300.0, out);
        String output = baos.toString();

        assertTrue("Output should contain Source:",
                output.contains("Source:"));
        assertTrue("Output should contain input dir path",
                output.contains("/tmp/batch_logs"));
        assertTrue("Output should contain memory estimation footnote",
                output.contains("value string length"));
        assertTrue("Output should contain TTL footnote",
                output.contains("inline"));
    }

    @Test
    public void testPerFileSummary() {
        ReportPrinter printer = ReportPrinter.forFileMode("/tmp/batch_logs");

        Map<String, Long> writesBefore = new HashMap<>();
        Map<String, Long> writesAfter = new HashMap<>();
        writesAfter.put("order:*:detail", 320L);
        writesAfter.put("user:*:profile", 180L);
        writesAfter.put("cache:*", 50L);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        printer.printPerFileSummary("node-53.log", writesBefore, writesAfter,
                150.2, 5, out);
        String output = baos.toString();

        assertTrue("Should contain file name", output.contains("node-53.log"));
        assertTrue("Should contain duration", output.contains("150.2s"));
        assertTrue("Should contain pattern name", output.contains("order:*:detail"));
        assertTrue("Should contain write count", output.contains("320"));
    }
}
