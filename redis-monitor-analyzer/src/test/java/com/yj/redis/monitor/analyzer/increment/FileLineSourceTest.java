package com.yj.redis.monitor.analyzer.increment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FileLineSourceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testListLogFiles() throws IOException {
        tempFolder.newFile("a.log");
        tempFolder.newFile("b.log");
        tempFolder.newFile("readme.txt");   // non-.log, excluded
        tempFolder.newFile("data.LOG");     // case sensitive, excluded

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        List<File> files = source.listLogFiles();

        assertEquals(2, files.size());
        assertTrue(files.get(0).getName().endsWith(".log"));
        assertTrue(files.get(1).getName().endsWith(".log"));
    }

    @Test
    public void testExtractTimestamp() {
        String line = "1777329020.191216 [0 10.0.0.53:33414] \"SETEX\" \"key\" \"100\" \"val\"";
        Double ts = FileLineSource.extractTimestamp(line);
        assertEquals(1777329020.191216, ts, 0.000001);
    }

    @Test
    public void testExtractTimestampMalformed() {
        assertNull(FileLineSource.extractTimestamp("no_timestamp_here"));
        assertNull(FileLineSource.extractTimestamp(""));
        assertNull(FileLineSource.extractTimestamp(null));
    }

    @Test
    public void testReadFile() throws IOException {
        File logFile = tempFolder.newFile("test.log");
        try (FileWriter fw = new FileWriter(logFile)) {
            fw.write("100.000 [0 10.0.0.1:1234] \"SET\" \"key1\" \"val1\"\n");
            fw.write("101.000 [0 10.0.0.1:1234] \"SET\" \"key2\" \"val2\"\n");
            fw.write("102.000 [0 10.0.0.1:1234] \"SET\" \"key3\" \"val3\"\n");
        }

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        AtomicInteger lineCount = new AtomicInteger(0);

        double[] ts = source.readFile(logFile, new MonitorLineHandler() {
            @Override
            public void onLine(String line) {
                lineCount.incrementAndGet();
            }
            @Override
            public void onError(Exception e) {
                fail("Should not error: " + e.getMessage());
            }
        });

        assertEquals(3, lineCount.get());
        assertEquals(100.0, ts[0], 0.001);
        assertEquals(102.0, ts[1], 0.001);
    }

    @Test
    public void testReadFileReturnsNegativeTimestampsForEmpty() throws IOException {
        File logFile = tempFolder.newFile("empty.log");

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        double[] ts = source.readFile(logFile, new MonitorLineHandler() {
            @Override public void onLine(String line) { }
            @Override public void onError(Exception e) { }
        });

        assertTrue("First ts should be negative for empty file", ts[0] < 0);
        assertTrue("Last ts should be negative for empty file", ts[1] < 0);
    }

    @Test
    public void testReadFilePassesAllLinesIncludingMalformed() throws IOException {
        File logFile = tempFolder.newFile("mixed.log");
        try (FileWriter fw = new FileWriter(logFile)) {
            fw.write("garbage line\n");
            fw.write("100.000 [0 10.0.0.1:1234] \"SET\" \"key1\" \"val1\"\n");
            fw.write("another garbage\n");
            fw.write("102.000 [0 10.0.0.1:1234] \"SET\" \"key2\" \"val2\"\n");
        }

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        AtomicInteger lineCount = new AtomicInteger(0);

        double[] ts = source.readFile(logFile, new MonitorLineHandler() {
            @Override public void onLine(String line) { lineCount.incrementAndGet(); }
            @Override public void onError(Exception e) { }
        });

        assertEquals(4, lineCount.get());
        assertEquals(100.0, ts[0], 0.001);
        assertEquals(102.0, ts[1], 0.001);
    }

    @Test
    public void testListLogFilesEmptyDirectory() throws IOException {
        tempFolder.newFolder(); // ensure directory exists but is empty

        FileLineSource source = new FileLineSource(tempFolder.getRoot().getAbsolutePath());
        List<File> files = source.listLogFiles();
        assertTrue(files.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNonexistentDirectory() {
        new FileLineSource("/nonexistent/path/xyz");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorPathIsFile() throws IOException {
        File file = tempFolder.newFile("notadir.log");
        new FileLineSource(file.getAbsolutePath());
    }
}
