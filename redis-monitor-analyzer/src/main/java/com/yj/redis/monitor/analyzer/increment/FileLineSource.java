package com.yj.redis.monitor.analyzer.increment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class FileLineSource {

    private final File directory;

    public FileLineSource(String directoryPath) {
        Objects.requireNonNull(directoryPath, "directoryPath must not be null");
        this.directory = new File(directoryPath);
        if (!directory.exists()) {
            throw new IllegalArgumentException(
                    "Directory does not exist: " + directoryPath);
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(
                    "Path is not a directory: " + directoryPath);
        }
    }

    /**
     * Lists .log files in the directory, sorted by name.
     */
    public List<File> listLogFiles() {
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".log"));
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }
        List<File> result = Arrays.asList(files);
        result.sort(Comparator.comparing(File::getName));
        return result;
    }

    /**
     * Reads a single log file line by line, delivering each to the handler.
     * Returns [firstTimestamp, lastTimestamp]. Both values are -1 if no
     * valid timestamps were found.
     */
    public double[] readFile(File file, MonitorLineHandler handler) throws IOException {
        double firstTs = -1;
        double lastTs = -1;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    handler.onLine(line);
                } catch (Exception e) {
                    handler.onError(e);
                }

                Double ts = extractTimestamp(line);
                if (ts != null) {
                    if (firstTs < 0) {
                        firstTs = ts;
                    }
                    lastTs = ts;
                }
            }
        } catch (IOException e) {
            handler.onError(e);
            throw e;
        }

        return new double[]{firstTs, lastTs};
    }

    /**
     * Extracts the Unix timestamp from a MONITOR output line.
     * Returns null if the line doesn't start with a valid timestamp.
     */
    public static Double extractTimestamp(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        int spaceIdx = line.indexOf(' ');
        if (spaceIdx < 0) {
            return null;
        }
        try {
            return Double.parseDouble(line.substring(0, spaceIdx));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
