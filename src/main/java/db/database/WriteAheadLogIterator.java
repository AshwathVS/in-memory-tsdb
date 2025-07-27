package db.database;

import com.fasterxml.jackson.core.type.TypeReference;
import db.model.MetricLabel;
import db.model.WALEntry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static db.database.FileUtils.OBJECT_MAPPER;
import static db.database.WriteAheadLogImpl.SEPARATOR;

public class WriteAheadLogIterator implements Iterator<WALEntry> {
    private final File[] files;
    private int currentIndex = 0;
    private BufferedReader reader;
    private String lineToParse;

    public WriteAheadLogIterator(File[] files, long checkpointTimestamp) {
        this.files = files;
        Arrays.sort(files, Comparator.comparing(File::getName));

        // Find the first file whose last timestamp >= checkpointTimestamp
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            try {
                String lastLine = FileUtils.readLastLine(file);
                if (lastLine == null) continue;

                long ts = Long.parseLong(lastLine.split(SEPARATOR, 4)[0]);
                if (ts >= checkpointTimestamp) {
                    currentIndex = i;
                    break;
                }
            } catch (IOException ignored) {
                // ignored!
            }
        }

        // Initialize reader if a valid file was found
        if (currentIndex < files.length) {
            try {
                reader = new BufferedReader(new FileReader(files[currentIndex]));
            } catch (FileNotFoundException e) {
                throw new UncheckedIOException("File not found during WAL initialization", e);
            }
        }
    }

    @Override
    public boolean hasNext() {
        try {
            while (reader != null && (lineToParse = reader.readLine()) == null) {
                currentIndex++;
                if (currentIndex >= files.length) {
                    reader.close();
                    reader = null;
                    return false;
                }
                reader.close(); // close previous reader
                reader = new BufferedReader(new FileReader(files[currentIndex]));
            }
            return lineToParse != null;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public WALEntry next() {
        if (lineToParse == null && !hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            String[] parts = lineToParse.split(SEPARATOR, 4);
            if (parts.length < 4) throw new IOException("Invalid WAL line: " + lineToParse);

            long timestamp = Long.parseLong(parts[0]);
            String metricName = parts[1];
            Map<String, String> map = OBJECT_MAPPER.readValue(parts[2], new TypeReference<>() {});
            MetricLabel label = new MetricLabel(map);
            double value = Double.parseDouble(parts[3]);

            lineToParse = null; // reset for next iteration
            return new WALEntry(timestamp, metricName, label, value);
        } catch (IOException e) {
            throw new NoSuchElementException("Failed to parse WAL line: " + e.getMessage());
        }
    }
}

