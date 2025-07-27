package db.database;

import db.model.MetricLabel;
import db.model.WALEntry;

import java.io.IOException;
import java.util.Iterator;

public interface WriteAheadLog {
    void append(String metricName, MetricLabel label, long timestamp, double value);

    Iterator<WALEntry> readFrom(long checkpointTimestamp) throws IOException;

    void rotate() throws IOException;

    void truncateUpTo(long timestamp) throws IOException;
}
