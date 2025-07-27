package db.model;

import java.io.Serializable;

public record WALEntry(long timestamp, String metricName, MetricLabel label, double value) implements Serializable {
}