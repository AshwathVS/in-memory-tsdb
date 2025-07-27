package db.model;

import java.util.Map;

public record SnapshotEntry(String metricKey, Map<String, String> labels, Map<Long, Double> dataPoints) {
}
