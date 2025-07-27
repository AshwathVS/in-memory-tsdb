package db.database;

import db.model.DataPoint;
import db.model.MetricLabel;
import db.model.Resolution;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class InMemoryTSDB {
    private final ConcurrentHashMap<String, Map<MetricLabel, MetricStore>> metrics;
    private long latestTimestamp;
    private final RetentionPolicy retentionPolicy;
    private final WriteAheadLog writeAheadLog;

    public InMemoryTSDB(RetentionPolicy retentionPolicy, WriteAheadLog writeAheadLog) {
        this.metrics = new ConcurrentHashMap<>();
        this.retentionPolicy = retentionPolicy;
        this.writeAheadLog = writeAheadLog;
    }

    private Map<MetricLabel, MetricStore> getMetricStoresByMetricKey(String metricKey) {
        return metrics.getOrDefault(metricKey, Collections.emptyMap());
    }

    private MetricStore getStore(String metricKey, MetricLabel metricLabel) {
        var metricStore = getMetricStoresByMetricKey(metricKey);
        return metricStore.get(metricLabel);
    }

    // Core internal method that inserts data point without WAL or retention logic
    public void putInternal(String metricKey, MetricLabel metricLabel, long timestamp, double value) {
        var labelMap = metrics.computeIfAbsent(metricKey, k -> new ConcurrentHashMap<>());
        var metricStore = labelMap.computeIfAbsent(metricLabel, k -> new MetricStore());

        metricStore.addDataPoint(timestamp, value);
        latestTimestamp = Math.max(latestTimestamp, metricStore.getLatestTimestamp());
    }

    // Public write API - used in normal runtime, includes WAL and retention
    public void put(String metricKey, MetricLabel metricLabel, long timestamp, double value) {
        putInternal(metricKey, metricLabel, timestamp, value);

        writeAheadLog.append(metricKey, metricLabel, timestamp, value);
        retentionPolicy.applyRetention(metrics.get(metricKey).get(metricLabel), System.currentTimeMillis());
    }

    public void put(String metricKey, Map<String, String> labels, long timestamp, double value) {
        MetricLabel metricLabel = (labels == null) ? new MetricLabel(Collections.emptyMap()) : new MetricLabel(labels);
        put(metricKey, metricLabel, timestamp, value);
    }

    public List<DataPoint> query(String metricKey, MetricLabel metricLabel, long startTime, long endTime, Resolution resolution) {
        MetricStore store = getStore(metricKey, metricLabel);
        return store != null ? store.query(startTime, endTime, resolution) : Collections.emptyList();
    }

    public Map<MetricLabel, List<DataPoint>> query(String metricKey, Map<String, String> labels, long startTime, long endTime, Resolution resolution) {
        Map<MetricLabel, MetricStore> labelMetricStoreMap = getMetricStoresByMetricKey(metricKey);
        Map<MetricLabel, List<DataPoint>> response = new HashMap<>();

        labelMetricStoreMap.entrySet().parallelStream().forEach(entry -> {
            MetricLabel label = entry.getKey();
            if (label.containsAll(labels)) {
                MetricStore store = entry.getValue();
                response.put(label, store.query(startTime, endTime, resolution));
            }
        });

        return response;
    }

    // Aggregation helpers (sum, avg, min, max)
    public double sum(String metricKey, MetricLabel metricLabel, long startTime, long endTime, Resolution resolution) {
        MetricStore store = getStore(metricKey, metricLabel);
        return store != null ? store.sum(startTime, endTime, resolution) : 0.0;
    }

    public double average(String metricKey, MetricLabel metricLabel, long startTime, long endTime, Resolution resolution) {
        MetricStore store = getStore(metricKey, metricLabel);
        return store != null ? store.average(startTime, endTime, resolution) : 0.0;
    }

    public double max(String metricKey, MetricLabel metricLabel, long startTime, long endTime, Resolution resolution) {
        MetricStore store = getStore(metricKey, metricLabel);
        return store != null ? store.max(startTime, endTime, resolution) : 0.0;
    }

    public double min(String metricKey, MetricLabel metricLabel, long startTime, long endTime, Resolution resolution) {
        MetricStore store = getStore(metricKey, metricLabel);
        return store != null ? store.min(startTime, endTime, resolution) : 0.0;
    }

    Map<String, Map<MetricLabel, MetricStore>> getMetrics() {
        return metrics
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                e -> Map.copyOf(e.getValue())
            ));
    }

    public long getLatestTimestamp() {
        return Math.min(latestTimestamp, System.currentTimeMillis());
    }
}
