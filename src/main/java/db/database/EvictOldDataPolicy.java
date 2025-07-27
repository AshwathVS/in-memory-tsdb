package db.database;

import db.model.Resolution;
import org.springframework.stereotype.Service;
import db.TSDBConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

@Service
public class EvictOldDataPolicy implements RetentionPolicy {
    private final long retentionMillis;

    public EvictOldDataPolicy(TSDBConfiguration configuration) {
        this.retentionMillis = configuration.retentionMs();
    }

    public void applyRetention(MetricStore metricStore, long currentTimestampMillis) {
        metricStore
            .getMetricsForRetention()
            .entrySet()
            .parallelStream()
            .forEach(entry -> applyRetention(entry, currentTimestampMillis));
    }

    void applyRetention(Map.Entry<Resolution, ConcurrentSkipListMap<Long, Double>> metricStoreEntry, long currentTimestampMillis) {
        long cutoffTime = currentTimestampMillis - retentionMillis;
        long normalizedCutoff = MetricUtils.normalizeTimestamp(cutoffTime, metricStoreEntry.getKey());

        if (normalizedCutoff > 0) {
            metricStoreEntry.getValue().headMap(normalizedCutoff).clear();
        }
    }
}