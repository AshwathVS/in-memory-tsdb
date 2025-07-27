package db.database;

public interface RetentionPolicy {
    void applyRetention(MetricStore metricStore, long currentTimestampMillis);
}
