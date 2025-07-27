package db.database;

import lombok.Getter;
import db.model.DataPoint;
import db.model.Resolution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static db.database.MetricUtils.normalizeTimestamp;

public class MetricStore {
    private final Map<Resolution, ConcurrentSkipListMap<Long, Double>> metricStoreMap;
    private final ConcurrentSkipListMap<Long, Double> secondlyStore;
    private final ConcurrentSkipListMap<Long, Double> minutelyStore;
    private final ConcurrentSkipListMap<Long, Double> hourlyStore;

    @Getter
    private long latestTimestamp;

    public MetricStore() {
        this.secondlyStore = new ConcurrentSkipListMap<>();
        this.minutelyStore = new ConcurrentSkipListMap<>();
        this.hourlyStore = new ConcurrentSkipListMap<>();

        metricStoreMap = Map.of(
            Resolution.SECONDLY, secondlyStore,
            Resolution.MINUTELY, minutelyStore,
            Resolution.HOURLY, hourlyStore
        );
    }

    void addDataPoint(long timestamp, double value) {
        long secKey = normalizeTimestamp(timestamp, Resolution.SECONDLY);
        secondlyStore.merge(secKey, value, Double::sum);

        long minKey = normalizeTimestamp(timestamp, Resolution.MINUTELY);
        minutelyStore.merge(minKey, value, Double::sum);

        long hourKey = normalizeTimestamp(timestamp, Resolution.HOURLY);
        hourlyStore.merge(hourKey, value, Double::sum);

        latestTimestamp = Math.max(latestTimestamp, timestamp);
    }

    List<DataPoint> query(long startTime, long endTime, Resolution resolution) {
        var mapToUse = switch (resolution) {
            case SECONDLY -> secondlyStore;
            case MINUTELY -> minutelyStore;
            case HOURLY -> hourlyStore;
        };

        long normalizedStart = switch (resolution) {
            case SECONDLY -> normalizeTimestamp(startTime, Resolution.SECONDLY);
            case MINUTELY -> normalizeTimestamp(startTime, Resolution.MINUTELY);
            case HOURLY -> normalizeTimestamp(startTime, Resolution.HOURLY);
        };

        long normalizedEnd = switch (resolution) {
            case SECONDLY -> normalizeTimestamp(endTime, Resolution.SECONDLY);
            case MINUTELY -> normalizeTimestamp(endTime, Resolution.MINUTELY);
            case HOURLY -> normalizeTimestamp(endTime, Resolution.HOURLY);
        };


        return mapToUse
            .subMap(normalizedStart, true, normalizedEnd, true)
            .entrySet()
            .stream()
            .map(entry -> new DataPoint(entry.getKey(), entry.getValue()))
            .toList();
    }

    public double average(long startTime, long endTime, Resolution resolution) {
        return query(startTime, endTime, resolution)
            .stream()
            .mapToDouble(DataPoint::value)
            .average()
            .orElse(0.0);
    }

    public double sum(long startTime, long endTime, Resolution resolution) {
        return query(startTime, endTime, resolution)
            .stream()
            .mapToDouble(DataPoint::value)
            .sum();
    }

    public double min(long startTime, long endTime, Resolution resolution) {
        return query(startTime, endTime, resolution)
            .stream()
            .mapToDouble(DataPoint::value)
            .min()
            .orElse(0.0);
    }

    public double max(long startTime, long endTime, Resolution resolution) {
        return query(startTime, endTime, resolution)
            .stream()
            .mapToDouble(DataPoint::value)
            .max()
            .orElse(0.0);
    }

    public Map<Resolution, ConcurrentSkipListMap<Long, Double>> getMetricsForRetention() {
        return metricStoreMap;
    }

    public Map<Long, Double> getDataByResolution(Resolution resolution) {
        return new HashMap<>(metricStoreMap.get(resolution));
    }
}