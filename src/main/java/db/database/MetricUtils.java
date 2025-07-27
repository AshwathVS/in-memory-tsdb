package db.database;

import lombok.experimental.UtilityClass;
import db.model.Resolution;

@UtilityClass
public class MetricUtils {
    private static final long MILLIS_PER_SECOND = 1000;
    private static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
    private static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;

    public static long normalizeTimestamp(long timestamp, Resolution resolution) {
        return switch(resolution) {
            case SECONDLY -> timestamp / MILLIS_PER_SECOND;
            case MINUTELY -> timestamp / MILLIS_PER_MINUTE;
            case HOURLY -> timestamp / MILLIS_PER_HOUR;
        };
    }
}
