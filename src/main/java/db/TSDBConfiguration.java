package db;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "configuration")
public record TSDBConfiguration(SnapshotProperties snapshot, WALProperties walProperties, long retentionMs) {
    public record SnapshotProperties(String path, long initialDelayMs, long intervalMs) {
    }

    public record WALProperties(String path, long rotationByteSize) {
    }
}
