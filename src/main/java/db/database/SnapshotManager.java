package db.database;

import db.model.MetricLabel;
import db.model.Resolution;
import db.model.SnapshotEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import db.TSDBConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static db.database.FileUtils.OBJECT_MAPPER;

@Slf4j
@Service
public class SnapshotManager {
    private final Path snapshotPath;

    public SnapshotManager(TSDBConfiguration configuration) throws IOException {
        this.snapshotPath = Paths.get(configuration.snapshot().path());
        if (!Files.exists(snapshotPath)) {
            Files.createDirectories(snapshotPath);
        }

    }

    public void save(InMemoryTSDB tsdb) throws IOException {
        Map<String, Map<MetricLabel, MetricStore>> metrics = tsdb.getMetrics();
        Path filePath = snapshotPath.toAbsolutePath().resolve(System.currentTimeMillis() + ".snapshot");

        try (var writer = new java.io.FileWriter(filePath.toFile())) {
            for (var entry : metrics.entrySet()) {
                var metricKey = entry.getKey();

                for (var metricStoreEntry : entry.getValue().entrySet()) {
                    var tags = metricStoreEntry.getKey().getTags();
                    var metricStore = metricStoreEntry.getValue();

                    for (var resolution : Resolution.values()) {
                        var snapshotEntry = new SnapshotEntry(metricKey, tags, metricStore.getDataByResolution(resolution));
                        writer.write(OBJECT_MAPPER.writeValueAsString(snapshotEntry));
                        writer.write("\n");
                    }
                }
            }
        }
    }

    public InMemoryTSDB load(RetentionPolicy retentionPolicy, WriteAheadLog writeAheadLog) throws IOException {
        return loadBySnapshot(retentionPolicy, writeAheadLog, FileUtils.findLatestSnapshot(snapshotPath.toFile()));
    }

    public InMemoryTSDB loadBySnapshot(RetentionPolicy retentionPolicy, WriteAheadLog writeAheadLog, File file) throws IOException {
        var tsdb = new InMemoryTSDB(retentionPolicy, writeAheadLog);
        if (file == null || !file.exists()) {
            log.info("No snapshot file found!");
            return null;
        }
        try (var reader = new java.io.FileReader(file)) {
            var bufferedReader = new java.io.BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                var snapshotEntry = OBJECT_MAPPER.readValue(line, SnapshotEntry.class);
                for (var dataPoint : snapshotEntry.dataPoints().entrySet()) {
                    tsdb.putInternal(snapshotEntry.metricKey(), new MetricLabel(snapshotEntry.labels()), dataPoint.getKey(), dataPoint.getValue());
                }
            }
        }

        return tsdb;
    }
}