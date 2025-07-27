package db.database;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class DefaultTSDBBootstrapper implements TSDBBootstrapper {
    private final SnapshotManager snapshotManager;
    private final WriteAheadLog writeAheadLog;
    private final RetentionPolicy retentionPolicy;

    public DefaultTSDBBootstrapper(SnapshotManager snapshotManager,
                                   WriteAheadLog writeAheadLog,
                                   RetentionPolicy retentionPolicy) {
        this.snapshotManager = snapshotManager;
        this.writeAheadLog = writeAheadLog;
        this.retentionPolicy = retentionPolicy;
    }

    @Override
    public InMemoryTSDB restore() throws IOException {
        InMemoryTSDB snapshot = snapshotManager.load(retentionPolicy, writeAheadLog);
        long latestTimeStamp;

        if (snapshot == null) {
            snapshot = new InMemoryTSDB(retentionPolicy, writeAheadLog);
            latestTimeStamp = Long.MIN_VALUE;
        } else {
            latestTimeStamp = snapshot.getLatestTimestamp();
        }

        var iterator = writeAheadLog.readFrom(latestTimeStamp);

        int replayed = 0;
        while (iterator.hasNext()) {
            var next = iterator.next();
            snapshot.putInternal(next.metricName(), next.label(), next.timestamp(), next.value());
            replayed++;
        }

        log.info("Bootstrapped TSDB with snapshot and replayed {} WAL entries", replayed);
        return snapshot;

    }

    @Override
    public void snapshot(InMemoryTSDB tsdb) throws IOException {
        snapshotManager.save(tsdb);
    }
}
