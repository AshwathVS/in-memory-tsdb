package db.database;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import db.TSDBConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class TSDBScheduler implements Closeable {
    private final ScheduledExecutorService executor;

    public TSDBScheduler(
        TSDBConfiguration configuration,
        TSDBBootstrapper bootstrapper,
        InMemoryTSDB tsdb,
        ScheduledExecutorService executor,
        WriteAheadLog writeAheadLog
    ) {
        this.executor = executor;
        initScheduledJobs(configuration, bootstrapper, tsdb, executor, writeAheadLog);
    }

    private void initScheduledJobs(TSDBConfiguration configuration, TSDBBootstrapper bootstrapper, InMemoryTSDB tsdb, ScheduledExecutorService executor, WriteAheadLog writeAheadLog) {
        executor.scheduleAtFixedRate(() -> schedulePeriodicSnapshotting(bootstrapper, tsdb), configuration.snapshot().initialDelayMs(), configuration.snapshot().intervalMs(), TimeUnit.MILLISECONDS);

        executor.scheduleAtFixedRate(() -> schedulePeriodicTruncating(writeAheadLog), configuration.snapshot().initialDelayMs(), configuration.snapshot().intervalMs(), TimeUnit.MILLISECONDS);
    }

    private void schedulePeriodicTruncating(WriteAheadLog writeAheadLog) {
        long twoWeeksAgo = System.currentTimeMillis() - Duration.ofMinutes(1).toMillis();
        try {
            writeAheadLog.truncateUpTo(twoWeeksAgo);
        } catch (IOException e) {
            log.error("Truncating TSDB scheduled job failed", e);
        }
    }

    private void schedulePeriodicSnapshotting(TSDBBootstrapper bootstrapper, InMemoryTSDB tsdb) {
        try {
            bootstrapper.snapshot(tsdb);
        } catch (IOException e) {
            log.error("Periodic snapshot failed!", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.executor.shutdown();
    }
}
