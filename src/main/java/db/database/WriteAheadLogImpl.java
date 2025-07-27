package db.database;

import lombok.extern.slf4j.Slf4j;
import db.model.MetricLabel;
import db.model.WALEntry;
import org.springframework.stereotype.Service;
import db.TSDBConfiguration;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class WriteAheadLogImpl implements WriteAheadLog, Closeable {
    private static final String LOG_FILE_FORMAT = "%s/metrics_%d.log";
    public static final String SEPARATOR = ";;";

    private final TSDBConfiguration.WALProperties walProperties;
    private final BlockingQueue<String> writeQueue = new LinkedBlockingQueue<>();

    private Thread writerThread;
    private volatile boolean running = true;
    private BufferedWriter logWriter;
    private final AtomicInteger logFileCount;
    private final AtomicLong bytesWritten;

    public WriteAheadLogImpl(TSDBConfiguration configuration) {
        this.walProperties = configuration.walProperties();

        if (new File(walProperties.path()).mkdirs()) {
            log.info("WAL log directory initialised");
        }
        this.logFileCount = new AtomicInteger(FileUtils.findLatestLogFile(walProperties.path()));
        if (logFileCount.get() <= 0) logFileCount.incrementAndGet();

        this.bytesWritten = new AtomicLong(0);

        startWriterThread();
        initWriter();
    }

    private void initWriter() {
        try {
            var currentLogFile = String.format(LOG_FILE_FORMAT, walProperties.path(), logFileCount.get());
            if (FileUtils.fileExists(currentLogFile)) {
                this.logWriter = new BufferedWriter(new FileWriter(currentLogFile, true));
                this.bytesWritten.set(FileUtils.bytesWrittenSoFar(currentLogFile));
            } else {
                this.logWriter = new BufferedWriter(new FileWriter(currentLogFile, true));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize WAL writer", e);
        }
    }

    private void startWriterThread() {
        writerThread = new Thread(() -> {
            while (running) {
                try {
                    processQueue();
                } catch (Exception e) {
                    // Log error and restart unless shutting down
                    if (running) {
                        log.error("WAL writer thread crashed", e);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {

                        }
                        log.info("Restarting WAL writer thread...");
                    } else {
                        // If shutting down, exit cleanly
                        break;
                    }
                }
            }
        }, "WAL-Writer");
        writerThread.start();
    }

    private void processQueue() throws InterruptedException, IOException {
        while (running || !writeQueue.isEmpty()) {
            String entry = writeQueue.poll(100, TimeUnit.MILLISECONDS);
            if (entry != null) {
                logWriter.write(entry);

                if (bytesWritten.addAndGet(entry.getBytes(StandardCharsets.UTF_8).length) > walProperties.rotationByteSize()) {
                    rotate();
                }

                logWriter.flush();
            }
        }
    }

    @Override
    public synchronized void append(String metricName, MetricLabel label, long timestamp, double value) {
        if (running) {
            String entry = timestamp + SEPARATOR + metricName + SEPARATOR + label + SEPARATOR + value + "\n";
            writeQueue.add(entry);
        }
    }

    @Override
    public Iterator<WALEntry> readFrom(long checkpointTimestamp) {
        File[] files = new File(walProperties.path()).listFiles((dir, name) -> name.endsWith(".log"));
        return new WriteAheadLogIterator(files, checkpointTimestamp);
    }

    @Override
    public synchronized void rotate() throws IOException {
        if (bytesWritten.get() > walProperties.rotationByteSize()) {
            bytesWritten.set(0);
            logWriter.flush();
            logWriter.close();
            logFileCount.incrementAndGet();
            initWriter();
        }
    }

    @Override
    public synchronized void truncateUpTo(long timestamp) {
        File dir = new File(walProperties.path());
        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (files == null) return;

        for (File file : files) {
            try {
                String lastLine = FileUtils.readLastLine(file);
                if (lastLine == null) break;
                else {
                    long ts = Long.parseLong(lastLine.split(SEPARATOR)[0]);
                    if (ts <= timestamp) {
                        Files.delete(file.toPath());
                        log.info("Deleted log file {}", file.getAbsolutePath());
                    }
                }
            } catch (IOException ex) {
                // DO NOTHING
            }
        }
    }

    @Override
    public void close() throws IOException {
        running = false;

        // Then, wait for the thread to process remaining entries
        if (writerThread != null) {
            try {
                writerThread.join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt(); // restore interrupt status
            }
        }

        logWriter.flush();
        logWriter.close();
    }
}
