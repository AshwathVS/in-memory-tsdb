package db.database;

import java.io.IOException;

public interface TSDBBootstrapper {
    /**
     * Restores the TSDB from the latest snapshot and applies any WAL entries beyond it.
     *
     * @return a fully restored InMemoryTSDB
     * @throws IOException if snapshot or WAL loading fails
     */
    InMemoryTSDB restore() throws IOException;

    /**
     * Saves the current state of the TSDB as a snapshot.
     *
     * @param tsdb the TSDB to snapshot
     * @throws IOException if snapshot writing fails
     */
    void snapshot(InMemoryTSDB tsdb) throws IOException;
}
