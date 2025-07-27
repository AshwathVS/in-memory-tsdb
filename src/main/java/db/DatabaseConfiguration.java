package db;

import db.database.InMemoryTSDB;
import db.database.RetentionPolicy;
import db.database.TSDBBootstrapper;
import db.database.WriteAheadLog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Configuration
public class DatabaseConfiguration {
    @Bean
    public ScheduledExecutorService scheduledExecutorService(@Value("${core-pool-size}") int corePoolSize) {
        return new ScheduledThreadPoolExecutor(corePoolSize);
    }

    @Bean
    public InMemoryTSDB inMemoryTSDB(TSDBBootstrapper tsdbBootstrapper) throws IOException {
        return tsdbBootstrapper.restore();
    }
}
