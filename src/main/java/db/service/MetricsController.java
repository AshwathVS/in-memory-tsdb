package db.service;

import db.database.InMemoryTSDB;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import db.model.DataPoint;
import db.model.MetricLabel;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/metrics")
@RequiredArgsConstructor
public class MetricsController {
    private final InMemoryTSDB inMemoryTSDB;

    @PostMapping("/put")
    public Mono<String> putMetrics(@Valid @RequestBody List<MetricPutRequest> requests) {
        try {
            requests
                .parallelStream()
                .forEach(request -> inMemoryTSDB.put(request.metricName(), request.labels(), request.timestamp(), request.value()));
            return Mono.just("ok");
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @PostMapping("/query")
    public Mono<Map<MetricLabel, List<DataPoint>>> query(@RequestBody MetricQueryRequest query) {
        if (query.from() >= query.to()) {
            return Mono.error(new IllegalArgumentException("'from' timestamp should be less than 'to' timestamp"));
        }

        if (query.strictMatch()) {
            var label = query.labels() == null ? new MetricLabel(Collections.emptyMap()) : new MetricLabel(query.labels());
            return Mono.just(Map.of(label, inMemoryTSDB.query(query.metricName(), label, query.from(), query.to(), query.resolution())));
        } else {
            return Mono.just(
                inMemoryTSDB.query(query.metricName(), query.labels(), query.from(), query.to(), query.resolution())
            );
        }
    }
}
