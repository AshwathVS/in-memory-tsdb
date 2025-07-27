package db.service;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

import java.util.Map;

public record MetricPutRequest(
    @NotBlank
    String metricName,

    Map<String, String> labels,

    @Positive
    long timestamp,

    double value) {
}
