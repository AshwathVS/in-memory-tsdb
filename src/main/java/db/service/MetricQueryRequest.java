package db.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import db.model.Resolution;

import java.util.Map;

public record MetricQueryRequest(
    @JsonProperty(required = true)
    @NotNull
    String metricName,

    boolean strictMatch,

    Map<String, String> labels,

    @JsonProperty(required = true)
    @NotNull
    long from,

    @JsonProperty(required = true)
    @NotNull
    long to,

    @NotNull
    Resolution resolution) {
}
