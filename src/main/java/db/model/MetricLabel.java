package db.model;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import static db.database.FileUtils.OBJECT_MAPPER;

public class MetricLabel implements Serializable {
    private final TreeMap<String, String> tags;

    public MetricLabel(Map<String, String> tags) {
        this.tags = new TreeMap<>(tags);
    }

    public boolean containsAll(Map<String, String> tags) {
        return this.tags.entrySet().containsAll(tags.entrySet());
    }

    public Map<String, String> getTags() {
        return Map.copyOf(tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricLabel metricKey = (MetricLabel) o;
        return tags.equals(metricKey.tags);
    }

    @Override
    public int hashCode() {
        return tags.hashCode();
    }

    @Override
    public String toString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(tags);
        } catch (JsonProcessingException e) {
            // DO NOTHING
        }
        return "";
    }
}
