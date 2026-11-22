package io.github.javatoailab.streamintel.ingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * One observation, as producers publish it to {@code streamintel.metrics}, keyed by
 * {@code service/metric} so each series stays on one partition and arrives in order.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricPoint(String service, String metric, double value, Instant timestamp) {

    public String seriesKey() {
        return service + "/" + metric;
    }
}
