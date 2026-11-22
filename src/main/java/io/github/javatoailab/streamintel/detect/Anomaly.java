package io.github.javatoailab.streamintel.detect;

import java.time.Instant;

/**
 * A point that deviates from its series' recent behaviour.
 *
 * @param baseline the rolling median the point was compared with
 * @param score    robust z-score: (value - median) / (1.4826 · MAD)
 */
public record Anomaly(String service, String metric, double value, double baseline, double score, Instant at) {

    public String seriesKey() {
        return service + "/" + metric;
    }

    public double changePercent() {
        return baseline == 0 ? Double.POSITIVE_INFINITY : (value - baseline) / Math.abs(baseline) * 100;
    }
}
