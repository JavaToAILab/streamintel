package io.github.javatoailab.streamintel.incident;

import io.github.javatoailab.streamintel.detect.Anomaly;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Anomalies that happened close together, treated as one problem. Each affected series keeps its
 * first sighting, its worst point and the baseline it was measured against: the facts an alert is
 * written from.
 */
public final class Incident {

    public enum Status { OPEN, RESOLVED }

    public record SeriesImpact(String service, String metric, Instant firstSeen, double peakValue, double baseline,
                               double peakScore, int anomalies) {

        public double changePercent() {
            return baseline == 0 ? 0 : Math.round((peakValue - baseline) / Math.abs(baseline) * 1000) / 10.0;
        }
    }

    private final String id;
    private final Instant openedAt;
    private Instant lastAnomalyAt;
    private Instant resolvedAt;
    private Status status = Status.OPEN;
    private final Map<String, SeriesImpact> series = new LinkedHashMap<>();

    Incident(String id, Anomaly first) {
        this.id = id;
        this.openedAt = first.at();
        this.lastAnomalyAt = first.at();
        add(first);
    }

    synchronized void add(Anomaly a) {
        if (a.at().isAfter(lastAnomalyAt)) {
            lastAnomalyAt = a.at();
        }
        series.merge(a.seriesKey(),
                new SeriesImpact(a.service(), a.metric(), a.at(), a.value(), a.baseline(), a.score(), 1),
                (old, now) -> Math.abs(now.peakScore()) > Math.abs(old.peakScore())
                        ? new SeriesImpact(old.service(), old.metric(), old.firstSeen(), now.peakValue(), now.baseline(),
                        now.peakScore(), old.anomalies() + 1)
                        : new SeriesImpact(old.service(), old.metric(), old.firstSeen(), old.peakValue(), old.baseline(),
                        old.peakScore(), old.anomalies() + 1));
    }

    synchronized void resolve(Instant at) {
        status = Status.RESOLVED;
        resolvedAt = at;
    }

    /**
     * CRITICAL when more than one service is affected (it is spreading) or a series is extremely far
     * from normal; WARNING otherwise.
     */
    public synchronized Severity severity() {
        boolean extreme = series.values().stream().anyMatch(s -> Math.abs(s.peakScore()) >= 10);
        return services().size() > 1 || extreme ? Severity.CRITICAL : Severity.WARNING;
    }

    /**
     * Affected services in the order their first anomaly happened (event time, not arrival time:
     * partitions are consumed in parallel). The first service is usually closest to the cause.
     */
    public synchronized Set<String> services() {
        Set<String> out = new LinkedHashSet<>();
        series.values().stream().sorted(java.util.Comparator.comparing(SeriesImpact::firstSeen))
                .forEach(s -> out.add(s.service()));
        return out;
    }

    public String id() { return id; }
    public Instant openedAt() { return openedAt; }
    public synchronized Instant lastAnomalyAt() { return lastAnomalyAt; }
    public synchronized Instant resolvedAt() { return resolvedAt; }
    public synchronized Status status() { return status; }
    public synchronized List<SeriesImpact> series() { return new ArrayList<>(series.values()); }
}
