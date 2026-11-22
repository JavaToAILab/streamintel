package io.github.javatoailab.streamintel.incident;

import io.github.javatoailab.streamintel.detect.Anomaly;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Groups anomalies into incidents by time: an anomaly within {@code window} of an open incident's
 * latest anomaly joins it, even from another service, because a slow database and a failing checkout
 * five seconds apart are one page, not two. An incident with no anomalies for {@code quietPeriod} is
 * resolved.
 *
 * <p>Notifications are deliberately rare: one when an incident opens, one when it escalates (a new
 * service joins or severity rises), one when it resolves. Further anomalies in an already-reported
 * series only update the record.
 */
public final class IncidentCorrelator {

    public enum Change { OPENED, ESCALATED, UPDATED, RESOLVED }

    public record Update(Incident incident, Change change) {

        public boolean notifies() {
            return change != Change.UPDATED;
        }
    }

    private final Duration window;
    private final Duration quietPeriod;
    private final Map<String, Incident> incidents = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    public IncidentCorrelator(Duration window, Duration quietPeriod) {
        this.window = window;
        this.quietPeriod = quietPeriod;
    }

    public synchronized Update onAnomaly(Anomaly a) {
        Optional<Incident> open = incidents.values().stream()
                .filter(i -> i.status() == Incident.Status.OPEN)
                .filter(i -> !a.at().isAfter(i.lastAnomalyAt().plus(window)))
                .findFirst();
        if (open.isEmpty()) {
            Incident created = new Incident("INC-" + sequence.incrementAndGet(), a);
            incidents.put(created.id(), created);
            return new Update(created, Change.OPENED);
        }
        Incident i = open.get();
        int servicesBefore = i.services().size();
        Severity severityBefore = i.severity();
        i.add(a);
        boolean escalated = i.services().size() > servicesBefore || i.severity().compareTo(severityBefore) > 0;
        return new Update(i, escalated ? Change.ESCALATED : Change.UPDATED);
    }

    /** Resolves incidents that have been quiet long enough. Call periodically. */
    public synchronized List<Update> resolveQuiet(Instant now) {
        List<Update> out = new ArrayList<>();
        for (Incident i : incidents.values()) {
            if (i.status() == Incident.Status.OPEN && i.lastAnomalyAt().plus(quietPeriod).isBefore(now)) {
                i.resolve(now);
                out.add(new Update(i, Change.RESOLVED));
            }
        }
        return out;
    }

    public Optional<Incident> find(String id) {
        return Optional.ofNullable(incidents.get(id));
    }

    public Collection<Incident> all() {
        return incidents.values();
    }

    public long openCount() {
        return incidents.values().stream().filter(i -> i.status() == Incident.Status.OPEN).count();
    }
}
