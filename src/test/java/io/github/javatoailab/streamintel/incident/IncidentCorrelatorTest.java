package io.github.javatoailab.streamintel.incident;

import io.github.javatoailab.streamintel.detect.Anomaly;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class IncidentCorrelatorTest {

    private final Instant t0 = Instant.parse("2026-11-22T14:00:00Z");
    private final IncidentCorrelator c = new IncidentCorrelator(Duration.ofMinutes(2), Duration.ofMinutes(5));

    private Anomaly a(String service, String metric, double z, long secondsAfter) {
        return new Anomaly(service, metric, 1600, 180, z, t0.plusSeconds(secondsAfter));
    }

    @Test
    void relatedAnomaliesBecomeOneIncidentThatEscalatesWhenItSpreads() {
        var first = c.onAnomaly(a("payments", "latency_p99_ms", 8, 0));
        assertThat(first.incident().severity()).isEqualTo(Severity.WARNING);
        var again = c.onAnomaly(a("payments", "latency_p99_ms", 9, 10));
        var spread = c.onAnomaly(a("checkout", "latency_p99_ms", 7, 30));

        assertThat(first.change()).isEqualTo(IncidentCorrelator.Change.OPENED);
        assertThat(again.change()).isEqualTo(IncidentCorrelator.Change.UPDATED);
        assertThat(again.notifies()).isFalse();
        assertThat(spread.change()).isEqualTo(IncidentCorrelator.Change.ESCALATED);
        assertThat(spread.incident().id()).isEqualTo(first.incident().id());
        assertThat(spread.incident().severity()).isEqualTo(Severity.CRITICAL);
        assertThat(spread.incident().series()).hasSize(2);
    }

    @Test
    void anomaliesFarApartAreSeparateIncidents() {
        var one = c.onAnomaly(a("payments", "error_rate", 8, 0));
        var two = c.onAnomaly(a("inventory", "error_rate", 8, 600));
        assertThat(two.change()).isEqualTo(IncidentCorrelator.Change.OPENED);
        assertThat(two.incident().id()).isNotEqualTo(one.incident().id());
    }

    @Test
    void quietIncidentsResolve() {
        c.onAnomaly(a("payments", "error_rate", 8, 0));
        assertThat(c.resolveQuiet(t0.plus(Duration.ofMinutes(4)))).isEmpty();
        var resolved = c.resolveQuiet(t0.plus(Duration.ofMinutes(6)));
        assertThat(resolved).singleElement().satisfies(u -> {
            assertThat(u.change()).isEqualTo(IncidentCorrelator.Change.RESOLVED);
            assertThat(u.incident().status()).isEqualTo(Incident.Status.RESOLVED);
        });
        assertThat(c.openCount()).isZero();
    }

    @Test
    void theWorstPointIsKeptForEachSeries() {
        c.onAnomaly(new Anomaly("payments", "latency_p99_ms", 900, 180, 7, t0));
        var u = c.onAnomaly(new Anomaly("payments", "latency_p99_ms", 1700, 180, 15, t0.plusSeconds(5)));
        Incident.SeriesImpact s = u.incident().series().getFirst();
        assertThat(s.peakValue()).isEqualTo(1700);
        assertThat(s.anomalies()).isEqualTo(2);
        assertThat(s.changePercent()).isEqualTo(844.4);
    }
}
