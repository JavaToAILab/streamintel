package io.github.javatoailab.streamintel.alert;

import io.github.javatoailab.streamintel.detect.Anomaly;
import io.github.javatoailab.streamintel.incident.IncidentCorrelator;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AlertWriterTest {

    private final Instant t0 = Instant.parse("2026-11-22T14:00:00Z");

    private IncidentCorrelator.Update incident() {
        IncidentCorrelator c = new IncidentCorrelator(Duration.ofMinutes(2), Duration.ofMinutes(5));
        c.onAnomaly(new Anomaly("payments", "latency_p99_ms", 1620, 180, 41.2, t0));
        return c.onAnomaly(new Anomaly("checkout", "latency_p99_ms", 704, 220, 9.8, t0.plusSeconds(20)));
    }

    private Alert write(LlmClient llm) {
        IncidentCorrelator.Update u = incident();
        return new AlertWriter(llm, () -> Set.of("payments", "checkout", "inventory"), Clock.systemUTC())
                .write(u.incident(), u.change());
    }

    @Test
    void aGroundedSummaryIsUsed() {
        Alert a = write((s, u) -> "Payments p99 latency is at 1620 ms against a 180 ms baseline, and checkout, "
                + "which calls payments, rose to 704 ms. Check the payments database and its connection pool first.");
        assertThat(a.writtenBy()).isEqualTo("llm");
        assertThat(a.summary()).contains("1620");
        assertThat(a.severity()).isEqualTo("CRITICAL");
        assertThat(a.facts()).hasSize(2).first().asString().startsWith("payments latency_p99_ms");
    }

    @Test
    void anInventedNumberFallsBackToTheTemplate() {
        Alert a = write((s, u) -> "Payments latency hit 2400 ms; checkout is slow too.");
        assertThat(a.writtenBy()).isEqualTo("template");
        assertThat(a.summary()).startsWith("Detected 2 anomalous series across 2 service(s).");
    }

    @Test
    void namingAnUnaffectedServiceFallsBackToTheTemplate() {
        Alert a = write((s, u) -> "Inventory is overloaded, slowing payments and checkout.");
        assertThat(a.writtenBy()).isEqualTo("template");
    }

    @Test
    void aFailingOrMissingModelNeverBlocksTheAlert() {
        assertThat(write((s, u) -> { throw new IllegalStateException("timeout"); }).writtenBy()).isEqualTo("template");
        assertThat(write(null).writtenBy()).isEqualTo("template");
    }

    @Test
    void numbersAreParsedWithSeparatorsAndSigns() {
        assertThat(AlertWriter.numbers("rose 1,620 ms (+800.0%) z=41.2")).containsExactly(1620.0, 800.0, 41.2);
    }
}
