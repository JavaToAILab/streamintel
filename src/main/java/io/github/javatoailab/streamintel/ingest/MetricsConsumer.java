package io.github.javatoailab.streamintel.ingest;

import io.github.javatoailab.streamintel.alert.AlertDispatcher;
import io.github.javatoailab.streamintel.detect.Anomaly;
import io.github.javatoailab.streamintel.detect.DetectorRegistry;
import io.github.javatoailab.streamintel.incident.IncidentCorrelator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.kafka.annotation.KafkaListener;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Batch consumer for the metrics topic. Each point goes through its series detector; confirmed
 * anomalies go to the correlator; opens, escalations and resolutions go to the alert dispatcher.
 * Bad records are counted and skipped rather than blocking the partition.
 */
public class MetricsConsumer {

    private final DetectorRegistry detectors;
    private final IncidentCorrelator correlator;
    private final AlertDispatcher alerts;
    private final JsonMapper json;
    private final Counter points;
    private final Counter rejected;
    private final Counter anomalies;
    private final Timer batchTimer;
    private final Set<String> services = ConcurrentHashMap.newKeySet();

    public MetricsConsumer(DetectorRegistry detectors, IncidentCorrelator correlator, AlertDispatcher alerts,
                           JsonMapper json, MeterRegistry meters) {
        this.detectors = detectors;
        this.correlator = correlator;
        this.alerts = alerts;
        this.json = json;
        this.points = meters.counter("streamintel.points");
        this.rejected = meters.counter("streamintel.points_rejected");
        this.anomalies = meters.counter("streamintel.anomalies");
        this.batchTimer = meters.timer("streamintel.batch");
        meters.gauge("streamintel.incidents_open", correlator, c -> c.openCount());
        meters.gauge("streamintel.series", detectors, DetectorRegistry::seriesCount);
    }

    @KafkaListener(topics = "${streamintel.topics.metrics:streamintel.metrics}", groupId = "streamintel",
            batch = "true", concurrency = "${streamintel.consumer-concurrency:3}")
    public void onBatch(List<String> records) {
        batchTimer.record(() -> records.forEach(this::process));
    }

    void process(String raw) {
        MetricPoint p;
        try {
            p = json.readValue(raw, MetricPoint.class);
        } catch (JacksonException e) {
            rejected.increment();
            return;
        }
        if (p.service() == null || p.metric() == null || p.timestamp() == null || !Double.isFinite(p.value())) {
            rejected.increment();
            return;
        }
        points.increment();
        services.add(p.service());
        Optional<Anomaly> a = detectors.observe(p);
        if (a.isPresent()) {
            anomalies.increment();
            IncidentCorrelator.Update u = correlator.onAnomaly(a.get());
            if (u.notifies()) {
                alerts.dispatch(u);
            }
        }
    }

    public Set<String> knownServices() {
        return Set.copyOf(services);
    }
}
