package io.github.javatoailab.streamintel.api;

import io.github.javatoailab.streamintel.alert.Alert;
import io.github.javatoailab.streamintel.alert.AlertDispatcher;
import io.github.javatoailab.streamintel.incident.Incident;
import io.github.javatoailab.streamintel.incident.IncidentCorrelator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

@RestController
public class IncidentController {

    public record IncidentView(String id, String status, String severity, List<String> services, Instant openedAt,
                               Instant lastAnomalyAt, Instant resolvedAt, List<Incident.SeriesImpact> series) {

        static IncidentView of(Incident i) {
            return new IncidentView(i.id(), i.status().name(), i.severity().name(), List.copyOf(i.services()),
                    i.openedAt(), i.lastAnomalyAt(), i.resolvedAt(), i.series());
        }
    }

    private final IncidentCorrelator correlator;
    private final AlertDispatcher alerts;

    public IncidentController(IncidentCorrelator correlator, AlertDispatcher alerts) {
        this.correlator = correlator;
        this.alerts = alerts;
    }

    @GetMapping("/incidents")
    public List<IncidentView> incidents(@RequestParam(required = false) String status) {
        return correlator.all().stream()
                .filter(i -> status == null || i.status().name().equalsIgnoreCase(status))
                .sorted(Comparator.comparing(Incident::openedAt).reversed())
                .map(IncidentView::of).toList();
    }

    @GetMapping("/incidents/{id}")
    public ResponseEntity<IncidentView> incident(@PathVariable String id) {
        return ResponseEntity.of(correlator.find(id).map(IncidentView::of));
    }

    @GetMapping("/alerts")
    public List<Alert> alerts() {
        return alerts.recent();
    }
}
