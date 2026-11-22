package io.github.javatoailab.streamintel.demo;

import io.github.javatoailab.streamintel.ingest.MetricPoint;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Random;

/**
 * Demo load: three services, three metrics each, one point per series per second with realistic
 * noise. {@code POST /demo/incident} makes the payments database slow for two minutes: payments
 * latency and errors climb, and checkout, which calls payments, slows down too.
 * Enabled with {@code streamintel.demo.enabled=true}.
 */
@RestController
@ConditionalOnProperty("streamintel.demo.enabled")
public class DemoTraffic {

    private record Base(double mean, double sd) {
    }

    private static final Map<String, Map<String, Base>> SERVICES = Map.of(
            "checkout", Map.of("latency_p99_ms", new Base(220, 18), "error_rate", new Base(0.004, 0.0008), "rps", new Base(900, 35)),
            "payments", Map.of("latency_p99_ms", new Base(180, 14), "error_rate", new Base(0.002, 0.0005), "rps", new Base(400, 20)),
            "inventory", Map.of("latency_p99_ms", new Base(95, 8), "error_rate", new Base(0.001, 0.0003), "rps", new Base(650, 30)));

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final Clock clock;
    private final String topic;
    private final Random random = new Random(42);
    private volatile Instant incidentUntil = Instant.EPOCH;

    public DemoTraffic(KafkaTemplate<String, String> kafka, JsonMapper json, Clock clock,
                       io.github.javatoailab.streamintel.config.StreamIntelProperties props) {
        this.kafka = kafka;
        this.json = json;
        this.clock = clock;
        this.topic = props.topics().metrics();
    }

    @Scheduled(fixedRate = 1000)
    public void tick() {
        Instant now = clock.instant();
        boolean incident = now.isBefore(incidentUntil);
        SERVICES.forEach((service, metrics) -> metrics.forEach((metric, base) -> {
            double v = Math.max(0, base.mean() + random.nextGaussian() * base.sd());
            if (incident) {
                v *= switch (service + "/" + metric) {
                    case "payments/latency_p99_ms" -> 9.0;
                    case "payments/error_rate" -> 14.0;
                    case "checkout/latency_p99_ms" -> 3.2;
                    case "checkout/rps" -> 0.8;
                    default -> 1.0;
                };
            }
            MetricPoint p = new MetricPoint(service, metric, v, now);
            kafka.send(topic, p.seriesKey(), json.writeValueAsString(p));
        }));
    }

    @PostMapping("/demo/incident")
    public Map<String, Object> incident(@RequestParam(defaultValue = "120") int seconds) {
        incidentUntil = clock.instant().plusSeconds(seconds);
        return Map.of("incidentUntil", incidentUntil, "note", "payments database slowdown, checkout impacted");
    }
}
