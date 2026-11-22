package io.github.javatoailab.streamintel.alert;

import io.github.javatoailab.streamintel.incident.IncidentCorrelator;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Writes and delivers alerts off the consumer thread, on virtual threads, so a slow model never holds
 * up metric processing. Alerts for the same incident are chained, so OPENED is always delivered
 * before ESCALATED and RESOLVED; different incidents still proceed in parallel. Delivery goes to the
 * alerts topic and, if configured, a Slack-compatible webhook.
 */
public final class AlertDispatcher implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(AlertDispatcher.class.getName());

    private final AlertWriter writer;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private final URI webhook;
    private final JsonMapper json;
    private final MeterRegistry meters;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final Deque<Alert> recent = new ArrayDeque<>();
    private final Map<String, java.util.concurrent.CompletableFuture<Void>> chains = new java.util.concurrent.ConcurrentHashMap<>();

    public AlertDispatcher(AlertWriter writer, KafkaTemplate<String, String> kafka, String topic, URI webhook,
                           JsonMapper json, MeterRegistry meters) {
        this.writer = writer;
        this.kafka = kafka;
        this.topic = topic;
        this.webhook = webhook;
        this.json = json;
        this.meters = meters;
    }

    public void dispatch(IncidentCorrelator.Update update) {
        chains.compute(update.incident().id(), (id, previous) ->
                (previous == null ? java.util.concurrent.CompletableFuture.<Void>completedFuture(null) : previous)
                        .thenRunAsync(() -> deliver(update), executor));
    }

    private void deliver(IncidentCorrelator.Update update) {
        {
            try {
                Alert alert = writer.write(update.incident(), update.change());
                synchronized (recent) {
                    recent.addFirst(alert);
                    while (recent.size() > 200) {
                        recent.removeLast();
                    }
                }
                meters.counter("streamintel.alerts", "change", alert.change(), "written_by", alert.writtenBy()).increment();
                kafka.send(topic, alert.incidentId(), json.writeValueAsString(alert));
                if (webhook != null) {
                    String text = alert.title() + "\n" + alert.summary() + "\n• " + String.join("\n• ", alert.facts());
                    http.send(HttpRequest.newBuilder(webhook).timeout(Duration.ofSeconds(5))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(Map.of("text", text))))
                            .build(), HttpResponse.BodyHandlers.discarding());
                }
            } catch (Exception e) {
                meters.counter("streamintel.alert_failures").increment();
                LOG.log(System.Logger.Level.ERROR, "alert delivery failed for " + update.incident().id(), e);
            } finally {
                if (update.change() == IncidentCorrelator.Change.RESOLVED) {
                    chains.remove(update.incident().id()); // a resolved incident never alerts again
                }
            }
        }
    }

    public List<Alert> recent() {
        synchronized (recent) {
            return List.copyOf(recent);
        }
    }

    @Override
    public void close() {
        executor.close();
    }
}
