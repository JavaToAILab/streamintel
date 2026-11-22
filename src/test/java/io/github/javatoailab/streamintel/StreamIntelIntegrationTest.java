package io.github.javatoailab.streamintel;

import com.sun.net.httpserver.HttpServer;
import io.github.javatoailab.streamintel.ingest.MetricPoint;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/**
 * Metrics in through Kafka, alerts out through Kafka and a webhook, with a fake LLM that writes its
 * summary from the facts it was sent.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "streamintel.llm.enabled=true"
})
@EmbeddedKafka(partitions = 3, topics = {"streamintel.metrics", "streamintel.alerts"})
@AutoConfigureMockMvc
class StreamIntelIntegrationTest {

    static final List<String> WEBHOOK = new CopyOnWriteArrayList<>();
    static final HttpServer FAKES;

    static {
        try {
            FAKES = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Pattern fact = Pattern.compile("- (\\w+) (\\w+): peak (\\d+) vs baseline (\\d+)");
        FAKES.createContext("/v1/chat/completions", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Matcher m = fact.matcher(body);
            String text = m.find()
                    ? m.group(1) + " " + m.group(2) + " is at " + m.group(3) + " against a baseline of " + m.group(4)
                    + ". Check " + m.group(1) + " dependencies first."
                    : "unknown";
            byte[] out = ("{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"" + text + "\"}}]}")
                    .getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        FAKES.createContext("/hook", ex -> {
            WEBHOOK.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        FAKES.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        String base = "http://127.0.0.1:" + FAKES.getAddress().getPort();
        r.add("streamintel.llm.base-url", () -> base);
        r.add("streamintel.webhook-url", () -> base + "/hook");
    }

    @AfterAll
    static void stop() {
        FAKES.stop(0);
    }

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired JsonMapper json;
    @Autowired MockMvc mvc;

    private void send(String service, String metric, double value, Instant at) {
        MetricPoint p = new MetricPoint(service, metric, value, at);
        kafka.send("streamintel.metrics", p.seriesKey(), json.writeValueAsString(p));
    }

    @Test
    void anIncidentIsDetectedCorrelatedAndAnnouncedInPlainEnglish() throws Exception {
        Random r = new Random(7);
        Instant t = Instant.parse("2026-11-22T14:00:00Z");
        for (int i = 0; i < 120; i++, t = t.plusSeconds(1)) {
            send("payments", "latency_p99_ms", 180 + r.nextGaussian() * 14, t);
            send("checkout", "latency_p99_ms", 220 + r.nextGaussian() * 18, t);
            send("inventory", "latency_p99_ms", 95 + r.nextGaussian() * 8, t);
            send("payments", "not-json", 0, t);
        }
        kafka.send("streamintel.metrics", "bad", "{not json");
        for (int i = 0; i < 10; i++, t = t.plusSeconds(1)) {
            send("payments", "latency_p99_ms", 1600 + r.nextGaussian() * 40, t);
            send("checkout", "latency_p99_ms", (i < 4 ? 220 : 700) + r.nextGaussian() * 18, t);
            send("inventory", "latency_p99_ms", 95 + r.nextGaussian() * 8, t);
        }
        kafka.flush();

        var props = KafkaTestUtils.consumerProps(broker, "alerts-test", true);
        props.put("auto.offset.reset", "earliest");
        List<JsonNode> alerts = new ArrayList<>();
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new org.apache.kafka.common.serialization.StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, "streamintel.alerts");
            long deadline = System.currentTimeMillis() + 30_000;
            while (alerts.size() < 2 && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> rec : KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1))) {
                    alerts.add(json.readTree(rec.value()));
                }
            }
        }

        assertThat(alerts).hasSize(2);
        JsonNode opened = alerts.get(0);
        assertThat(opened.get("change").asString()).isEqualTo("OPENED");
        assertThat(opened.get("writtenBy").asString()).isEqualTo("llm");
        // Which service opens the incident depends on which partition a consumer thread reads first.
        assertThat(opened.get("summary").asString()).matches("(payments|checkout) latency_p99_ms is at \\d+ .*");
        JsonNode escalated = alerts.get(1);
        assertThat(escalated.get("change").asString()).isEqualTo("ESCALATED");
        assertThat(escalated.get("severity").asString()).isEqualTo("CRITICAL");
        // Services are listed in event-time order: payments degraded first, checkout four seconds later.
        assertThat(escalated.get("title").asString()).contains("payments, checkout");
        assertThat(escalated.get("writtenBy").asString()).isEqualTo("llm");
        assertThat(escalated.toString()).doesNotContain("inventory");

        long deadline = System.currentTimeMillis() + 5_000;
        while (WEBHOOK.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(WEBHOOK).hasSize(2);
        assertThat(WEBHOOK.getFirst()).contains("\"text\"", "latency_p99_ms");

        String incidents = mvc.perform(get("/incidents?status=open")).andReturn().getResponse().getContentAsString();
        assertThat(json.readTree(incidents)).hasSize(1);

        String prom = mvc.perform(get("/actuator/prometheus")).andReturn().getResponse().getContentAsString();
        assertThat(prom).contains("streamintel_points_total", "streamintel_points_rejected_total", "streamintel_incidents_open 1.0",
                "streamintel_alerts_total{change=\"OPENED\",written_by=\"llm\"}");
    }
}
