package io.github.javatoailab.streamintel.config;

import io.github.javatoailab.streamintel.alert.AlertDispatcher;
import io.github.javatoailab.streamintel.alert.AlertWriter;
import io.github.javatoailab.streamintel.alert.OpenAiLlmClient;
import io.github.javatoailab.streamintel.detect.DetectorRegistry;
import io.github.javatoailab.streamintel.detect.Direction;
import io.github.javatoailab.streamintel.detect.SeriesDetector;
import io.github.javatoailab.streamintel.incident.IncidentCorrelator;
import io.github.javatoailab.streamintel.ingest.MetricsConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

@Configuration
@EnableScheduling
public class StreamIntelConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    DetectorRegistry detectorRegistry(StreamIntelProperties props) {
        StreamIntelProperties.Detection d = props.detection();
        return new DetectorRegistry(metric -> new SeriesDetector(d.window(), d.minSamples(), d.threshold(),
                props.directions().getOrDefault(metric, Direction.BOTH), d.minRelativeChange(), d.confirm(), d.of()));
    }

    @Bean
    IncidentCorrelator incidentCorrelator(StreamIntelProperties props) {
        return new IncidentCorrelator(props.correlation().window(), props.correlation().quietPeriod());
    }

    @Bean
    AlertWriter alertWriter(StreamIntelProperties props, JsonMapper json, @Lazy MetricsConsumer consumer, Clock clock) {
        StreamIntelProperties.Llm l = props.llm();
        OpenAiLlmClient llm = l.enabled() ? new OpenAiLlmClient(l.baseUrl(), l.model(), l.apiKey(), l.timeout(), json) : null;
        return new AlertWriter(llm, consumer::knownServices, clock);
    }

    @Bean(destroyMethod = "close")
    AlertDispatcher alertDispatcher(AlertWriter writer, KafkaTemplate<String, String> kafka, StreamIntelProperties props,
                                    JsonMapper json, MeterRegistry meters) {
        java.net.URI hook = props.webhookUrl() == null || props.webhookUrl().toString().isBlank() ? null : props.webhookUrl();
        return new AlertDispatcher(writer, kafka, props.topics().alerts(), hook, json, meters);
    }

    @Bean
    MetricsConsumer metricsConsumer(DetectorRegistry detectors, IncidentCorrelator correlator, AlertDispatcher alerts,
                                    JsonMapper json, MeterRegistry meters) {
        return new MetricsConsumer(detectors, correlator, alerts, json, meters);
    }

    @Bean
    NewTopic metricsTopic(StreamIntelProperties props) {
        return TopicBuilder.name(props.topics().metrics()).partitions(6).replicas(1).build();
    }

    @Bean
    NewTopic alertsTopic(StreamIntelProperties props) {
        return TopicBuilder.name(props.topics().alerts()).partitions(1).replicas(1).build();
    }

    @Bean
    Resolver resolver(IncidentCorrelator correlator, AlertDispatcher alerts, Clock clock) {
        return new Resolver(correlator, alerts, clock);
    }

    /** Closes quiet incidents and sends their resolution notices. */
    public static class Resolver {
        private final IncidentCorrelator correlator;
        private final AlertDispatcher alerts;
        private final Clock clock;

        Resolver(IncidentCorrelator correlator, AlertDispatcher alerts, Clock clock) {
            this.correlator = correlator;
            this.alerts = alerts;
            this.clock = clock;
        }

        @Scheduled(fixedDelayString = "PT15S")
        public void run() {
            correlator.resolveQuiet(clock.instant()).forEach(alerts::dispatch);
        }
    }
}
