package io.github.javatoailab.streamintel.config;

import io.github.javatoailab.streamintel.detect.Direction;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

@ConfigurationProperties("streamintel")
public record StreamIntelProperties(
        @DefaultValue Detection detection,
        @DefaultValue Map<String, Direction> directions,
        @DefaultValue Correlation correlation,
        @DefaultValue Llm llm,
        URI webhookUrl,
        @DefaultValue Topics topics) {

    public record Detection(@DefaultValue("240") int window, @DefaultValue("60") int minSamples,
                            @DefaultValue("6.0") double threshold, @DefaultValue("0.25") double minRelativeChange,
                            @DefaultValue("3") int confirm, @DefaultValue("5") int of) {
    }

    public record Correlation(@DefaultValue("2m") Duration window, @DefaultValue("5m") Duration quietPeriod) {
    }

    public record Llm(@DefaultValue("false") boolean enabled, @DefaultValue("http://localhost:11434") URI baseUrl,
                      @DefaultValue("llama3.2") String model, String apiKey, @DefaultValue("8s") Duration timeout) {
    }

    public record Topics(@DefaultValue("streamintel.metrics") String metrics,
                         @DefaultValue("streamintel.alerts") String alerts) {
    }
}
