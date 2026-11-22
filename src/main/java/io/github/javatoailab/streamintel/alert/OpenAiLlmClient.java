package io.github.javatoailab.streamintel.alert;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/** OpenAI-compatible chat completions (OpenAI, Ollama, vLLM, or LLM-Gateway in front of several). */
public final class OpenAiLlmClient implements LlmClient {

    record Msg(String role, String content) {
    }

    record Request(String model, List<Msg> messages, double temperature, @JsonProperty("max_tokens") int maxTokens) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Response(List<Choice> choices) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Choice(Msg message) {
    }

    private final URI endpoint;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final JsonMapper json;
    private final HttpClient http;

    public OpenAiLlmClient(URI baseUrl, String model, String apiKey, Duration timeout, JsonMapper json) {
        this.endpoint = baseUrl.resolve("/v1/chat/completions");
        this.model = model;
        this.apiKey = apiKey;
        this.timeout = timeout;
        this.json = json;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    }

    @Override
    public String complete(String system, String user) {
        HttpRequest.Builder b = HttpRequest.newBuilder(endpoint).timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(
                        new Request(model, List.of(new Msg("system", system), new Msg("user", user)), 0.1, 300))));
        if (apiKey != null && !apiKey.isBlank()) {
            b.header("Authorization", "Bearer " + apiKey);
        }
        try {
            HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() >= 300) {
                throw new IllegalStateException("LLM returned " + r.statusCode());
            }
            return json.readValue(r.body(), Response.class).choices().getFirst().message().content();
        } catch (IOException e) {
            throw new IllegalStateException("LLM unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }
}
