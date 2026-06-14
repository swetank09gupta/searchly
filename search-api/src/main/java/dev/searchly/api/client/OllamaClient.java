package dev.searchly.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Thin client for Ollama's /api/generate endpoint (non-streaming).
 */
@Component
public class OllamaClient {
    private static final Logger log = LoggerFactory.getLogger(OllamaClient.class);

    private final String baseUrl;
    private final String model;
    private final ObjectMapper mapper;
    private final HttpClient http;

    public OllamaClient(
            @Value("${searchly.ollama.url:http://localhost:11434}") String baseUrl,
            @Value("${searchly.ollama.model:llama3.2:3b}") String model,
            ObjectMapper mapper) {
        this.baseUrl = baseUrl;
        this.model = model;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Sends a prompt and returns the model's text response.
     * Returns null if Ollama is unavailable (RAG answer will be omitted gracefully).
     */
    @SuppressWarnings("unchecked")
    public String generate(String prompt) {
        try {
            Map<String, Object> body = Map.of(
                    "model", model,
                    "prompt", prompt,
                    "stream", false);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/generate"))
                    .header("Content-Type", "application/json")
                    // LLM generation can take 10-30 s on CPU — generous timeout
                    .timeout(Duration.ofSeconds(120))
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                log.warn("Ollama HTTP {}: {}", resp.statusCode(), resp.body());
                return null;
            }
            Map<String, Object> result = mapper.readValue(resp.body(), Map.class);
            return (String) result.get("response");
        } catch (Exception e) {
            log.warn("Ollama call failed: {}", e.getMessage());
            return null;
        }
    }
}
