package dev.searchly.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
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
 * Protected by a circuit breaker so a downed Ollama does not make every
 * search request block for 120 s before returning.
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
                .version(HttpClient.Version.HTTP_1_1)  // Ollama/FastAPI don't support HTTP/2
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Sends a prompt and returns the model's text response.
     * Returns null via fallback if Ollama is unavailable (RAG answer gracefully omitted).
     */
    @CircuitBreaker(name = "ollama", fallbackMethod = "generateFallback")
    @SuppressWarnings("unchecked")
    public String generate(String prompt) {
        try {
            Map<String, Object> body = Map.of(
                    "model",  model,
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
                throw new RuntimeException("Ollama HTTP " + resp.statusCode() + ": "
                        + resp.body().substring(0, Math.min(200, resp.body().length())));
            }
            Map<String, Object> result = mapper.readValue(resp.body(), Map.class);
            return (String) result.get("response");
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("Ollama call failed", e);
        }
    }

    @SuppressWarnings("unused")
    private String generateFallback(String prompt, Throwable t) {
        log.warn("Ollama circuit open or failed — skipping LLM answer: {}", t.getMessage());
        return null;
    }
}
