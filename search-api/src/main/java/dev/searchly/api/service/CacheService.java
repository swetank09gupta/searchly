package dev.searchly.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.searchly.common.DocumentDto;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Set;

@Service
public class CacheService {
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Duration ttl;

    public CacheService(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
        this.ttl = Duration.ofSeconds(60);
    }

    public String key(String tenantId, String role, String q, int page, int size) {
        String raw = tenantId + "|" + role + "|" + q + "|" + page + "|" + size;
        return "cache:" + tenantId + ":" + sha256(raw);
    }

    public DocumentDto.SearchResponse get(String key) {
        String json = redis.opsForValue().get(key);
        if (json == null) return null;
        try {
            return mapper.readValue(json, DocumentDto.SearchResponse.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public void put(String key, DocumentDto.SearchResponse value) {
        try {
            redis.opsForValue().set(key, mapper.writeValueAsString(value), ttl);
        } catch (JsonProcessingException ignored) {}
    }

    public void invalidateTenant(String tenantId) {
        Set<String> keys = redis.keys("cache:" + tenantId + ":*");
        if (keys != null && !keys.isEmpty()) redis.delete(keys);
    }

    private static String sha256(String s) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
