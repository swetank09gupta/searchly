package dev.searchly.common;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;

public class DocumentDto {

    public record CreateRequest(
            @NotBlank @Size(max = 500) String title,
            @NotBlank @Size(max = 1_000_000) String content,
            Map<String, Object> metadata
    ) {}

    public record CreateResponse(
            String id,
            String tenantId,
            String status,
            Instant createdAt
    ) {}

    public record DocumentView(
            String id,
            String tenantId,
            String title,
            String content,
            Map<String, Object> metadata,
            String status,
            Instant createdAt
    ) {}

    public record SearchHit(
            String id,
            double score,
            String title,
            java.util.List<String> highlights,
            Map<String, Object> metadata
    ) {}

    public record SearchResponse(
            long took,
            long total,
            int page,
            int size,
            java.util.List<SearchHit> hits,
            Map<String, Map<String, Long>> facets
    ) {}
}
