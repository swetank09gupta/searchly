package dev.searchly.common;

import java.time.Instant;
import java.util.Map;

public record IndexingEvent(
        String docId,
        String tenantId,
        Tier tier,
        String title,
        String content,
        Map<String, Object> metadata,
        Instant createdAt,
        String idempotencyKey
) {}
