package dev.searchly.common;

import java.util.Map;

public record IndexingEvent(
        String docId,
        String tenantId,
        Tier tier,
        String title,
        String content,
        Map<String, Object> metadata,
        long createdAt,
        String idempotencyKey
) {}
