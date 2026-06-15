package dev.searchly.api.controller;

import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.api.service.SyncService;
import dev.searchly.common.TenantContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * Sync lifecycle endpoints for the connector.
 *
 * After each sync cycle the connector calls POST /purge-stale to remove
 * OpenSearch + Postgres records for source documents that were not seen
 * during the current sync (deleted Jira tickets, removed Confluence pages, etc.).
 *
 * Tracking rows are automatically created / refreshed by DocumentService.create()
 * whenever a document is indexed with metadata.source_id + metadata.source set.
 * No separate mark-seen call is needed from the connector.
 */
@RestController
@RequestMapping("/api/v1/admin/sync")
public class SyncController {

    private final SyncService syncService;

    public SyncController(SyncService syncService) {
        this.syncService = syncService;
    }

    /**
     * POST /api/v1/admin/sync/purge-stale
     *
     * Body:
     * {
     *   "source_type":     "jira",
     *   "sync_started_at": "2024-05-01T10:00:00Z"
     * }
     *
     * Deletes all tracked source docs for this tenant whose last_seen_at is
     * earlier than sync_started_at — i.e. they were not indexed in this cycle.
     */
    @PostMapping("/purge-stale")
    public ResponseEntity<Map<String, Object>> purgeStale(
            @RequestBody Map<String, String> body) {

        TenantContext ctx = TenantContextHolder.require();
        if (!ctx.roles().contains("TENANT_ADMIN")
                && !ctx.roles().contains("SERVICE")
                && !ctx.roles().contains("EDITOR")) {
            return ResponseEntity.status(403)
                    .body(Map.of("error", "Insufficient role — requires TENANT_ADMIN, SERVICE, or EDITOR"));
        }

        String sourceType    = body.get("source_type");
        String syncStartedAt = body.get("sync_started_at");

        if (sourceType == null || sourceType.isBlank()
                || syncStartedAt == null || syncStartedAt.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "source_type and sync_started_at are required"));
        }

        Instant cutoff;
        try {
            cutoff = Instant.parse(syncStartedAt);
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "sync_started_at must be an ISO-8601 instant"));
        }

        String tenantId = ctx.tenantId();
        int purged = syncService.purgeStale(tenantId, sourceType, cutoff);
        return ResponseEntity.ok(Map.of("purged", purged, "source_type", sourceType));
    }
}
