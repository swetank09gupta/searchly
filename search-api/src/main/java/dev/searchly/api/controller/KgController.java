package dev.searchly.api.controller;

import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.api.service.KnowledgeGraphService;
import dev.searchly.api.service.KnowledgeGraphService.Neighbor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * P3.1 Knowledge Graph REST API.
 *
 * Auth/tenant isolation is enforced upstream by TenantSecurityFilter.
 * tenantId is read from the thread-local TenantContextHolder.
 */
@RestController
@RequestMapping("/api/v1/kg")
public class KgController {

    private final KnowledgeGraphService kgService;

    public KgController(KnowledgeGraphService kgService) {
        this.kgService = kgService;
    }

    /**
     * POST /api/v1/kg/entity
     *
     * Body: { "entity_type": "...", "entity_id": "...", "name": "...", "properties": {} }
     */
    @PostMapping("/entity")
    public ResponseEntity<Map<String, Object>> upsertEntity(
            @RequestBody Map<String, Object> body) {

        String tenantId  = TenantContextHolder.require().tenantId();
        String entityType = (String) body.get("entity_type");
        String entityId   = (String) body.get("entity_id");
        String name       = (String) body.get("name");

        if (entityType == null || entityType.isBlank()
                || entityId == null || entityId.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "entity_type and entity_id are required"));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = body.get("properties") instanceof Map
                ? (Map<String, Object>) body.get("properties")
                : Map.of();

        kgService.upsertEntity(entityType, entityId, tenantId, name, properties);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("status", "ok"));
    }

    /**
     * POST /api/v1/kg/relationship
     *
     * Body: { "from_type": "...", "from_id": "...", "relation": "...",
     *         "to_type": "...", "to_id": "...", "properties": {} }
     */
    @PostMapping("/relationship")
    public ResponseEntity<Map<String, Object>> upsertRelationship(
            @RequestBody Map<String, Object> body) {

        String tenantId = TenantContextHolder.require().tenantId();
        String fromType = (String) body.get("from_type");
        String fromId   = (String) body.get("from_id");
        String relation = (String) body.get("relation");
        String toType   = (String) body.get("to_type");
        String toId     = (String) body.get("to_id");

        if (fromType == null || fromType.isBlank()
                || fromId == null || fromId.isBlank()
                || relation == null || relation.isBlank()
                || toType == null || toType.isBlank()
                || toId == null || toId.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "from_type, from_id, relation, to_type, and to_id are required"));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = body.get("properties") instanceof Map
                ? (Map<String, Object>) body.get("properties")
                : Map.of();

        kgService.upsertRelationship(fromType, fromId, relation, toType, toId, tenantId, properties);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("status", "ok"));
    }

    /**
     * GET /api/v1/kg/entity/{entityType}/{entityId}/neighbors
     */
    @GetMapping("/entity/{entityType}/{entityId}/neighbors")
    public ResponseEntity<List<Neighbor>> getNeighbors(
            @PathVariable String entityType,
            @PathVariable String entityId) {

        String tenantId = TenantContextHolder.require().tenantId();
        List<Neighbor> neighbors = kgService.getNeighbors(entityType, entityId, tenantId);
        return ResponseEntity.ok(neighbors);
    }

    /**
     * GET /api/v1/kg/traverse/{entityType}/{entityId}?depth=3
     */
    @GetMapping("/traverse/{entityType}/{entityId}")
    public ResponseEntity<List<Map<String, Object>>> traverse(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @RequestParam(defaultValue = "3") int depth) {

        String tenantId = TenantContextHolder.require().tenantId();

        if (depth < 1) depth = 1;
        if (depth > 5) depth = 5;

        List<Map<String, Object>> result = kgService.traverse(entityType, entityId, tenantId, depth);
        return ResponseEntity.ok(result);
    }
}
