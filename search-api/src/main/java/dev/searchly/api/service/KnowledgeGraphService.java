package dev.searchly.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.searchly.api.model.KgEntityEntity;
import dev.searchly.api.model.KgRelationshipEntity;
import dev.searchly.api.repository.KgEntityRepository;
import dev.searchly.api.repository.KgRelationshipRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * P3.1 Knowledge Graph — stores and traverses explicit relationships between
 * knowledge entities (Jira issues, PRs, commits, services, deployments, customers, releases).
 */
@Service
public class KnowledgeGraphService {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeGraphService.class);

    private static final String TRAVERSE_SQL =
            "WITH RECURSIVE graph(entity_type, entity_id, depth) AS (" +
            "    SELECT CAST(:startType AS VARCHAR), CAST(:startId AS VARCHAR), 0" +
            "    UNION ALL" +
            "    SELECT r.to_type, r.to_id, g.depth + 1" +
            "    FROM graph g" +
            "    JOIN kg_relationships r" +
            "         ON r.from_type = g.entity_type" +
            "        AND r.from_id   = g.entity_id" +
            "        AND r.tenant_id = :tenantId" +
            "    WHERE g.depth < :maxDepth" +
            ")" +
            "SELECT DISTINCT e.entity_type, e.entity_id, e.name, g.depth" +
            "  FROM graph g" +
            "  JOIN kg_entities e" +
            "       ON e.entity_type = g.entity_type" +
            "      AND e.entity_id   = g.entity_id" +
            "      AND e.tenant_id   = :tenantId" +
            " ORDER BY g.depth, e.entity_type, e.entity_id";

    private final KgEntityRepository       entityRepo;
    private final KgRelationshipRepository relRepo;
    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper              mapper;

    public KnowledgeGraphService(KgEntityRepository entityRepo,
                                  KgRelationshipRepository relRepo,
                                  NamedParameterJdbcTemplate jdbc,
                                  ObjectMapper mapper) {
        this.entityRepo = entityRepo;
        this.relRepo    = relRepo;
        this.jdbc       = jdbc;
        this.mapper     = mapper;
    }

    /**
     * Inserts or updates a knowledge graph entity.
     * If the entity already exists (entity_type + entity_id + tenant_id), updates name and properties.
     */
    @Transactional
    public void upsertEntity(String entityType, String entityId, String tenantId,
                             String name, Map<String, Object> properties) {
        String propsJson = serializeProperties(properties);
        Optional<KgEntityEntity> existing =
                entityRepo.findByEntityTypeAndEntityIdAndTenantId(entityType, entityId, tenantId);
        if (existing.isPresent()) {
            KgEntityEntity e = existing.get();
            e.setName(name);
            e.setPropertiesJson(propsJson);
            e.setUpdatedAt(java.time.Instant.now());
            entityRepo.saveAndFlush(e);
        } else {
            entityRepo.saveAndFlush(
                    new KgEntityEntity(entityType, entityId, tenantId, name, propsJson));
        }
    }

    /**
     * Inserts a directed relationship between two entities.
     * If the relationship already exists (unique constraint), silently ignores the duplicate.
     */
    @Transactional
    public void upsertRelationship(String fromType, String fromId, String relation,
                                   String toType, String toId, String tenantId,
                                   Map<String, Object> properties) {
        Optional<KgRelationshipEntity> existing =
                relRepo.findByFromTypeAndFromIdAndRelationAndToTypeAndToIdAndTenantId(
                        fromType, fromId, relation, toType, toId, tenantId);
        if (existing.isPresent()) {
            // Relationship already recorded — nothing to update (idempotent)
            return;
        }
        try {
            String propsJson = serializeProperties(properties);
            relRepo.saveAndFlush(
                    new KgRelationshipEntity(fromType, fromId, relation, toType, toId, tenantId, propsJson));
        } catch (DataIntegrityViolationException e) {
            // Race condition: concurrent insert hit the UNIQUE constraint — safe to ignore
            log.debug("upsertRelationship: duplicate ignored for {}/{} -[{}]-> {}/{}",
                    fromType, fromId, relation, toType, toId);
        }
    }

    /**
     * Returns the immediate neighbors of the given entity (one hop in either direction).
     */
    public List<Neighbor> getNeighbors(String entityType, String entityId, String tenantId) {
        List<Neighbor> result = new ArrayList<>();

        // Outbound edges: entity --[relation]--> neighbor
        for (KgRelationshipEntity r : relRepo.findByFromTypeAndFromIdAndTenantId(entityType, entityId, tenantId)) {
            String name = entityRepo
                    .findByEntityTypeAndEntityIdAndTenantId(r.getToType(), r.getToId(), tenantId)
                    .map(KgEntityEntity::getName)
                    .orElse(null);
            result.add(new Neighbor(r.getToType(), r.getToId(), name, r.getRelation(), "out"));
        }

        // Inbound edges: neighbor --[relation]--> entity
        for (KgRelationshipEntity r : relRepo.findByToTypeAndToIdAndTenantId(entityType, entityId, tenantId)) {
            String name = entityRepo
                    .findByEntityTypeAndEntityIdAndTenantId(r.getFromType(), r.getFromId(), tenantId)
                    .map(KgEntityEntity::getName)
                    .orElse(null);
            result.add(new Neighbor(r.getFromType(), r.getFromId(), name, r.getRelation(), "in"));
        }

        return result;
    }

    /**
     * BFS traversal up to maxDepth hops (outbound only) via recursive CTE.
     *
     * @return list of maps with keys: entity_type, entity_id, name, depth
     */
    public List<Map<String, Object>> traverse(String entityType, String entityId,
                                               String tenantId, int maxDepth) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("startType", entityType)
                .addValue("startId",   entityId)
                .addValue("tenantId",  tenantId)
                .addValue("maxDepth",  maxDepth);

        return jdbc.query(TRAVERSE_SQL, params, (rs, rowNum) -> {
            Map<String, Object> row = new HashMap<>();
            row.put("entity_type", rs.getString("entity_type"));
            row.put("entity_id",   rs.getString("entity_id"));
            row.put("name",        rs.getString("name"));
            row.put("depth",       rs.getInt("depth"));
            return row;
        });
    }

    // -------------------------------------------------------------------------

    private String serializeProperties(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return "{}";
        }
        try {
            return mapper.writeValueAsString(properties);
        } catch (Exception e) {
            log.warn("Failed to serialize KG properties, storing as {}: {}", "{}", e.getMessage());
            return "{}";
        }
    }

    // -------------------------------------------------------------------------

    public record Neighbor(
            String entityType,
            String entityId,
            String name,
            String relation,
            String direction   // "out" | "in"
    ) {}
}
