package dev.searchly.api.repository;

import dev.searchly.api.model.KgRelationshipEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface KgRelationshipRepository extends JpaRepository<KgRelationshipEntity, Long> {

    List<KgRelationshipEntity> findByFromTypeAndFromIdAndTenantId(
            String fromType, String fromId, String tenantId);

    List<KgRelationshipEntity> findByToTypeAndToIdAndTenantId(
            String toType, String toId, String tenantId);

    Optional<KgRelationshipEntity> findByFromTypeAndFromIdAndRelationAndToTypeAndToIdAndTenantId(
            String fromType, String fromId, String relation,
            String toType, String toId, String tenantId);
}
