package dev.searchly.api.repository;

import dev.searchly.api.model.KgEntityEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface KgEntityRepository extends JpaRepository<KgEntityEntity, Long> {

    Optional<KgEntityEntity> findByEntityTypeAndEntityIdAndTenantId(
            String entityType, String entityId, String tenantId);

    List<KgEntityEntity> findByTenantIdAndEntityType(String tenantId, String entityType);
}
