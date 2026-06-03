package dev.searchly.api.repository;

import dev.searchly.api.model.DocumentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface DocumentRepository extends JpaRepository<DocumentEntity, UUID> {
    Optional<DocumentEntity> findByIdAndTenantId(UUID id, String tenantId);
    long countByTenantId(String tenantId);
    void deleteByIdAndTenantId(UUID id, String tenantId);
}
