package dev.searchly.api.repository;

import dev.searchly.api.model.SourceDocumentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SourceDocumentRepository extends JpaRepository<SourceDocumentEntity, Long> {

    Optional<SourceDocumentEntity> findBySourceIdAndSourceTypeAndTenantId(
            String sourceId, String sourceType, String tenantId);

    /** Returns all tracked docs for a tenant+type that were NOT seen since the cutoff. */
    List<SourceDocumentEntity> findByTenantIdAndSourceTypeAndLastSeenAtBefore(
            String tenantId, String sourceType, Instant cutoff);

    /** Deletes the tracking rows; callers are responsible for removing OS/Postgres/MinIO data first. */
    @Modifying
    @Query("DELETE FROM SourceDocumentEntity s WHERE s.tenantId = :tenantId " +
           "AND s.sourceType = :sourceType AND s.lastSeenAt < :cutoff")
    int deleteStale(@Param("tenantId") String tenantId,
                    @Param("sourceType") String sourceType,
                    @Param("cutoff") Instant cutoff);
}
