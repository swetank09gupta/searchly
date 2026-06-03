package dev.searchly.api.repository;

import dev.searchly.api.model.TenantEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TenantRepository extends JpaRepository<TenantEntity, String> {
}
