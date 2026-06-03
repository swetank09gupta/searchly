package dev.searchly.common;

import java.util.Set;

public record TenantContext(
        String tenantId,
        String userId,
        Tier tier,
        Set<String> roles
) {
    public boolean hasRole(String role) {
        return roles != null && roles.contains(role);
    }
}
