package dev.searchly.api.security;

import dev.searchly.api.model.TenantEntity;
import dev.searchly.api.model.UserEntity;
import dev.searchly.api.repository.TenantRepository;
import dev.searchly.api.repository.UserRepository;
import dev.searchly.common.TenantContext;
import dev.searchly.common.Tier;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * Dev-mode tenant + role resolution.
 * Production: parse JWT (RS256), validate iss/aud/exp, extract tenant_id + roles claims.
 * Here: read X-Tenant-Id and X-User-Roles headers. Enforces tenant exists.
 * Critical rule (anti-IDOR): in JWT mode, JWT.tenant_id MUST equal X-Tenant-Id or path tenant param.
 */
@Component
@Order(1)
public class TenantSecurityFilter extends OncePerRequestFilter {

    private static final String H_TENANT = "X-Tenant-Id";
    private static final String H_ROLES = "X-User-Roles";
    private static final String H_USER = "X-User-Id";

    private final TenantRepository tenantRepo;
    private final UserRepository userRepo;

    public TenantSecurityFilter(TenantRepository tenantRepo, UserRepository userRepo) {
        this.tenantRepo = tenantRepo;
        this.userRepo = userRepo;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        // Admin endpoints have their own token-based auth (see AdminController).
        return path.startsWith("/actuator") || path.startsWith("/api/v1/admin");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        String tenantId = req.getHeader(H_TENANT);
        if (tenantId == null && req.getParameter("tenant") != null) {
            tenantId = req.getParameter("tenant");
        }
        if (tenantId == null || tenantId.isBlank()) {
            resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing X-Tenant-Id");
            return;
        }

        Optional<TenantEntity> tenant = tenantRepo.findById(tenantId);
        if (tenant.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Unknown tenant");
            return;
        }

        String userId = Optional.ofNullable(req.getHeader(H_USER)).orElse(null);
        Set<String> roles;
        if (userId != null) {
            // Look up user in DB; enforce user belongs to claimed tenant (anti-IDOR at user level).
            Optional<UserEntity> user = userRepo.findById(userId);
            if (user.isEmpty() || !user.get().getTenantId().equals(tenantId)) {
                resp.sendError(HttpServletResponse.SC_FORBIDDEN, "User not authorized for this tenant");
                return;
            }
            roles = Set.of(user.get().getRoles().split(","));
        } else {
            // Anonymous header-only mode (no X-User-Id) — roles from header, defaults to VIEWER.
            String rolesHeader = Optional.ofNullable(req.getHeader(H_ROLES)).orElse("VIEWER");
            roles = Set.of(rolesHeader.split(","));
            userId = "dev-anonymous";
        }
        Tier tier = tenant.get().getTier();

        try {
            TenantContextHolder.set(new TenantContext(tenantId, userId, tier, roles));
            chain.doFilter(req, resp);
        } finally {
            TenantContextHolder.clear();
        }
    }
}
