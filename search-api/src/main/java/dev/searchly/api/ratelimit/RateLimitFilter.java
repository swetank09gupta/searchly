package dev.searchly.api.ratelimit;

import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.common.TenantContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
@Order(2) // after TenantSecurityFilter
public class RateLimitFilter extends OncePerRequestFilter {

    private final SlidingWindowRateLimiter limiter;

    public RateLimitFilter(SlidingWindowRateLimiter limiter) {
        this.limiter = limiter;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String p = request.getRequestURI();
        return p.startsWith("/actuator") || p.startsWith("/api/v1/admin");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        TenantContext ctx = TenantContextHolder.get();
        if (ctx == null) {
            chain.doFilter(req, resp);
            return;
        }
        int limit = ctx.tier().qpsLimit;
        boolean allowed = limiter.tryAcquire(ctx.tenantId(), limit, 1000L);
        if (!allowed) {
            resp.setStatus(429);
            resp.setHeader("Retry-After", "1");
            resp.setContentType("application/json");
            resp.getWriter().write(String.format(
                "{\"status\":429,\"detail\":\"Rate limit exceeded for tenant %s (%s tier: %d req/s)\"}",
                ctx.tenantId(), ctx.tier().name(), limit));
            return;
        }
        chain.doFilter(req, resp);
    }
}
