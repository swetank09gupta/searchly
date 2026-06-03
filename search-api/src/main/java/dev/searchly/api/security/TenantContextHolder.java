package dev.searchly.api.security;

import dev.searchly.common.TenantContext;

public final class TenantContextHolder {
    private static final ThreadLocal<TenantContext> CTX = new ThreadLocal<>();

    private TenantContextHolder() {}

    public static void set(TenantContext ctx) { CTX.set(ctx); }
    public static TenantContext get() { return CTX.get(); }
    public static void clear() { CTX.remove(); }

    public static TenantContext require() {
        TenantContext c = CTX.get();
        if (c == null) throw new IllegalStateException("Tenant context not set");
        return c;
    }
}
