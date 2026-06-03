# ADR 0008: OIDC + JWT (RS256) via Keycloak

**Status:** Accepted
**Date:** 2026-06-03

## Context

The service must authenticate human users and machine clients across many tenants, and identity must propagate to downstream services without extra round-trips. Options: server-side sessions, opaque tokens with introspection, or signed JWTs.

## Decision

Use **OIDC** with **JWT bearer tokens signed with RS256**, issued by **Keycloak** in the local stack (and intended to be swappable for Okta/Auth0/Cognito in customer deployments).

JWT claims carry: `sub`, `tenant_id`, `tier`, `roles`, `scopes`, standard timing claims.

## Consequences

**Positive**
- Stateless verification at the gateway and at each service using cached JWKS — no per-request IdP round-trip.
- Asymmetric signing means downstream services only need the public key; the private key never leaves the IdP.
- Identity, tenant, tier, and roles all travel with the request — no extra lookups for authZ.
- OIDC discovery + JWKS rotation handled by standard libraries.

**Negative**
- JWTs can't be revoked synchronously; mitigated by short TTL (15 min) + refresh tokens + a revocation list (Redis bloom filter) for emergency revoke.
- Token size grows with claims; we keep claims minimal (no PII).

**Defense in depth**
- JWT validated at the **gateway** AND again at the **service** layer (so a misconfigured gateway can't bypass auth).
- `TenantSecurityFilter` enforces JWT `tenant_id` == request `tenant_id`.
- M2M clients use OAuth2 client_credentials (or scoped API keys, Argon2-hashed at rest, for legacy integrations).
