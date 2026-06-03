# ADR 0010: Spring Cloud Gateway as Edge

**Status:** Accepted
**Date:** 2026-06-03

## Context

We need an edge layer to handle TLS termination, request routing, JWT validation, rate limiting, CORS, security headers, and request-size limits before requests reach the service tier. Options: Kong, Envoy, NGINX, AWS API Gateway, Spring Cloud Gateway.

## Decision

Use **Spring Cloud Gateway** (reactive, Netty-based) as the edge.

## Consequences

**Positive**
- Same language and ecosystem as the services — shared security configuration, shared tracing/metrics, single CI pipeline.
- Reactive WebFlux model handles many concurrent connections per pod efficiently.
- First-class integration with Spring Security, Micrometer, OpenTelemetry, and Resilience4j.
- Java-implementable filters for custom logic (sliding-window rate limit, tenant routing).

**Negative**
- Operationally heavier than NGINX/Envoy at very high scale (100K+ QPS per pod).
- JVM cold starts are non-trivial; mitigated by minimum pod count and (future) GraalVM native image.

**At higher scale (future)**
- Front Spring Cloud Gateway with a thin L7 layer (Envoy or a managed CDN/WAF) for TLS, DDoS, and geo-routing — Spring Cloud Gateway then handles policy and business-aware routing.

**Rejected alternatives**
- **Kong:** great gateway but adds another runtime and language.
- **AWS API Gateway:** vendor lock-in; harder to test locally.
- **NGINX/Envoy alone:** powerful but custom policy (per-tenant sliding-window from Redis) is easier in Java.
