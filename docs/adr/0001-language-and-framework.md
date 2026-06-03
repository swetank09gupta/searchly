# ADR 0001: Java 21 + Spring Boot 3 + Maven

**Status:** Accepted
**Date:** 2026-06-03

## Context

We need to choose a language and framework for an enterprise-grade multi-tenant document search service. Constraints: 3–4h implementation budget, must demonstrate production-grade patterns (security, observability, resilience, multi-tenancy), and align with the author's strongest stack for fastest delivery.

## Decision

Use **Java 21** (LTS) with **Spring Boot 3.x** and **Maven** as the build tool, structured as a multi-module project.

## Consequences

**Positive**
- Maximum coverage of enterprise concerns with the least bespoke code: Spring Security, Spring Cloud Gateway, Spring Data, Actuator, Micrometer all integrate natively.
- Java 21 virtual threads simplify high-concurrency request handling.
- Maven multi-module enforces separation between gateway, search-api, indexer, and shared code.
- Large hiring pool and well-known patterns make the code accessible to reviewers.

**Negative**
- Heavier baseline footprint than Go/Rust for the same problem; JVM tuning required at scale.
- Maven is more verbose than Gradle; accepted by explicit user preference.

**Neutral**
- Native image (GraalVM) remains available as a future optimization for the Gateway.
