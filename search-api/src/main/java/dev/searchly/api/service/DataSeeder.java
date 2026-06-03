package dev.searchly.api.service;

import dev.searchly.api.model.TenantEntity;
import dev.searchly.api.repository.DocumentRepository;
import dev.searchly.api.repository.TenantRepository;
import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.common.DocumentDto;
import dev.searchly.common.TenantContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Seeds sample documents on startup if the documents table is empty.
 * Goes through DocumentService so the records end up in Postgres AND Kafka → OpenSearch,
 * exercising the full async indexing path. Idempotent (skips if any doc already exists).
 * Disable in production via SEARCHLY_SEED_DATA=false.
 */
@Component
public class DataSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DataSeeder.class);

    private final DocumentService docs;
    private final DocumentRepository docRepo;
    private final TenantRepository tenantRepo;
    private final boolean enabled;

    public DataSeeder(DocumentService docs, DocumentRepository docRepo, TenantRepository tenantRepo,
                      @Value("${searchly.seed-data:true}") boolean enabled) {
        this.docs = docs;
        this.docRepo = docRepo;
        this.tenantRepo = tenantRepo;
        this.enabled = enabled;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!enabled) {
            log.info("Data seeding disabled (searchly.seed-data=false).");
            return;
        }
        if (docRepo.count() > 0) {
            log.info("Documents already present ({}). Skipping seed.", docRepo.count());
            return;
        }

        log.info("Seeding sample documents…");
        seedFor("acme", "alice", samples("acme"));
        seedFor("globex", "dave", samples("globex"));
        seedFor("initech", "grace", samples("initech"));
        seedFor("umbrella", "judy", samples("umbrella"));
        log.info("Seed complete. Indexer will materialize into OpenSearch asynchronously.");
    }

    private void seedFor(String tenantId, String userId, List<DocumentDto.CreateRequest> reqs) {
        TenantEntity tenant = tenantRepo.findById(tenantId).orElse(null);
        if (tenant == null) {
            log.warn("Tenant {} not found, skipping seed for it.", tenantId);
            return;
        }
        TenantContext ctx = new TenantContext(tenantId, userId, tenant.getTier(),
                Set.of("TENANT_ADMIN", "EDITOR", "VIEWER"));
        try {
            TenantContextHolder.set(ctx);
            for (DocumentDto.CreateRequest req : reqs) {
                try {
                    docs.create(req, null);
                } catch (Exception e) {
                    log.warn("Seed failed for tenant {} doc '{}': {}", tenantId, req.title(), e.getMessage());
                }
            }
            log.info("Seeded {} documents for tenant {}.", reqs.size(), tenantId);
        } finally {
            TenantContextHolder.clear();
        }
    }

    private static List<DocumentDto.CreateRequest> samples(String tenantId) {
        return switch (tenantId) {
            case "acme" -> List.of(
                doc("Q4 2025 Revenue Report",
                    "Acme Corp reported revenue growth of 23% year-over-year, driven by strong enterprise sales in EMEA and APAC. Operating margins expanded by 180 basis points.",
                    "alice", List.of("finance", "2025", "earnings")),
                doc("Engineering Roadmap H1 2026",
                    "Priorities: migrate the auth service to OIDC, complete the Kafka tiered-storage rollout, and ship the new search relevance model in March.",
                    "bob", List.of("engineering", "roadmap", "2026")),
                doc("Customer Success Playbook",
                    "Guidance for onboarding enterprise customers: dedicated CSM assignment, 30-day technical kickoff, quarterly business reviews.",
                    "alice", List.of("customer-success", "playbook")),
                doc("Acme Security Whitepaper",
                    "Overview of Acme's security posture: SOC 2 Type II, encryption at rest and in transit, zero-trust network architecture, and quarterly third-party pen tests.",
                    "carol", List.of("security", "compliance", "whitepaper"))
            );
            case "globex" -> List.of(
                doc("Globex Annual Strategy 2026",
                    "Three-year horizon: expand into Latin America, double-down on data products, retire legacy ERP integration by Q3.",
                    "dave", List.of("strategy", "2026", "executive")),
                doc("Platform SLA Definitions",
                    "Tier-1 customers receive a 99.95% availability SLA with credits applied automatically. Tier-2 receive 99.9%.",
                    "eve", List.of("sla", "platform", "contracts")),
                doc("Incident Postmortem: 2025-11-14 Kafka lag",
                    "A downstream consumer lagged by 45 minutes during peak traffic due to a synchronous DB call inside the consumer loop. Resolution: scale consumers, add missing index, ship CI rule against synchronous DB in listeners.",
                    "dave", List.of("incident", "postmortem", "kafka"))
            );
            case "initech" -> List.of(
                doc("Initech Office Manual",
                    "Office hours, dress code, and the famously contested TPS report cover-sheet policy. See section 7 for printer troubleshooting.",
                    "grace", List.of("hr", "manual", "office")),
                doc("Quarterly All-Hands Notes",
                    "Revenue up 4% quarter-over-quarter. New hires in customer support and a refreshed product roadmap focused on stability over features.",
                    "heidi", List.of("all-hands", "notes"))
            );
            case "umbrella" -> List.of(
                doc("Project NEMESIS — Phase 2 Design",
                    "Architecture for the next-generation distributed coordination service. Multi-region active-active, conflict-free replicated data types, and a custom consensus protocol.",
                    "judy", List.of("project", "design", "distributed-systems")),
                doc("Compliance Audit Findings — 2025-Q4",
                    "Findings: (a) two services missing structured audit logging, (b) one S3 bucket lacking versioning, (c) JWT TTL longer than policy. All remediated; re-audit scheduled for Q1.",
                    "mallory", List.of("compliance", "audit", "security")),
                doc("Production Readiness Checklist",
                    "Before promoting any service to GA: SLOs defined, alerts wired, runbook written, on-call rotation set, dashboards live, backup tested, chaos game-day completed.",
                    "judy", List.of("production", "readiness", "checklist")),
                doc("Tenant Tiering Policy",
                    "Customers are placed on FREE, STANDARD, PREMIUM, or ENTERPRISE tier based on contracted volume. Tier upgrades trigger automatic re-routing to dedicated infrastructure.",
                    "nia", List.of("tenants", "policy", "tiering"))
            );
            default -> List.of();
        };
    }

    private static DocumentDto.CreateRequest doc(String title, String content, String author, List<String> tags) {
        return new DocumentDto.CreateRequest(title, content,
                Map.of("author", author, "tags", tags));
    }
}
