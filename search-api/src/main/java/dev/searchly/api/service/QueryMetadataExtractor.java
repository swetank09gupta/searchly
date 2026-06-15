package dev.searchly.api.service;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts metadata filters directly from the user's natural language query.
 *
 * "Redis issue in operator-backend in prod"
 *   → {env=prod, service=operator-backend}
 *
 * "Show all deployment issues in staging"
 *   → {env=staging, source=deployment_state}
 *
 * Filters are applied to both BM25 and kNN queries before retrieval —
 * the LLM never makes filter decisions.
 */
@Component
public class QueryMetadataExtractor {

    // ── Environment ──────────────────────────────────────────────────────────
    private static final Pattern ENV_PATTERN = Pattern.compile(
            "\\b(prod(?:uction)?|staging|stage|uat|dev(?:elopment)?|test(?:ing)?)\\b",
            Pattern.CASE_INSENSITIVE);

    // ── Known service names ───────────────────────────────────────────────────
    private static final Pattern SERVICE_PATTERN = Pattern.compile(
            "\\b(operator[- ]backend|allocator|picker|induct(?:or)?|" +
            "filebeat|oga|go-agent|rds|redis|kafka|opensearch|elastic|" +
            "wms|greymatter|pick[- ]assist|gsb|rdc)\\b",
            Pattern.CASE_INSENSITIVE);

    // ── Source type signals ───────────────────────────────────────────────────
    private static final Pattern DEPLOYMENT_PATTERN = Pattern.compile(
            "\\b(deploy(?:ment)?|version|release|rollout|running version|which version|" +
            "deployed|pod|container|image)\\b",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern LOGS_PATTERN = Pattern.compile(
            "\\b(log|error|exception|traceback|crash|stacktrace|stderr|" +
            "500|timeout|refused|failed|failure)\\b",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern JIRA_PATTERN = Pattern.compile(
            "\\b(jira|ticket|issue|bug|AES-\\d+|GM-\\d+|known[ -]issue|open[ -]bug)\\b",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern CODE_PATTERN = Pattern.compile(
            "\\b(code|github|git|repository|class|method|function|" +
            "implementation|interface|api|src|service code)\\b",
            Pattern.CASE_INSENSITIVE);

    // ── Env normalisation map ─────────────────────────────────────────────────
    private static final Map<String, String> ENV_NORMALIZE = Map.of(
            "production", "prod",
            "stage",      "staging",
            "uat",        "staging",
            "development","dev",
            "testing",    "testing",
            "test",       "testing"
    );

    public record QueryFilters(
            String env,          // prod / staging / dev / testing — or null
            String service,      // service name — or null
            String sourceType,   // deployment_state / warehouse_logs / jira / git / confluence — or null
            Map<String, String> asMap
    ) {
        public static QueryFilters empty() {
            return new QueryFilters(null, null, null, Map.of());
        }

        public boolean hasAny() {
            return env != null || service != null || sourceType != null;
        }
    }

    public QueryFilters extract(String query) {
        if (query == null || query.isBlank()) return QueryFilters.empty();

        String env       = extractEnv(query);
        String service   = extractService(query);
        String source    = extractSourceType(query);

        Map<String, String> asMap = new LinkedHashMap<>();
        if (env     != null) asMap.put("env",     env);
        if (service != null) asMap.put("service", service);
        if (source  != null) asMap.put("source",  source);

        return new QueryFilters(env, service, source, asMap);
    }

    private String extractEnv(String q) {
        Matcher m = ENV_PATTERN.matcher(q);
        if (!m.find()) return null;
        String raw = m.group(1).toLowerCase();
        return ENV_NORMALIZE.getOrDefault(raw, raw);
    }

    private String extractService(String q) {
        Matcher m = SERVICE_PATTERN.matcher(q);
        if (!m.find()) return null;
        return m.group(1).toLowerCase().replace(" ", "-");
    }

    private String extractSourceType(String q) {
        if (DEPLOYMENT_PATTERN.matcher(q).find()) return "deployment_state";
        if (LOGS_PATTERN.matcher(q).find())       return "warehouse_logs";
        if (JIRA_PATTERN.matcher(q).find())        return "jira";
        if (CODE_PATTERN.matcher(q).find())        return "git";
        return null;
    }
}
