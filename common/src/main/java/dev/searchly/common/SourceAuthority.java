package dev.searchly.common;

/**
 * Authority weight for each source type used in RRF score boosting.
 * Higher weight = source is treated as more authoritative during retrieval.
 */
public enum SourceAuthority {
    LIVE_LOGS(100),
    DEPLOYMENT(90),
    CODE(80),
    JIRA(70),
    RELEASE_NOTES(60),
    CONFLUENCE(50),
    UNKNOWN(50);

    private final int weight;

    SourceAuthority(int weight) {
        this.weight = weight;
    }

    public int weight() {
        return weight;
    }

    /** Returns weight normalised to [0.0, 1.0] for use as a score multiplier. */
    public double normalizedWeight() {
        return weight / 100.0;
    }

    public static SourceAuthority forSource(String source) {
        if (source == null || source.isBlank()) return UNKNOWN;
        return switch (source.toLowerCase()) {
            case "warehouse_logs", "logs"         -> LIVE_LOGS;
            case "deployment_state", "deployment" -> DEPLOYMENT;
            case "git", "code", "github"          -> CODE;
            case "jira"                           -> JIRA;
            case "release_notes"                  -> RELEASE_NOTES;
            case "confluence"                     -> CONFLUENCE;
            default                               -> UNKNOWN;
        };
    }
}
