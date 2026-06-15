package dev.searchly.indexer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Computes a stable SHA-256 fingerprint of document content.
 * Stored in OpenSearch as {@code content_fingerprint} so the indexer can skip
 * chunk re-embedding when a document's content has not changed between syncs.
 */
public final class ContentFingerprinter {

    private ContentFingerprinter() {}

    public static String fingerprint(String title, String content) {
        String normalized = normalize(title) + "\n\n" + normalize(content);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(normalized.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String normalize(String s) {
        if (s == null) return "";
        return s.strip().replaceAll("\\s+", " ").toLowerCase();
    }
}
