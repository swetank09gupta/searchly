package dev.searchly.indexer;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits document text into overlapping chunks suitable for embedding.
 * Target: ~375 tokens per chunk (1 token ≈ 4 chars), 50-token overlap.
 * Breaks at sentence boundaries when possible to avoid mid-sentence splits.
 */
@Service
public class ChunkingService {

    private static final int CHUNK_CHARS = 1500;
    private static final int OVERLAP_CHARS = 200;

    public List<String> chunk(String text) {
        List<String> chunks = new ArrayList<>();
        if (text == null || text.isBlank()) return chunks;

        String t = text.strip();
        int start = 0;
        while (start < t.length()) {
            int end = Math.min(start + CHUNK_CHARS, t.length());
            // Prefer to break at a sentence boundary in the second half of the window
            if (end < t.length()) {
                int boundary = lastSentenceBoundary(t, start + CHUNK_CHARS / 2, end);
                if (boundary > 0) end = boundary;
            }
            String chunk = t.substring(start, end).strip();
            if (!chunk.isEmpty()) chunks.add(chunk);
            start = end - OVERLAP_CHARS;
            if (start <= 0 || start >= t.length()) break;
        }
        return chunks;
    }

    // Returns the position just after the last '. ', '? ', or '! ' in [from, to).
    private int lastSentenceBoundary(String text, int from, int to) {
        int best = -1;
        for (int i = to - 1; i >= from; i--) {
            char c = text.charAt(i);
            if ((c == '.' || c == '?' || c == '!') && i + 1 < text.length()) {
                best = i + 1;
                break;
            }
        }
        return best;
    }
}
