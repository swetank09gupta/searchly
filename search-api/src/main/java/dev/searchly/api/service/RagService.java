package dev.searchly.api.service;

import dev.searchly.api.client.EmbeddingClient;
import dev.searchly.api.client.KnnSearchClient;
import dev.searchly.api.client.OllamaClient;
import dev.searchly.api.client.RerankClient;
import dev.searchly.api.client.IntelligenceAgentClient;
import dev.searchly.api.service.QueryMetadataExtractor.QueryFilters;
import dev.searchly.common.DocumentDto;
import dev.searchly.common.SourceAuthority;
import dev.searchly.common.TenantContext;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * RAG pipeline — multi-product, customer-aware:
 *
 *  1. Embed query
 *  2. k-NN semantic search on chunks (shared knowledge: code, Jira, Confluence)
 *  3. BM25 keyword search on chunks
 *  4. If customer= is specified: ALSO search customer-specific chunks
 *     (live logs + deployment state for that customer)
 *  5. RRF merge all result lists
 *  6. Inject customer deployment context into LLM prompt
 *  7. Ollama generates a grounded answer
 *
 * The LLM answer therefore knows:
 *   "Customer acme-corp is running Platform v6.0.5 + ServiceAgent v2.3.1 (prod).
 *    Their operator-backend pod has 3 ERRORs in the last 10 min related to Redis.
 *    AES-891 is a known bug for this version, fixed in v2.3.2."
 */
@Service
public class RagService {
    private static final Logger log = LoggerFactory.getLogger(RagService.class);

    private static final int CANDIDATE_K = 50;
    private static final int RERANK_CANDIDATES = 30;
    private static final int CONTEXT_CHUNKS = 6;
    private static final int RRF_K = 60;

    // Virtual-thread pool for I/O-bound retrieval legs — zero OS threads blocked
    private static final ExecutorService RETRIEVAL_POOL = Executors.newVirtualThreadPerTaskExecutor();

    // A query that IS just a Jira key (e.g. "AES-891") is an exact-ID lookup — rewriting
    // would only add noise. Any free-text query, even a short one, still benefits from rewriting.
    private static final Pattern JIRA_KEY_ONLY_RE = Pattern.compile(
        "^[A-Z]{2,6}-\\d{2,6}$",
        Pattern.CASE_INSENSITIVE
    );

    private final EmbeddingClient embedder;
    private final KnnSearchClient knnClient;
    private final OllamaClient ollama;
    private final OpenSearchClient os;
    private final IntelligenceAgentClient intelligenceAgent;
    private final RerankClient reranker;
    private final QueryMetadataExtractor metadataExtractor;

    public RagService(EmbeddingClient embedder, KnnSearchClient knnClient,
                      OllamaClient ollama, OpenSearchClient os,
                      IntelligenceAgentClient intelligenceAgent, RerankClient reranker,
                      QueryMetadataExtractor metadataExtractor) {
        this.embedder = embedder;
        this.knnClient = knnClient;
        this.ollama = ollama;
        this.os = os;
        this.intelligenceAgent = intelligenceAgent;
        this.reranker = reranker;
        this.metadataExtractor = metadataExtractor;
    }

    public record RagResult(
            String  answer,
            List<String> sources,
            String  sessionId,
            boolean needsClarification,
            String  resolvedCustomer,
            String  resolvedEnv,
            String  lifecycleStage,
            String  lifecycleLabel,
            boolean hasLiveData,
            List<DocumentDto.RetrievalTrace> retrievalTraces
    ) {
        // Backward-compat: static RAG result with no conversational fields
        public RagResult(String answer, List<String> sources) {
            this(answer, sources, null, false, null, null, null, null, false, List.of());
        }
    }

    /** Holds the merged RRF result together with per-chunk rank data for tracing. */
    private record MergeResult(
            List<KnnSearchClient.ChunkHit> hits,
            Map<String, Double>  rrfScores,
            Map<String, Integer> knnRanks,
            Map<String, Integer> bm25Ranks
    ) {}

    /** Holds the reranked selection together with per-chunk cross-encoder scores. */
    private record RerankResult(
            List<KnnSearchClient.ChunkHit> selected,
            Map<String, Double>  rerankerScores
    ) {}

    /**
     * @param question  user's natural-language query
     * @param ctx       tenant context
     * @param customer  optional customer ID — if set, customer-specific logs + deployment
     *                  state are added to the context alongside shared knowledge
     * @param product   optional product filter — narrows retrieval to one product's docs
     * @param env       optional env filter — prod / staging / dev
     */
    public RagResult answer(String question, TenantContext ctx,
                            String customer, String product, String env,
                            String sessionId) throws IOException {
        return answer(question, ctx, customer, product, env, sessionId, false);
    }

    public RagResult answer(String question, TenantContext ctx,
                            String customer, String product, String env,
                            String sessionId, boolean ragOnly) throws IOException {

        // ── Primary path: intelligence agent chat (ALL queries when enabled) ────
        // ragOnly=true skips this path — used when the intelligence agent itself calls
        // search_knowledge to avoid an infinite routing loop:
        //   agent → gateway → search-api → intelligenceAgent.chat() → agent → ...
        if (!ragOnly && intelligenceAgent.isEnabled()) {
            try {
                IntelligenceAgentClient.AgentChatResult chat =
                        intelligenceAgent.chat(question, sessionId, customer, env, product);
                if (chat != null) {
                    log.debug("Intelligence agent: ops={} clarify={} customer={} env={}",
                              chat.isOperational(), chat.needsClarification(),
                              chat.resolvedCustomer(), chat.resolvedEnv());
                    List<String> sources = chat.toolsCalled().stream()
                            .map(t -> "live:" + t).toList();
                    return new RagResult(
                            chat.answer(), sources,
                            chat.sessionId(), chat.needsClarification(),
                            chat.resolvedCustomer(), chat.resolvedEnv(),
                            chat.lifecycleStage(), chat.lifecycleLabel(),
                            chat.hasLiveData(), List.of());
                }
            } catch (Exception e) {
                log.warn("Intelligence agent error, falling back to static RAG: {}", e.getMessage());
            }
        }

        String chunkIndex = chunkIndexName(ctx);

        // 0. Metadata-aware retrieval: extract env/service/source filters from query text.
        //    Explicit caller params (customer, product, env) take precedence over extracted ones.
        QueryFilters qf = metadataExtractor.extract(question);
        String effectiveEnv     = (env     != null && !env.isBlank())     ? env     : qf.env();
        String effectiveProduct = (product != null && !product.isBlank()) ? product : null;
        // service filter → stored in metadata.service in OpenSearch
        String effectiveService = qf.service();
        log.debug("Metadata filters: env={} service={} source={} (explicit env={} product={})",
                  effectiveEnv, effectiveService, qf.sourceType(), env, product);

        // 1. Rewrite query + embed original — run concurrently since they're independent.
        //    Short queries and Jira-key lookups skip rewrite: no LLM overhead, less drift.
        CompletableFuture<String> rewriteFuture = shouldSkipRewrite(question)
                ? CompletableFuture.completedFuture(question)
                : CompletableFuture.supplyAsync(() -> rewriteQuery(question), RETRIEVAL_POOL);
        CompletableFuture<List<Double>> origVecFuture =
                CompletableFuture.supplyAsync(() -> embedder.embedQuery(question), RETRIEVAL_POOL);

        String       rewrittenQuery = rewriteFuture.join();
        List<Double> origVec        = origVecFuture.join();
        boolean      queryChanged   = !rewrittenQuery.equals(question);
        log.debug("Query rewrite: [{}] -> [{}] (changed={})", question, rewrittenQuery, queryChanged);

        // 2. Embed rewritten (fast, ~25ms — do inline now that origVec + rewrite are done)
        List<Double> rewriteVec = queryChanged ? embedder.embedQuery(rewrittenQuery) : origVec;

        // 3 + 4. All retrieval legs in parallel — each is independent I/O against OpenSearch.
        //        With 6 legs × ~50ms sequential = ~300ms → parallel wall-clock ~50ms.
        final String  fi = chunkIndex;
        final String  ft = ctx.tenantId();
        final String  fc = customer, fp = effectiveProduct, fe = effectiveEnv, fs = effectiveService;
        final List<Double> fOrig = origVec, fRew = rewriteVec;

        // Base 4 legs search all indexed knowledge (Jira, Confluence, GitHub, logs).
        // They must NOT filter by customer/product/env — those fields only exist on live-ops
        // docs (deployment state, logs). Applying them here would silently exclude all
        // Jira and Confluence results when customer= is set.
        // The dedicated customer-specific legs (fCustKnn, fCustBm25) below handle
        // the strict-scoped live-ops search with 2× RRF weight.
        CompletableFuture<List<KnnSearchClient.ChunkHit>> fKnnOrig = origVec.isEmpty()
                ? CompletableFuture.completedFuture(List.of())
                : asyncRetrieval(() -> knnClient.searchWithService(fi, fOrig, ft, CANDIDATE_K, null, null, null, null));
        CompletableFuture<List<KnnSearchClient.ChunkHit>> fKnnRew = (!queryChanged || rewriteVec.isEmpty())
                ? CompletableFuture.completedFuture(List.of())
                : asyncRetrieval(() -> knnClient.searchWithService(fi, fRew, ft, CANDIDATE_K, null, null, null, null));
        CompletableFuture<List<KnnSearchClient.ChunkHit>> fBm25Orig =
                asyncRetrieval(() -> bm25SearchWithService(fi, question, ft, null, null, null, null));
        CompletableFuture<List<KnnSearchClient.ChunkHit>> fBm25Rew = queryChanged
                ? asyncRetrieval(() -> bm25SearchWithService(fi, rewrittenQuery, ft, null, null, null, null))
                : CompletableFuture.completedFuture(List.of());
        CompletableFuture<List<KnnSearchClient.ChunkHit>> fCustKnn =
                (customer != null && !customer.isBlank() && !origVec.isEmpty())
                ? asyncRetrieval(() -> knnClient.searchByCustomer(fi, fOrig, ft, fc, CANDIDATE_K))
                : CompletableFuture.completedFuture(List.of());
        CompletableFuture<List<KnnSearchClient.ChunkHit>> fCustBm25 =
                (customer != null && !customer.isBlank())
                ? asyncRetrieval(() -> bm25SearchByCustomer(fi, question, ft, fc))
                : CompletableFuture.completedFuture(List.of());

        CompletableFuture.allOf(fKnnOrig, fKnnRew, fBm25Orig, fBm25Rew, fCustKnn, fCustBm25).join();

        List<KnnSearchClient.ChunkHit> knnOrig       = fKnnOrig.join();
        List<KnnSearchClient.ChunkHit> knnRew         = fKnnRew.join();
        List<KnnSearchClient.ChunkHit> bm25Orig       = fBm25Orig.join();
        List<KnnSearchClient.ChunkHit> bm25Rew         = fBm25Rew.join();
        List<KnnSearchClient.ChunkHit> customerKnnHits  = fCustKnn.join();
        List<KnnSearchClient.ChunkHit> customerBm25Hits = fCustBm25.join();

        // 5. RRF merge all 6 lists — customer chunks 2×, rewrite lists 1× (additive recall)
        MergeResult merged = rrfMerge(
                knnOrig, knnRew, bm25Orig, bm25Rew, customerKnnHits, customerBm25Hits);

        // 6. Cross-encoder rerank: take top RERANK_CANDIDATES from RRF, rerank, keep top CONTEXT_CHUNKS
        List<KnnSearchClient.ChunkHit> rerankCandidates = merged.hits().stream()
                .limit(RERANK_CANDIDATES)
                .toList();
        RerankResult rerankResult = rerank(question, rerankCandidates);
        List<KnnSearchClient.ChunkHit> topChunks = rerankResult.selected();

        if (topChunks.isEmpty()) {
            return new RagResult(
                "No relevant information found. Try rephrasing or check if the data has been indexed.",
                List.of());
        }

        // 7. Build per-chunk retrieval traces (P3.2)
        Map<String, Integer> finalRankMap = new java.util.HashMap<>();
        for (int i = 0; i < topChunks.size(); i++) {
            finalRankMap.put(topChunks.get(i).chunkId(), i + 1);
        }
        List<DocumentDto.RetrievalTrace> traces = new ArrayList<>();
        for (int i = 0; i < rerankCandidates.size(); i++) {
            KnnSearchClient.ChunkHit h = rerankCandidates.get(i);
            String cid = h.chunkId();
            String src = h.metadata() != null
                    ? String.valueOf(h.metadata().getOrDefault("source", "")) : "";
            traces.add(new DocumentDto.RetrievalTrace(
                    cid,
                    h.docId(),
                    src,
                    merged.knnRanks().get(cid),
                    merged.bm25Ranks().get(cid),
                    merged.rrfScores().getOrDefault(cid, 0.0),
                    i + 1,
                    rerankResult.rerankerScores().get(cid),
                    finalRankMap.get(cid),
                    finalRankMap.containsKey(cid),
                    h.embeddingVersion()));
        }

        // 8. Build prompt with customer deployment context header
        String prompt = buildPrompt(question, topChunks, customer, product, env);
        log.debug("RAG prompt {} chars, customer={}", prompt.length(), customer);

        // 9. Generate
        String answer = ollama.generate(prompt);
        if (answer == null) {
            answer = "The answer generation service is temporarily unavailable. " +
                     "Showing retrieved context — check the sources.";
        }

        List<String> sources = topChunks.stream()
                .map(KnnSearchClient.ChunkHit::chunkId).distinct().toList();

        return new RagResult(answer.strip(), sources,
                null, false, null, null, null, null, false, traces);
    }

    // BM25 on shared knowledge (optional metadata filters)
    private List<KnnSearchClient.ChunkHit> bm25Search(String chunkIndex, String q,
            String tenantId, String customer, String product, String env) throws IOException {
        return bm25SearchWithService(chunkIndex, q, tenantId, customer, product, env, null);
    }

    private List<KnnSearchClient.ChunkHit> bm25SearchWithService(String chunkIndex, String q,
            String tenantId, String customer, String product, String env,
            String service) throws IOException {
        return bm25Internal(chunkIndex, q, tenantId,
                buildMetadataFilters(tenantId, customer, product, env, service));
    }

    // BM25 scoped strictly to one customer's logs + deployment docs
    @SuppressWarnings("unchecked")
    private List<KnnSearchClient.ChunkHit> bm25SearchByCustomer(String chunkIndex, String q,
            String tenantId, String customer) throws IOException {
        Query tenantFilter = termQuery("tenant_id", tenantId);
        Query customerFilter = termQuery("metadata.customer", customer);
        Query combined = Query.of(b -> b.bool(bool -> bool
                .must(Query.of(m -> m.match(mm -> mm.field("chunk_text")
                        .query(v -> v.stringValue(q)).fuzziness("AUTO"))))
                .filter(List.of(tenantFilter, customerFilter))));
        return runBm25(chunkIndex, tenantId, combined);
    }

    @SuppressWarnings("unchecked")
    private List<KnnSearchClient.ChunkHit> bm25Internal(String chunkIndex, String q,
            String tenantId, List<Query> filters) throws IOException {
        Query textQuery = Query.of(b -> b.match(m -> m.field("chunk_text")
                .query(v -> v.stringValue(q)).fuzziness("AUTO")));
        Query combined = Query.of(b -> b.bool(bool -> bool.must(textQuery).filter(filters)));
        return runBm25(chunkIndex, tenantId, combined);
    }

    @SuppressWarnings("unchecked")
    private List<KnnSearchClient.ChunkHit> runBm25(String chunkIndex,
            String tenantId, Query combined) throws IOException {
        SearchRequest req = SearchRequest.of(s -> s
                .index(chunkIndex).routing(tenantId).query(combined).size(CANDIDATE_K));
        log.debug("runBm25: index={} routing={}", chunkIndex, tenantId);
        try {
            SearchResponse<Map> res = os.search(req, Map.class);
            List<KnnSearchClient.ChunkHit> hits = new ArrayList<>();
            for (Hit<Map> h : res.hits().hits()) {
                Map<String, Object> src = h.source() != null ? h.source() : new HashMap<>();
                hits.add(new KnnSearchClient.ChunkHit(
                        h.id(),
                        (String) src.get("doc_id"),
                        (String) src.getOrDefault("title", ""),
                        (String) src.getOrDefault("chunk_text", ""),
                        (int) src.getOrDefault("chunk_index", 0),
                        (Map<String, Object>) src.getOrDefault("metadata", Map.of()),
                        (String) src.get("embedding_version")));
            }
            return hits;
        } catch (org.opensearch.client.opensearch._types.OpenSearchException e) {
            log.error("BM25 OpenSearch error on index={}: {}", chunkIndex, e.getMessage());
            if (e.error() != null && e.error().causedBy() != null)
                log.error("BM25 caused by: {}", e.error().causedBy().reason());
            if (e.getMessage() != null && e.getMessage().contains("index_not_found"))
                return List.of();
            throw e;
        }
    }

    private List<Query> buildMetadataFilters(String tenantId, String customer,
                                              String product, String env, String service) {
        List<Query> filters = new ArrayList<>();
        filters.add(termQuery("tenant_id", tenantId));
        if (customer != null && !customer.isBlank())
            filters.add(termQuery("metadata.customer", customer));
        if (product != null && !product.isBlank())
            filters.add(termQuery("metadata.product", product));
        if (env != null && !env.isBlank())
            filters.add(termQuery("metadata.env", env));
        if (service != null && !service.isBlank())
            filters.add(termQuery("metadata.service", service));
        return filters;
    }

    private Query termQuery(String field, String value) {
        return Query.of(b -> b.term(t -> t.field(field).value(v -> v.stringValue(value))));
    }

    /**
     * 6-list RRF merge.
     * Authority weighting encoded via SourceAuthority — the LLM never decides authority.
     * Rewrite lists get weight 0.7 (additive recall). Customer chunks get 2× (most relevant).
     * Returns a MergeResult carrying the sorted list + per-chunk rank data for P3.2 tracing.
     */
    private MergeResult rrfMerge(
            List<KnnSearchClient.ChunkHit> knnOrig,
            List<KnnSearchClient.ChunkHit> knnRew,
            List<KnnSearchClient.ChunkHit> bm25Orig,
            List<KnnSearchClient.ChunkHit> bm25Rew,
            List<KnnSearchClient.ChunkHit> custKnn,
            List<KnnSearchClient.ChunkHit> custBm25) {

        Map<String, Double> scores = new LinkedHashMap<>();
        Map<String, KnnSearchClient.ChunkHit> byId = new LinkedHashMap<>();
        Map<String, Integer> knnRanks  = new HashMap<>();
        Map<String, Integer> bm25Ranks = new HashMap<>();

        addToRrf(knnOrig,  1.0, scores, byId);  trackRanks(knnOrig,  knnRanks);
        addToRrf(knnRew,   0.7, scores, byId);  trackRanks(knnRew,   knnRanks);
        addToRrf(bm25Orig, 1.0, scores, byId);  trackRanks(bm25Orig, bm25Ranks);
        addToRrf(bm25Rew,  0.7, scores, byId);  trackRanks(bm25Rew,  bm25Ranks);
        addToRrf(custKnn,  2.0, scores, byId);  trackRanks(custKnn,  knnRanks);
        addToRrf(custBm25, 2.0, scores, byId);  trackRanks(custBm25, bm25Ranks);

        List<KnnSearchClient.ChunkHit> sorted = scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder()))
                .map(e -> byId.get(e.getKey()))
                .toList();
        return new MergeResult(sorted, scores, knnRanks, bm25Ranks);
    }

    /** Track best (lowest) rank for each chunk across multiple retrieval legs. */
    private void trackRanks(List<KnnSearchClient.ChunkHit> hits, Map<String, Integer> ranks) {
        for (int i = 0; i < hits.size(); i++) {
            ranks.merge(hits.get(i).chunkId(), i + 1, Math::min);
        }
    }

    private void addToRrf(List<KnnSearchClient.ChunkHit> hits, double weight,
                          Map<String, Double> scores, Map<String, KnnSearchClient.ChunkHit> byId) {
        for (int i = 0; i < hits.size(); i++) {
            KnnSearchClient.ChunkHit h = hits.get(i);
            String source = h.metadata() != null
                    ? (String) h.metadata().getOrDefault("source", "") : "";
            double authorityBoost = SourceAuthority.forSource(source).normalizedWeight();
            scores.merge(h.chunkId(), weight * authorityBoost / (RRF_K + i + 1), Double::sum);
            byId.putIfAbsent(h.chunkId(), h);
        }
    }

    /** Wrap a checked-exception supplier for use with CompletableFuture. */
    @FunctionalInterface
    private interface RetrievalTask { List<KnnSearchClient.ChunkHit> call() throws Exception; }

    private static CompletableFuture<List<KnnSearchClient.ChunkHit>> asyncRetrieval(RetrievalTask task) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return task.call();
            } catch (Exception e) {
                log.warn("Async retrieval leg failed: {}", e.getMessage());
                return List.of();
            }
        }, RETRIEVAL_POOL);
    }

    /** Only skip rewrite when the entire query is a bare Jira key — an exact-ID lookup. */
    private static boolean shouldSkipRewrite(String question) {
        return JIRA_KEY_ONLY_RE.matcher(question.trim()).matches();
    }

    /**
     * Query rewriting: use LLM to expand the query with synonyms, acronyms, and related terms.
     * Falls back to the original query if Ollama is unavailable or the response is malformed.
     */
    private String rewriteQuery(String question) {
        String prompt = "Rewrite the following search query to improve document retrieval. " +
                "Add synonyms, expand abbreviations, and include related technical terms. " +
                "Output ONLY the rewritten query — no explanation, no quotes, no punctuation changes.\n\n" +
                "Query: " + question + "\n\nRewritten:";
        try {
            String rewritten = ollama.generate(prompt);
            if (rewritten != null && !rewritten.isBlank() && rewritten.length() < question.length() * 5) {
                return rewritten.strip();
            }
        } catch (Exception e) {
            log.debug("Query rewrite failed: {}", e.getMessage());
        }
        return question;
    }

    // Source budget: max slots per source type in the final context window
    private static final Map<String, Integer> SOURCE_BUDGET = Map.of(
            "warehouse_logs",  2,
            "deployment_state", 1,
            "jira",            1,
            "git",             1,
            "confluence",      1
    );
    private static final int DEFAULT_SOURCE_BUDGET = 1;

    /**
     * Cross-encoder reranking + source-balanced context budgeting.
     * Pipeline: score all candidates → sort descending → pick greedily respecting SOURCE_BUDGET.
     * Falls back to RRF order if reranker is unavailable.
     * Returns a RerankResult carrying the selected chunks and per-chunk reranker scores for tracing.
     */
    private RerankResult rerank(String question, List<KnnSearchClient.ChunkHit> candidates) {
        if (candidates.isEmpty()) return new RerankResult(candidates, Map.of());
        List<String> passages = candidates.stream()
                .map(c -> (c.title().isBlank() ? "" : c.title() + "\n") + c.chunkText())
                .toList();
        List<Double> rawScores = reranker.rerank(question, passages);

        // Build chunk_id → reranker score map for traces
        Map<String, Double> rerankerScores = new HashMap<>();
        if (!rawScores.isEmpty() && rawScores.size() == candidates.size()) {
            for (int i = 0; i < candidates.size(); i++) {
                rerankerScores.put(candidates.get(i).chunkId(), rawScores.get(i));
            }
        }

        List<KnnSearchClient.ChunkHit> sorted;
        if (rawScores.isEmpty() || rawScores.size() != candidates.size()) {
            sorted = candidates; // RRF order as fallback
        } else {
            List<int[]> indices = new ArrayList<>();
            for (int i = 0; i < candidates.size(); i++) indices.add(new int[]{i});
            indices.sort((a, b) -> Double.compare(rawScores.get(b[0]), rawScores.get(a[0])));
            sorted = indices.stream().map(idx -> candidates.get(idx[0])).toList();
        }

        // Source-balanced selection: pick highest-scoring chunks while respecting per-source caps
        Map<String, Integer> used = new java.util.HashMap<>();
        List<KnnSearchClient.ChunkHit> selected = new ArrayList<>();
        for (KnnSearchClient.ChunkHit hit : sorted) {
            if (selected.size() >= CONTEXT_CHUNKS) break;
            String src = hit.metadata() != null
                    ? String.valueOf(hit.metadata().getOrDefault("source", "")) : "";
            int cap = SOURCE_BUDGET.getOrDefault(src, DEFAULT_SOURCE_BUDGET);
            int count = used.getOrDefault(src, 0);
            if (count < cap) {
                selected.add(hit);
                used.put(src, count + 1);
            }
        }
        // If budget constraints left us short, fill with next best regardless of source
        if (selected.size() < CONTEXT_CHUNKS) {
            for (KnnSearchClient.ChunkHit hit : sorted) {
                if (selected.size() >= CONTEXT_CHUNKS) break;
                if (!selected.contains(hit)) selected.add(hit);
            }
        }
        return new RerankResult(selected, rerankerScores);
    }

    private String buildPrompt(String question, List<KnnSearchClient.ChunkHit> chunks,
                                String customer, String product, String env) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a Searchly intelligence assistant.\n");
        sb.append("Answer the question using ONLY the context provided below.\n");
        sb.append("Cite document titles and Jira issue keys when referencing them.\n");
        sb.append("If the context is insufficient, say so clearly.\n\n");

        // Chunks are pre-ranked by authority (LIVE_LOGS > DEPLOYMENT > CODE > JIRA > CONFLUENCE)
        // via RRF * SourceAuthority weighting — authority is in the retrieval score, not here.

        // Customer deployment header — this is the key differentiator
        if (customer != null && !customer.isBlank()) {
            sb.append("CUSTOMER CONTEXT:\n");
            sb.append("  Customer ID : ").append(customer).append("\n");
            if (product != null && !product.isBlank())
                sb.append("  Product     : ").append(product).append("\n");
            if (env != null && !env.isBlank())
                sb.append("  Environment : ").append(env).append("\n");
            sb.append("  (Live logs and deployment state for this customer are included below)\n\n");
        }

        sb.append("CONTEXT:\n");
        for (int i = 0; i < chunks.size(); i++) {
            KnnSearchClient.ChunkHit c = chunks.get(i);
            Map<String, Object> meta = c.metadata();
            String src = meta != null ? String.valueOf(meta.getOrDefault("source", "")) : "";
            String label = src.equals("warehouse_logs") ? "🔴 LIVE LOGS" :
                           src.equals("deployment_state") ? "📦 DEPLOYMENT" :
                           src.equals("jira") ? "🎫 JIRA" :
                           src.equals("git") ? "💻 CODE" : "📄 DOCS";
            sb.append("--- [").append(i + 1).append("] ").append(label)
              .append(" | ").append(c.title()).append(" ---\n");
            sb.append(c.chunkText()).append("\n\n");
        }

        sb.append("QUESTION: ").append(question).append("\n\nANSWER:");
        return sb.toString();
    }

    private String chunkIndexName(TenantContext ctx) {
        return ctx.tier().name().equals("ENTERPRISE")
                ? "chunks-" + ctx.tenantId()
                : "chunks-shared";
    }
}
