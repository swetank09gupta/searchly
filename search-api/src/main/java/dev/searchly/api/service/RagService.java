package dev.searchly.api.service;

import dev.searchly.api.client.EmbeddingClient;
import dev.searchly.api.client.KnnSearchClient;
import dev.searchly.api.client.OllamaClient;
import dev.searchly.api.client.WarehouseAgentClient;
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
 *   "Customer sams-club-atlanta is running GreyMatter v6.0.5 + OGA v2.3.1 (prod).
 *    Their operator-backend pod has 3 ERRORs in the last 10 min related to Redis.
 *    AES-891 is a known bug for this version, fixed in v2.3.2."
 */
@Service
public class RagService {
    private static final Logger log = LoggerFactory.getLogger(RagService.class);

    private static final int CANDIDATE_K = 20;
    private static final int CONTEXT_CHUNKS = 6;
    private static final int RRF_K = 60;

    private final EmbeddingClient embedder;
    private final KnnSearchClient knnClient;
    private final OllamaClient ollama;
    private final OpenSearchClient os;
    private final WarehouseAgentClient warehouseAgent;

    public RagService(EmbeddingClient embedder, KnnSearchClient knnClient,
                      OllamaClient ollama, OpenSearchClient os,
                      WarehouseAgentClient warehouseAgent) {
        this.embedder = embedder;
        this.knnClient = knnClient;
        this.ollama = ollama;
        this.os = os;
        this.warehouseAgent = warehouseAgent;
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
            boolean hasLiveData
    ) {
        // Backward-compat: static RAG result with no conversational fields
        public RagResult(String answer, List<String> sources) {
            this(answer, sources, null, false, null, null, null, null, false);
        }
    }

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

        // ── Primary path: warehouse agent chat (ALL queries when enabled) ────
        // The agent handles everything:
        //   - Fuzzy customer name resolution ("samsclub atl" → "sams-club-atlanta")
        //   - Env extraction from the question ("in prod" → env=prod)
        //   - Clarification dialog (unknown customer → auto-registers through chat)
        //   - Live cluster data for configured envs
        //   - Static RAG knowledge for solution-phase or no-cluster queries
        //
        // The agent replaces static RAG entirely when available.
        // Fall back to static RAG below only if the agent service is down.
        if (warehouseAgent.isEnabled()) {
            try {
                WarehouseAgentClient.AgentChatResult chat =
                        warehouseAgent.chat(question, sessionId, customer, env, product);
                if (chat != null) {
                    log.debug("Warehouse agent: ops={} clarify={} customer={} env={}",
                              chat.isOperational(), chat.needsClarification(),
                              chat.resolvedCustomer(), chat.resolvedEnv());
                    List<String> sources = chat.toolsCalled().stream()
                            .map(t -> "live:" + t).toList();
                    return new RagResult(
                            chat.answer(), sources,
                            chat.sessionId(), chat.needsClarification(),
                            chat.resolvedCustomer(), chat.resolvedEnv(),
                            chat.lifecycleStage(), chat.lifecycleLabel(),
                            chat.hasLiveData());
                }
            } catch (Exception e) {
                log.warn("Warehouse agent error, falling back to static RAG: {}", e.getMessage());
            }
        }

        String chunkIndex = chunkIndexName(ctx);

        // 1. Embed query
        List<Double> queryVec = embedder.embed(question);

        // 2. Shared knowledge — k-NN + BM25 on chunks index
        List<KnnSearchClient.ChunkHit> knnHits = queryVec.isEmpty() ? List.of()
                : knnClient.search(chunkIndex, queryVec, ctx.tenantId(), CANDIDATE_K,
                                   customer, product, env);

        List<KnnSearchClient.ChunkHit> bm25Hits =
                bm25Search(chunkIndex, question, ctx.tenantId(), customer, product, env);

        // 3. Customer-specific chunks (logs + deployment state), if customer specified
        List<KnnSearchClient.ChunkHit> customerKnnHits = List.of();
        List<KnnSearchClient.ChunkHit> customerBm25Hits = List.of();
        if (customer != null && !customer.isBlank()) {
            customerKnnHits = queryVec.isEmpty() ? List.of()
                    : knnClient.searchByCustomer(chunkIndex, queryVec, ctx.tenantId(),
                                                 customer, CANDIDATE_K);
            customerBm25Hits = bm25SearchByCustomer(chunkIndex, question,
                                                     ctx.tenantId(), customer);
        }

        // 4. RRF merge — customer chunks weighted 2× (they're the most specific context)
        List<KnnSearchClient.ChunkHit> merged = rrfMerge(
                knnHits, bm25Hits, customerKnnHits, customerBm25Hits);

        List<KnnSearchClient.ChunkHit> topChunks = merged.stream()
                .limit(CONTEXT_CHUNKS)
                .toList();

        if (topChunks.isEmpty()) {
            return new RagResult(
                "No relevant information found. Try rephrasing or check if the data has been indexed.",
                List.of());
        }

        // 5. Build prompt with customer deployment context header
        String prompt = buildPrompt(question, topChunks, customer, product, env);
        log.debug("RAG prompt {} chars, customer={}", prompt.length(), customer);

        // 6. Generate
        String answer = ollama.generate(prompt);
        if (answer == null) {
            answer = "The answer generation service is temporarily unavailable. " +
                     "Showing retrieved context — check the sources.";
        }

        List<String> sources = topChunks.stream()
                .map(KnnSearchClient.ChunkHit::chunkId).distinct().toList();

        return new RagResult(answer.strip(), sources);
    }

    // BM25 on shared knowledge (optional metadata filters)
    @SuppressWarnings("unchecked")
    private List<KnnSearchClient.ChunkHit> bm25Search(String chunkIndex, String q,
            String tenantId, String customer, String product, String env) throws IOException {
        return bm25Internal(chunkIndex, q, tenantId,
                buildMetadataFilters(tenantId, customer, product, env));
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
                        (Map<String, Object>) src.getOrDefault("metadata", Map.of())));
            }
            return hits;
        } catch (org.opensearch.client.opensearch._types.OpenSearchException e) {
            if (e.getMessage() != null && e.getMessage().contains("index_not_found"))
                return List.of();
            throw e;
        }
    }

    private List<Query> buildMetadataFilters(String tenantId, String customer,
                                              String product, String env) {
        List<Query> filters = new ArrayList<>();
        filters.add(termQuery("tenant_id", tenantId));
        if (customer != null && !customer.isBlank())
            filters.add(termQuery("metadata.customer", customer));
        if (product != null && !product.isBlank())
            filters.add(termQuery("metadata.product", product));
        if (env != null && !env.isBlank())
            filters.add(termQuery("metadata.env", env));
        return filters;
    }

    private Query termQuery(String field, String value) {
        return Query.of(b -> b.term(t -> t.field(field).value(v -> v.stringValue(value))));
    }

    /**
     * RRF merge — customer-specific chunks (logs/deployment) get 2× weight
     * because they're the most directly relevant to the warehouse situation.
     */
    private List<KnnSearchClient.ChunkHit> rrfMerge(
            List<KnnSearchClient.ChunkHit> knn,
            List<KnnSearchClient.ChunkHit> bm25,
            List<KnnSearchClient.ChunkHit> custKnn,
            List<KnnSearchClient.ChunkHit> custBm25) {

        Map<String, Double> scores = new LinkedHashMap<>();
        Map<String, KnnSearchClient.ChunkHit> byId = new LinkedHashMap<>();

        addToRrf(knn, 1.0, scores, byId);
        addToRrf(bm25, 1.0, scores, byId);
        addToRrf(custKnn, 2.0, scores, byId);   // customer logs get double weight
        addToRrf(custBm25, 2.0, scores, byId);

        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder()))
                .map(e -> byId.get(e.getKey()))
                .toList();
    }

    private void addToRrf(List<KnnSearchClient.ChunkHit> hits, double weight,
                          Map<String, Double> scores, Map<String, KnnSearchClient.ChunkHit> byId) {
        for (int i = 0; i < hits.size(); i++) {
            KnnSearchClient.ChunkHit h = hits.get(i);
            scores.merge(h.chunkId(), weight / (RRF_K + i + 1), Double::sum);
            byId.putIfAbsent(h.chunkId(), h);
        }
    }

    private String buildPrompt(String question, List<KnnSearchClient.ChunkHit> chunks,
                                String customer, String product, String env) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a GreyOrange warehouse intelligence assistant.\n");
        sb.append("Answer the question using ONLY the context provided below.\n");
        sb.append("Cite document titles and Jira issue keys when referencing them.\n");
        sb.append("If the context is insufficient, say so clearly.\n\n");

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
