package dev.searchly.api.controller;

import dev.searchly.api.service.SearchService;
import dev.searchly.common.DocumentDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

/**
 * Single hybrid search endpoint — BM25 + k-NN semantic + LLM + live operational data.
 *
 * Customer resolution is fully automatic — pass any spelling of the customer name.
 * The system fuzzy-matches it and, if uncertain, returns a clarification question
 * in the answer field (needs_clarification=true).  Send the user's reply back with
 * the same session_id to continue the conversation.
 *
 * Examples:
 *   # Knowledge query (no customer)
 *   GET /api/v1/search?q=how+does+hungarian+allocation+work
 *
 *   # Operational query — any customer name spelling works
 *   GET /api/v1/search?q=why+is+robot+not+coming&customer=samsclub+atl
 *   GET /api/v1/search?q=operator+allocation+failing&customer=sam%27s+club+atlanta&env=prod
 *
 *   # Multi-turn (pass session back)
 *   GET /api/v1/search?q=what+about+staging&session=<id_from_prior_response>
 *
 *   # New customer — system auto-registers through clarification dialog
 *   GET /api/v1/search?q=anything&customer=unknown+walmart+site
 *   → needs_clarification=true, answer asks which products they use
 *   GET /api/v1/search?q=pick-assist+and+greymatter&session=<id>
 *   → customer auto-registered at solution stage, knowledge answer returned
 */
@RestController
@RequestMapping("/api/v1/search")
public class SearchController {

    private final SearchService search;

    public SearchController(SearchService search) {
        this.search = search;
    }

    @GetMapping
    public DocumentDto.SearchResponse search(
            @RequestParam("q")                                          String       q,
            @RequestParam(value = "page",      defaultValue = "0")     int          page,
            @RequestParam(value = "size",      defaultValue = "20")    int          size,
            @RequestParam(value = "fuzzy",     defaultValue = "false")  boolean      fuzzy,
            @RequestParam(value = "highlight", defaultValue = "true")   boolean      highlight,
            @RequestParam(value = "facets",    required = false)        List<String> facets,
            @RequestParam(value = "customer",  required = false)        String       customer,
            @RequestParam(value = "product",   required = false)        String       product,
            @RequestParam(value = "env",       required = false)        String       env,
            @RequestParam(value = "session",   required = false)        String       sessionId,
            @RequestParam(value = "cursor",    required = false)        String       cursor
    ) throws IOException {
        if (size > 100) size = 100;
        return search.search(q, page, size, fuzzy, highlight, facets,
                             customer, product, env, sessionId, cursor);
    }
}
