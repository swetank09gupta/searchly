package dev.searchly.api.controller;

import dev.searchly.api.service.SearchService;
import dev.searchly.common.DocumentDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/v1/search")
public class SearchController {

    private final SearchService search;

    public SearchController(SearchService search) {
        this.search = search;
    }

    @GetMapping
    public DocumentDto.SearchResponse search(
            @RequestParam("q") String q,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size,
            @RequestParam(value = "fuzzy", defaultValue = "false") boolean fuzzy,
            @RequestParam(value = "highlight", defaultValue = "true") boolean highlight,
            @RequestParam(value = "facets", required = false) List<String> facets) throws IOException {
        if (size > 100) size = 100;
        return search.search(q, page, size, fuzzy, highlight, facets);
    }
}
