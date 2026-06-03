package dev.searchly.api.controller;

import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.api.service.DocumentService;
import dev.searchly.common.DocumentDto;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/documents")
public class DocumentController {

    private final DocumentService service;

    public DocumentController(DocumentService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<DocumentDto.CreateResponse> create(
            @Valid @RequestBody DocumentDto.CreateRequest req,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        requireRole("EDITOR", "TENANT_ADMIN");
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(service.create(req, idempotencyKey));
    }

    @GetMapping("/{id}")
    public DocumentDto.DocumentView get(@PathVariable UUID id) {
        return service.get(id);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) {
        requireRole("EDITOR", "TENANT_ADMIN");
        service.delete(id);
    }

    private static void requireRole(String... allowed) {
        var ctx = TenantContextHolder.require();
        for (String r : allowed) if (ctx.hasRole(r)) return;
        throw new SecurityException("Insufficient role; need one of " + java.util.Arrays.toString(allowed));
    }
}
