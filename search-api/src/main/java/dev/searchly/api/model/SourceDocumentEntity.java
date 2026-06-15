package dev.searchly.api.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "source_documents")
public class SourceDocumentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source_id", nullable = false, length = 512)
    private String sourceId;

    @Column(name = "source_type", nullable = false, length = 50)
    private String sourceType;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "doc_id", length = 255)
    private String docId;

    @Column(name = "last_seen_at", nullable = false)
    private Instant lastSeenAt;

    public SourceDocumentEntity() {}

    public SourceDocumentEntity(String sourceId, String sourceType,
                                 String tenantId, String docId) {
        this.sourceId    = sourceId;
        this.sourceType  = sourceType;
        this.tenantId    = tenantId;
        this.docId       = docId;
        this.lastSeenAt  = Instant.now();
    }

    public Long    getId()          { return id; }
    public String  getSourceId()    { return sourceId; }
    public String  getSourceType()  { return sourceType; }
    public String  getTenantId()    { return tenantId; }
    public String  getDocId()       { return docId; }
    public Instant getLastSeenAt()  { return lastSeenAt; }

    public void setLastSeenAt(Instant lastSeenAt) { this.lastSeenAt = lastSeenAt; }
    public void setDocId(String docId)             { this.docId = docId; }
}
