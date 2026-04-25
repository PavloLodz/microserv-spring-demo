package pl.ldz.microsrv.order.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "idempotency_key")
@Getter
@Setter
@NoArgsConstructor
public class IdempotencyKey {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    // unique = true mirrors the UNIQUE constraint in V3 migration.
    // Both are needed: the DB constraint is the safety net against concurrent race conditions.
    @Column(name = "key", nullable = false, length = 255, unique = true)
    private String key;

    // 64 chars is the exact size of a SHA-256 hex digest (32 bytes × 2 hex chars).
    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    // Nullable; NULL while status = IN_PROGRESS, populated when status = COMPLETED.
    // columnDefinition = "jsonb" prevents ddl-auto=validate type mismatch.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_body", columnDefinition = "jsonb")
    private String responseBody;

    // String not enum for flexibility. Values: IN_PROGRESS, COMPLETED.
    @Column(name = "status", nullable = false, length = 50)
    private String status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    // Non-nullable: every record must have a defined TTL for the cleanup job (Phase 10).
    @Column(name = "expires_at", nullable = false)
    private OffsetDateTime expiresAt;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdempotencyKey other)) return false;
        return id != null && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "IdempotencyKey{id=" + id + ", key=" + key + ", status=" + status + "}";
    }
}
