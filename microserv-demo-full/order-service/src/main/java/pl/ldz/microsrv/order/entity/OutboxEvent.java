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
@Table(name = "outbox_event")
@Getter
@Setter
@NoArgsConstructor
public class OutboxEvent {

  @Id
  @Column(name = "id", nullable = false, updatable = false)
  private UUID id;

  @Column(name = "aggregate_type", nullable = false, length = 50)
  private String aggregateType;

  @Column(name = "aggregate_id", nullable = false, updatable = false)
  private UUID aggregateId;

  @Column(name = "event_type", nullable = false, length = 50)
  private String eventType;

  // Java type is String; columnDefinition = "jsonb" tells Hibernate the PostgreSQL column type
  // so ddl-auto=validate does not report a type mismatch.
  // @JdbcTypeCode(SqlTypes.JSON) instructs Hibernate to bind this parameter as JSON/JSONB,
  // preventing PostgreSQL from rejecting the implicit VARCHAR → JSONB cast.
  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
  private String payload;

  // String not enum: status values (PENDING, PROCESSED, FAILED) are managed by the outbox worker (Phase 6).
  @Column(name = "status", nullable = false, length = 20)
  private String status;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  // Nullable; set by the worker after successful Kafka publish (Phase 6).
  @Column(name = "processed_at")
  private OffsetDateTime processedAt;

  // No @Version field: the outbox worker performs a single-writer status transition
  // and does not need optimistic locking.

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OutboxEvent other)) return false;
    return id != null && id.equals(other.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public String toString() {
    return "OutboxEvent{id=" + id + ", eventType=" + eventType + ", status=" + status + "}";
  }
}
