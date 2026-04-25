package pl.ldz.microsrv.order.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import pl.ldz.microsrv.common.event.OrderStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
public class Order {

  @Id
  @Column(name = "id", nullable = false, updatable = false)
  private UUID id;

  // debugId is DB-owned via GENERATED ALWAYS AS IDENTITY.
  // insertable = false, updatable = false prevents JPA from ever writing to this column.
  // @Setter on the class generates a setter, but the JPA layer enforces the guard.
  @Column(name = "debug_id", insertable = false, updatable = false)
  private Long debugId;

  // @Column(name = "customer_id", nullable = false, updatable = false)
  @Column(name = "customer_id", nullable = true)
  private UUID customerId;

  // EnumType.STRING stores the enum name (e.g. "CREATED"), not ordinal.
  // Ordinal storage breaks silently if enum values are reordered.
  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private OrderStatus status;

  // BigDecimal maps to NUMERIC(19,2) exactly. Never use double or float for monetary values.
  @Column(name = "total_amount", nullable = false, precision = 19, scale = 2)
  private BigDecimal totalAmount;

  // OffsetDateTime maps to TIMESTAMPTZ natively in Hibernate 6. Never use LocalDateTime.
  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "updated_at", nullable = false)
  private OffsetDateTime updatedAt;

  @Column(name = "deleted_at")
  private OffsetDateTime deletedAt;

  // @Version enables JPA optimistic locking. Maps to version BIGINT NOT NULL DEFAULT 0 in V1.
  @Version
  @Column(name = "version")
  private Long version;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Order other)) return false;
    return id != null && id.equals(other.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public String toString() {
    return "Order{id=" + id + ", debugId=" + debugId + ", status=" + status + "}";
  }
}
