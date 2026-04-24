package pl.ldz.microsrv.order.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import pl.ldz.microsrv.order.entity.Order;

import java.util.Optional;
import java.util.UUID;

/**
 * Repository for {@link Order} entities.
 *
 * <p>All query methods are soft-delete-aware: they filter rows where {@code deleted_at IS NULL}.
 * No native SQL is used — method names map directly to the indexed columns
 * ({@code customer_id}, {@code deleted_at}) created by Flyway migration V4.
 */
public interface OrderRepository extends JpaRepository<Order, UUID> {

  /**
   * Returns a page of non-deleted orders belonging to the given customer.
   * Used by {@code GET /api/v1/orders?customerId=...}.
   */
  Page<Order> findByCustomerIdAndDeletedAtIsNull(UUID customerId, Pageable pageable);

  /**
   * Returns a page of all non-deleted orders.
   * Used by {@code GET /api/v1/orders} when no customerId filter is supplied.
   */
  Page<Order> findByDeletedAtIsNull(Pageable pageable);

  /**
   * Fetches a single non-deleted order by its external UUID.
   * Used by all single-record operations (GET, PUT, DELETE by id).
   */
  Optional<Order> findByIdAndDeletedAtIsNull(UUID id);
}
