package pl.ldz.microsrv.order.service;

import com.fasterxml.uuid.Generators;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.microsrv.common.event.OrderStatus;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;
import pl.ldz.microsrv.order.entity.Order;
import pl.ldz.microsrv.order.exception.OrderNotFoundException;
import pl.ldz.microsrv.order.mapper.OrderMapper;
import pl.ldz.microsrv.order.repository.OrderRepository;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Business logic for Order CRUD operations.
 *
 * <p>This service is the sole owner of all lifecycle fields on {@link Order}:
 * {@code id}, {@code status}, {@code createdAt}, {@code updatedAt}, {@code deletedAt}.
 * The mapper never writes these; the controller never reads the entity directly.
 *
 * <p>No Kafka, no outbox, no idempotency in this phase — those are deferred.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

  private final OrderRepository orderRepository;
  private final OrderMapper orderMapper;

  // ── Create ────────────────────────────────────────────────────────────────

  /**
   * Creates a new order.
   *
   * <ol>
   *   <li>Generates a time-ordered UUIDv7 for the external {@code id}.
   *   <li>Maps the request DTO to an entity (lifecycle fields are ignored by the mapper).
   *   <li>Sets all lifecycle fields explicitly.
   *   <li>Persists and returns the mapped response DTO.
   * </ol>
   */
  @Transactional
  public OrderResponse createOrder(OrderRequest request, String idempotencyKey) {
    UUID id = Generators.timeBasedEpochGenerator().generate();

    Order order = orderMapper.toEntity(request);
    order.setId(id);
    order.setStatus(OrderStatus.CREATED);
    order.setCreatedAt(OffsetDateTime.now());
    order.setUpdatedAt(OffsetDateTime.now());

    Order saved = orderRepository.save(order);
    log.info("Order created: id={}", saved.getId());

    return orderMapper.toResponse(saved);
  }

  // ── Read (single) ─────────────────────────────────────────────────────────

  /**
   * Returns a single non-deleted order by its external UUID.
   *
   * @throws OrderNotFoundException if no live order with {@code id} exists
   */
  @Transactional(readOnly = true)
  public OrderResponse getOrder(UUID id) {
    Order order = orderRepository.findByIdAndDeletedAtIsNull(id)
        .orElseThrow(() -> new OrderNotFoundException(id));
    return orderMapper.toResponse(order);
  }

  // ── Read (list) ───────────────────────────────────────────────────────────

  /**
   * Returns a page of non-deleted orders, optionally filtered by {@code customerId}.
   *
   * <p>When {@code customerId} is {@code null} all non-deleted orders are returned;
   * otherwise only those belonging to the specified customer.
   */
  @Transactional(readOnly = true)
  public Page<OrderResponse> listOrders(UUID customerId, Pageable pageable) {
    Page<Order> page = (customerId != null)
        ? orderRepository.findByCustomerIdAndDeletedAtIsNull(customerId, pageable)
        : orderRepository.findByDeletedAtIsNull(pageable);

    return page.map(orderMapper::toResponse);
  }

  // ── Update ────────────────────────────────────────────────────────────────

  /**
   * Updates the mutable fields of an existing order.
   *
   * <p>Only {@code totalAmount} is accepted from the request body — {@code status} is
   * server-controlled and is never updated via a free-form PUT in this phase.
   * Concurrent modifications are handled by the {@code @Version} optimistic lock on the
   * entity; {@link org.springframework.orm.ObjectOptimisticLockingFailureException} is
   * mapped to HTTP 409 by {@link pl.ldz.microsrv.order.exception.GlobalExceptionHandler}.
   *
   * @throws OrderNotFoundException if no live order with {@code id} exists
   */
  @Transactional
  public OrderResponse updateOrder(UUID id, OrderRequest request, String idempotencyKey) {
    Order order = orderRepository.findByIdAndDeletedAtIsNull(id)
        .orElseThrow(() -> new OrderNotFoundException(id));

    order.setTotalAmount(request.getTotalAmount());
    order.setUpdatedAt(OffsetDateTime.now());

    Order saved = orderRepository.save(order);
    log.info("Order updated: id={}", saved.getId());

    return orderMapper.toResponse(saved);
  }

  // ── Delete (soft) ─────────────────────────────────────────────────────────

  /**
   * Soft-deletes an order by setting {@code deletedAt} to the current timestamp.
   * After this call the order is excluded from all repository query methods that
   * filter on {@code deletedAtIsNull}.
   *
   * @throws OrderNotFoundException if no live order with {@code id} exists
   */
  @Transactional
  public void deleteOrder(UUID id, String idempotencyKey) {
    Order order = orderRepository.findByIdAndDeletedAtIsNull(id)
        .orElseThrow(() -> new OrderNotFoundException(id));

    order.setDeletedAt(OffsetDateTime.now());
    order.setUpdatedAt(OffsetDateTime.now());

    orderRepository.save(order);
    log.info("Order soft-deleted: id={}", id);
  }
}
