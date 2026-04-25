package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.microsrv.common.event.OrderCreatedEvent;
import pl.ldz.microsrv.common.event.OrderDeletedEvent;
import pl.ldz.microsrv.common.event.OrderStatus;
import pl.ldz.microsrv.common.event.OrderUpdatedEvent;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;
import pl.ldz.microsrv.order.entity.Order;
import pl.ldz.microsrv.order.exception.OrderNotFoundException;
import pl.ldz.microsrv.order.mapper.OrderMapper;
import pl.ldz.microsrv.order.repository.OrderRepository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Business logic for Order CRUD operations.
 *
 * <p>This service is the sole owner of all lifecycle fields on {@link Order}:
 * {@code id}, {@code status}, {@code createdAt}, {@code updatedAt}, {@code deletedAt}.
 * The mapper never writes these; the controller never reads the entity directly.
 *
 * <p>Idempotency is enforced via {@link IdempotencyService}; domain events are persisted
 * via {@link OutboxService} within the same {@code @Transactional} scope as the order write.
 * This service never calls {@code KafkaTemplate} directly.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

  private final OrderRepository orderRepository;
  private final OrderMapper orderMapper;
  private final IdempotencyService idempotencyService;
  private final OutboxService outboxService;
  private final ObjectMapper objectMapper;

  // ── Create ────────────────────────────────────────────────────────────────

  /**
     * Creates a new order with idempotency support.
     *
     * <ol>
     *   <li>Checks the idempotency key — returns the stored response immediately on replay.</li>
     *   <li>Generates a time-ordered UUIDv7 for the external {@code id}.</li>
     *   <li>Maps the request DTO to an entity (lifecycle fields are ignored by the mapper).</li>
     *   <li>Sets all lifecycle fields explicitly.</li>
     *   <li>Persists, writes the outbox event, marks the key completed, and returns the DTO.</li>
     * </ol>
     */
  @Transactional
  public OrderResponse createOrder(OrderRequest request, String idempotencyKey) {
    // Idempotency gate — return stored response on replay
    Optional<String> cached = idempotencyService.checkAndStore(idempotencyKey, request);
    if (cached.isPresent()) {
      try {
        return objectMapper.readValue(cached.get(), OrderResponse.class);
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialise cached idempotency response", e);
      }
    }

    UUID id = Generators.timeBasedEpochGenerator().generate();

    Order order = orderMapper.toEntity(request);
    order.setId(id);
    order.setStatus(OrderStatus.CREATED);
    order.setCreatedAt(OffsetDateTime.now());
    order.setUpdatedAt(OffsetDateTime.now());

    Order saved = orderRepository.save(order);

    // Write outbox event within the same transaction
    OrderCreatedEvent event = new OrderCreatedEvent(
        saved.getId(), saved.getCustomerId(), saved.getTotalAmount(), Instant.now());
    outboxService.saveEvent(saved.getId(), "ORDER_CREATED", event);

    OrderResponse response = orderMapper.toResponse(saved);

    // Seal the idempotency record
    try {
      String responseJson = objectMapper.writeValueAsString(response);
      idempotencyService.markCompleted(idempotencyKey, responseJson);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialise OrderResponse for idempotency record", e);
    }

    log.info("Order created: id={}, idempotencyKey={}", saved.getId(), idempotencyKey);
    return response;
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
     * Updates the mutable fields of an existing order with optional idempotency support.
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
    // Idempotency gate (optional for PUT)
    if (idempotencyKey != null) {
      Optional<String> cached = idempotencyService.checkAndStore(idempotencyKey, request);
      if (cached.isPresent()) {
        try {
          return objectMapper.readValue(cached.get(), OrderResponse.class);
        } catch (Exception e) {
          throw new RuntimeException("Failed to deserialise cached idempotency response", e);
        }
      }
    }

    Order order = orderRepository.findByIdAndDeletedAtIsNull(id)
        .orElseThrow(() -> new OrderNotFoundException(id));

    order.setTotalAmount(request.getTotalAmount());
    order.setUpdatedAt(OffsetDateTime.now());

    Order saved = orderRepository.save(order);

    // Write outbox event within the same transaction
    OrderUpdatedEvent event = new OrderUpdatedEvent(
        saved.getId(), saved.getCustomerId(), saved.getTotalAmount(),
        saved.getStatus(), Instant.now());
    outboxService.saveEvent(saved.getId(), "ORDER_UPDATED", event);

    OrderResponse response = orderMapper.toResponse(saved);

    // Seal the idempotency record if key was provided
    if (idempotencyKey != null) {
      try {
        String responseJson = objectMapper.writeValueAsString(response);
        idempotencyService.markCompleted(idempotencyKey, responseJson);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialise OrderResponse for idempotency record", e);
      }
    }

    log.info("Order updated: id={}, idempotencyKey={}", saved.getId(), idempotencyKey);
    return response;
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
    // Idempotency gate (optional for DELETE)
    if (idempotencyKey != null) {
      Optional<String> cached = idempotencyService.checkAndStore(idempotencyKey, id.toString());
      if (cached.isPresent()) {
        return; // 204 has no body — replay is a no-op
      }
    }

    Order order = orderRepository.findByIdAndDeletedAtIsNull(id)
        .orElseThrow(() -> new OrderNotFoundException(id));

    order.setDeletedAt(OffsetDateTime.now());
    order.setUpdatedAt(OffsetDateTime.now());

    orderRepository.save(order);

    // Write outbox event within the same transaction
    OrderDeletedEvent event = new OrderDeletedEvent(order.getId(), Instant.now());
    outboxService.saveEvent(order.getId(), "ORDER_DELETED", event);

    // Seal the idempotency record (no response body for 204)
    if (idempotencyKey != null) {
      idempotencyService.markCompleted(idempotencyKey, null);
    }

    log.info("Order soft-deleted: id={}, idempotencyKey={}", id, idempotencyKey);
  }
}
