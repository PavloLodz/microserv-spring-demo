package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import pl.ldz.microsrv.common.event.OrderStatus;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;
import pl.ldz.microsrv.order.entity.Order;
import pl.ldz.microsrv.order.exception.OrderNotFoundException;
import pl.ldz.microsrv.order.mapper.OrderMapper;
import pl.ldz.microsrv.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

  @Mock
  private OrderRepository orderRepository;

  @Mock
  private OrderMapper orderMapper;

  @Mock
  private IdempotencyService idempotencyService;

  @Mock
  private OutboxService outboxService;

  @Mock
  private ObjectMapper objectMapper;

  @InjectMocks
  private OrderService orderService;

  // ── Shared fixtures ───────────────────────────────────────────────────────

  private static final UUID CUSTOMER_ID = UUID.randomUUID();
  private static final BigDecimal AMOUNT = new BigDecimal("99.99");
  private static final String IDEM_KEY = UUID.randomUUID().toString();

  private OrderRequest buildRequest() {
    OrderRequest req = new OrderRequest();
    req.setCustomerId(CUSTOMER_ID);
    req.setTotalAmount(AMOUNT);
    return req;
  }

  private Order buildOrder(UUID id) {
    Order order = new Order();
    order.setId(id);
    order.setCustomerId(CUSTOMER_ID);
    order.setStatus(OrderStatus.CREATED);
    order.setTotalAmount(AMOUNT);
    order.setCreatedAt(OffsetDateTime.now());
    order.setUpdatedAt(OffsetDateTime.now());
    return order;
  }

  private OrderResponse buildResponse(UUID id) {
    OrderResponse resp = new OrderResponse();
    resp.setId(id);
    return resp;
  }

  // ── createOrder ───────────────────────────────────────────────────────────

  @Nested
  @DisplayName("createOrder")
  class CreateOrder {

    /**
     * Happy path: idempotency cache miss → order saved, outbox event written,
     * idempotency marked completed, DTO returned.
     */
    @Test
    @DisplayName("happy path: sets id, status=CREATED, timestamps; saves once; returns DTO")
    void happyPath() throws Exception {
      OrderRequest request = buildRequest();
      Order mappedEntity = new Order();
      when(orderMapper.toEntity(request)).thenReturn(mappedEntity);

      ArgumentCaptor<Order> saveCaptor = ArgumentCaptor.forClass(Order.class);
      when(orderRepository.save(saveCaptor.capture())).thenAnswer(inv -> inv.getArgument(0));

      OrderResponse expectedResponse = new OrderResponse();
      when(orderMapper.toResponse(any(Order.class))).thenReturn(expectedResponse);

      // Cache miss — proceed with creation
      when(idempotencyService.checkAndStore(eq(IDEM_KEY), eq(request)))
          .thenReturn(Optional.empty());
      when(objectMapper.writeValueAsString(any())).thenReturn("{\"id\":\"test\"}");

      OrderResponse result = orderService.createOrder(request, IDEM_KEY);

      verify(orderRepository, times(1)).save(any(Order.class));

      Order saved = saveCaptor.getValue();
      assertThat(saved.getId()).isNotNull();
      assertThat(saved.getStatus()).isEqualTo(OrderStatus.CREATED);
      assertThat(saved.getCreatedAt()).isNotNull();
      assertThat(saved.getUpdatedAt()).isNotNull();
      assertThat(result).isSameAs(expectedResponse);
    }

    /**
     * 16.3 — idempotency cache hit: stored response returned immediately,
     * orderRepository.save() never called.
     */
    @Test
    @DisplayName("idempotency cache hit: returns stored response without saving")
    void idempotencyCacheHit() throws Exception {
      OrderRequest request = buildRequest();
      OrderResponse storedResponse = buildResponse(UUID.randomUUID());
      String storedJson = "{\"id\":\"stored\"}";

      when(idempotencyService.checkAndStore(eq(IDEM_KEY), eq(request)))
          .thenReturn(Optional.of(storedJson));
      when(objectMapper.readValue(eq(storedJson), eq(OrderResponse.class)))
          .thenReturn(storedResponse);

      OrderResponse result = orderService.createOrder(request, IDEM_KEY);

      assertThat(result).isSameAs(storedResponse);
      verify(orderRepository, never()).save(any());
    }

    /**
     * 16.4 — outbox event written: on cache miss, outboxService.saveEvent() called once
     * with eventType="ORDER_CREATED" and non-null aggregateId.
     */
    @Test
    @DisplayName("outbox event written with ORDER_CREATED on cache miss")
    void outboxEventWritten() throws Exception {
      OrderRequest request = buildRequest();
      Order mappedEntity = new Order();
      when(orderMapper.toEntity(request)).thenReturn(mappedEntity);
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
      when(orderMapper.toResponse(any(Order.class))).thenReturn(new OrderResponse());
      when(idempotencyService.checkAndStore(eq(IDEM_KEY), eq(request)))
          .thenReturn(Optional.empty());
      when(objectMapper.writeValueAsString(any())).thenReturn("{}");

      orderService.createOrder(request, IDEM_KEY);

      ArgumentCaptor<String> eventTypeCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<UUID> aggregateIdCaptor = ArgumentCaptor.forClass(UUID.class);
      verify(outboxService, times(1))
          .saveEvent(aggregateIdCaptor.capture(), eventTypeCaptor.capture(), any());

      assertThat(eventTypeCaptor.getValue()).isEqualTo("ORDER_CREATED");
      assertThat(aggregateIdCaptor.getValue()).isNotNull();
    }

    /**
     * 16.5 — markCompleted called: idempotencyService.markCompleted() called exactly once
     * with the correct key after successful order creation.
     */
    @Test
    @DisplayName("markCompleted called once with correct key after creation")
    void markCompletedCalled() throws Exception {
      OrderRequest request = buildRequest();
      Order mappedEntity = new Order();
      when(orderMapper.toEntity(request)).thenReturn(mappedEntity);
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
      when(orderMapper.toResponse(any(Order.class))).thenReturn(new OrderResponse());
      when(idempotencyService.checkAndStore(eq(IDEM_KEY), eq(request)))
          .thenReturn(Optional.empty());
      when(objectMapper.writeValueAsString(any())).thenReturn("{}");

      orderService.createOrder(request, IDEM_KEY);

      verify(idempotencyService, times(1)).markCompleted(eq(IDEM_KEY), anyString());
    }
  }

  // ── getOrder ──────────────────────────────────────────────────────────────

  @Nested
  @DisplayName("getOrder")
  class GetOrder {

    @Test
    @DisplayName("found: loads entity, calls mapper, returns DTO")
    void found() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);
      OrderResponse response = buildResponse(id);

      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderMapper.toResponse(order)).thenReturn(response);

      OrderResponse result = orderService.getOrder(id);

      verify(orderMapper).toResponse(order);
      assertThat(result).isSameAs(response);
    }

    @Test
    @DisplayName("not found: throws OrderNotFoundException")
    void notFound() {
      UUID id = UUID.randomUUID();
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.empty());

      assertThatThrownBy(() -> orderService.getOrder(id))
          .isInstanceOf(OrderNotFoundException.class)
          .hasMessageContaining(id.toString());
    }
  }

  // ── listOrders ────────────────────────────────────────────────────────────

  @Nested
  @DisplayName("listOrders")
  class ListOrders {

    @Test
    @DisplayName("with customerId: calls filtered repository method")
    void withCustomerId() {
      Pageable pageable = PageRequest.of(0, 20);
      Page<Order> page = new PageImpl<>(List.of());

      when(orderRepository.findByCustomerIdAndDeletedAtIsNull(CUSTOMER_ID, pageable))
          .thenReturn(page);

      orderService.listOrders(CUSTOMER_ID, pageable);

      verify(orderRepository).findByCustomerIdAndDeletedAtIsNull(CUSTOMER_ID, pageable);
      verify(orderRepository, never()).findByDeletedAtIsNull(any());
    }

    @Test
    @DisplayName("without customerId: calls unfiltered repository method")
    void withoutCustomerId() {
      Pageable pageable = PageRequest.of(0, 20);
      Page<Order> page = new PageImpl<>(List.of());

      when(orderRepository.findByDeletedAtIsNull(pageable)).thenReturn(page);

      orderService.listOrders(null, pageable);

      verify(orderRepository).findByDeletedAtIsNull(pageable);
      verify(orderRepository, never()).findByCustomerIdAndDeletedAtIsNull(any(), any());
    }
  }

  // ── updateOrder ───────────────────────────────────────────────────────────

  @Nested
  @DisplayName("updateOrder")
  class UpdateOrder {

    /**
     * Happy path with null idempotency key: updates fields, outbox event written, DTO returned.
     */
    @Test
    @DisplayName("happy path (null key): updates totalAmount, refreshes updatedAt, saves, returns DTO")
    void happyPath() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);

      OrderRequest request = buildRequest();
      request.setTotalAmount(new BigDecimal("199.99"));

      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

      OrderResponse expectedResponse = buildResponse(id);
      when(orderMapper.toResponse(any(Order.class))).thenReturn(expectedResponse);

      OrderResponse result = orderService.updateOrder(id, request, null);

      ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
      verify(orderRepository).save(captor.capture());

      Order saved = captor.getValue();
      assertThat(saved.getTotalAmount()).isEqualByComparingTo(new BigDecimal("199.99"));
      assertThat(saved.getUpdatedAt()).isNotNull();
      assertThat(result).isSameAs(expectedResponse);
    }

    /**
     * 16.6 — outbox event written: saveEvent called with eventType="ORDER_UPDATED".
     */
    @Test
    @DisplayName("outbox event written with ORDER_UPDATED")
    void outboxEventWritten() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
      when(orderMapper.toResponse(any(Order.class))).thenReturn(buildResponse(id));

      orderService.updateOrder(id, buildRequest(), null);

      ArgumentCaptor<String> eventTypeCaptor = ArgumentCaptor.forClass(String.class);
      verify(outboxService, times(1))
          .saveEvent(any(UUID.class), eventTypeCaptor.capture(), any());
      assertThat(eventTypeCaptor.getValue()).isEqualTo("ORDER_UPDATED");
    }

    /**
     * 16.7 — skips idempotency when key is null.
     */
    @Test
    @DisplayName("skips idempotency check when key is null")
    void skipsIdempotencyWhenKeyNull() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
      when(orderMapper.toResponse(any(Order.class))).thenReturn(buildResponse(id));

      orderService.updateOrder(id, buildRequest(), null);

      verify(idempotencyService, never()).checkAndStore(any(), any());
    }

    @Test
    @DisplayName("not found: throws OrderNotFoundException")
    void notFound() {
      UUID id = UUID.randomUUID();
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.empty());

      assertThatThrownBy(() -> orderService.updateOrder(id, buildRequest(), null))
          .isInstanceOf(OrderNotFoundException.class)
          .hasMessageContaining(id.toString());
    }
  }

  // ── deleteOrder ───────────────────────────────────────────────────────────

  @Nested
  @DisplayName("deleteOrder")
  class DeleteOrder {

    /**
     * Happy path with null idempotency key: soft-delete sets deletedAt, outbox event written.
     */
    @Test
    @DisplayName("happy path (null key): sets deletedAt and updatedAt; saves once")
    void happyPath() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);

      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

      orderService.deleteOrder(id, null);

      ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
      verify(orderRepository).save(captor.capture());

      Order saved = captor.getValue();
      assertThat(saved.getDeletedAt()).isNotNull();
      assertThat(saved.getUpdatedAt()).isNotNull();
    }

    /**
     * 16.8 — outbox event written: saveEvent called with eventType="ORDER_DELETED".
     */
    @Test
    @DisplayName("outbox event written with ORDER_DELETED")
    void outboxEventWritten() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

      orderService.deleteOrder(id, null);

      ArgumentCaptor<String> eventTypeCaptor = ArgumentCaptor.forClass(String.class);
      verify(outboxService, times(1))
          .saveEvent(any(UUID.class), eventTypeCaptor.capture(), any());
      assertThat(eventTypeCaptor.getValue()).isEqualTo("ORDER_DELETED");
    }

    /**
     * 16.9 — skips idempotency when key is null.
     */
    @Test
    @DisplayName("skips idempotency check when key is null")
    void skipsIdempotencyWhenKeyNull() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

      orderService.deleteOrder(id, null);

      verify(idempotencyService, never()).checkAndStore(any(), any());
    }

    @Test
    @DisplayName("not found: throws OrderNotFoundException")
    void notFound() {
      UUID id = UUID.randomUUID();
      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.empty());

      assertThatThrownBy(() -> orderService.deleteOrder(id, null))
          .isInstanceOf(OrderNotFoundException.class)
          .hasMessageContaining(id.toString());
    }
  }
}
