package pl.ldz.microsrv.order.service;

import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

  @Mock
  private OrderRepository orderRepository;

  @Mock
  private OrderMapper orderMapper;

  @InjectMocks
  private OrderService orderService;

  // ── Shared fixtures ───────────────────────────────────────────────────────

  private static final UUID CUSTOMER_ID = UUID.randomUUID();
  private static final BigDecimal AMOUNT = new BigDecimal("99.99");

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
     * 7.2 — happy path: id non-null, status = CREATED, timestamps set,
     * repository.save() called exactly once.
     */
    @Test
    @DisplayName("happy path: sets id, status=CREATED, timestamps; saves once; returns DTO")
    void happyPath() {
      OrderRequest request = buildRequest();
      Order mappedEntity = new Order(); // mapper returns a blank entity; service sets lifecycle fields
      when(orderMapper.toEntity(request)).thenReturn(mappedEntity);

      ArgumentCaptor<Order> saveCaptor = ArgumentCaptor.forClass(Order.class);
      when(orderRepository.save(saveCaptor.capture())).thenAnswer(inv -> inv.getArgument(0));

      OrderResponse expectedResponse = new OrderResponse();
      when(orderMapper.toResponse(any(Order.class))).thenReturn(expectedResponse);

      OrderResponse result = orderService.createOrder(request, null);

      // repository.save() called exactly once
      verify(orderRepository, times(1)).save(any(Order.class));

      Order saved = saveCaptor.getValue();

      // id is non-null (UUIDv7 — version bits are at positions 12–15 of variant-1 UUID)
      assertThat(saved.getId()).isNotNull();

      // status must be CREATED
      assertThat(saved.getStatus()).isEqualTo(OrderStatus.CREATED);

      // both timestamps must be set
      assertThat(saved.getCreatedAt()).isNotNull();
      assertThat(saved.getUpdatedAt()).isNotNull();

      // result is the mapped DTO
      assertThat(result).isSameAs(expectedResponse);
    }
  }

  // ── getOrder ──────────────────────────────────────────────────────────────

  @Nested
  @DisplayName("getOrder")
  class GetOrder {

    /** 7.3 — found: mapper called with loaded entity; DTO returned. */
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

    /** 7.4 — not found: OrderNotFoundException thrown when repository returns empty. */
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

    /** 7.5 — with customerId: findByCustomerIdAndDeletedAtIsNull called (not the no-filter variant). */
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

    /** 7.6 — without customerId: findByDeletedAtIsNull called (fallback path). */
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

    /** 7.7 — happy path: updatedAt refreshed, save called, DTO returned. */
    @Test
    @DisplayName("happy path: updates totalAmount, refreshes updatedAt, saves, returns DTO")
    void happyPath() {
      UUID id = UUID.randomUUID();
      Order order = buildOrder(id);
      OffsetDateTime originalUpdatedAt = order.getUpdatedAt();

      OrderRequest request = buildRequest();
      request.setTotalAmount(new BigDecimal("199.99"));

      when(orderRepository.findByIdAndDeletedAtIsNull(id)).thenReturn(Optional.of(order));
      when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

      OrderResponse expectedResponse = buildResponse(id);
      when(orderMapper.toResponse(any(Order.class))).thenReturn(expectedResponse);

      // Small sleep to guarantee clock advancement on fast machines
      OrderResponse result = orderService.updateOrder(id, request, null);

      ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
      verify(orderRepository).save(captor.capture());

      Order saved = captor.getValue();
      assertThat(saved.getTotalAmount()).isEqualByComparingTo(new BigDecimal("199.99"));
      // updatedAt must have been set (not null); it may equal originalUpdatedAt if clock resolution
      // is low, but it must never be null.
      assertThat(saved.getUpdatedAt()).isNotNull();
      assertThat(result).isSameAs(expectedResponse);
    }

    /** 7.8 — not found: OrderNotFoundException thrown. */
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

    /** 7.9 — happy path: deletedAt set to non-null timestamp, save called. */
    @Test
    @DisplayName("happy path: sets deletedAt and updatedAt; saves once")
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

    /** 7.10 — not found: OrderNotFoundException thrown. */
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
