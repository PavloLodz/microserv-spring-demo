package pl.ldz.microsrv.order.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import pl.ldz.microsrv.order.api.OrdersApi;
import pl.ldz.microsrv.order.api.model.OrderListResponse;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;
import pl.ldz.microsrv.order.service.OrderService;

import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 * REST controller for Order CRUD operations.
 *
 * <p>Implements the {@link OrdersApi} interface generated from {@code orders-api.yaml}.
 * Contains zero business logic — all decisions live in {@link OrderService}.
 */
@RestController
@RequiredArgsConstructor
public class OrderController implements OrdersApi {

    private final OrderService orderService;

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code 201 Created} with a {@code Location} header pointing to the new resource.
     */
    @Override
    public ResponseEntity<OrderResponse> createOrder(String idempotencyKey, OrderRequest orderRequest) {
        OrderResponse response = orderService.createOrder(orderRequest, idempotencyKey);
        URI location = ServletUriComponentsBuilder
                .fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(response.getId())
                .toUri();
        return ResponseEntity.created(location).body(response);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code 200 OK} with a paginated list of orders,
     * optionally filtered by {@code customerId}.
     */
    @Override
    public ResponseEntity<OrderListResponse> listOrders(UUID customerId, Integer page, Integer size) {
        int pageNum = (page != null) ? page : 0;
        int pageSize = (size != null) ? size : 20;
        Pageable pageable = PageRequest.of(pageNum, pageSize);

        Page<OrderResponse> result = orderService.listOrders(customerId, pageable);

        OrderListResponse body = new OrderListResponse();
        body.setContent((List<OrderResponse>) result.getContent());
        body.setPage(result.getNumber());
        body.setSize(result.getSize());
        body.setTotalElements(result.getTotalElements());
        body.setTotalPages(result.getTotalPages());

        return ResponseEntity.ok(body);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code 200 OK} with the order data, or triggers
     * a {@code 404} via {@link pl.ldz.microsrv.order.exception.GlobalExceptionHandler}.
     */
    @Override
    public ResponseEntity<OrderResponse> getOrder(UUID id) {
        return ResponseEntity.ok(orderService.getOrder(id));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code 200 OK} with the updated order,
     * or {@code 404}/{@code 409} via the global handler.
     */
    @Override
    public ResponseEntity<OrderResponse> updateOrder(UUID id, OrderRequest orderRequest, String idempotencyKey) {
        return ResponseEntity.ok(orderService.updateOrder(id, orderRequest, idempotencyKey));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code 204 No Content} on success,
     * or {@code 404} via the global handler.
     */
    @Override
    public ResponseEntity<Void> deleteOrder(UUID id, String idempotencyKey) {
        orderService.deleteOrder(id, idempotencyKey);
        return ResponseEntity.noContent().build();
    }
}
