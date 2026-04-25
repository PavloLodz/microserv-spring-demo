package pl.ldz.microsrv.order;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import pl.ldz.microsrv.order.api.model.OrderListResponse;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code OrderController} running against a real PostgreSQL container
 * and Kafka container (via Testcontainers) with a fully started Spring Boot application context.
 *
 * <p>Tests cover the full HTTP request/response cycle: routing, serialization,
 * business logic through {@code OrderService}, persistence via JPA + Flyway,
 * and error responses through {@code GlobalExceptionHandler}.
 *
 * <p>All write operations (POST, PUT, DELETE) include the {@code Idempotency-Key} header
 * as required by the API contract. Each test uses a fresh key to avoid cross-test replay hits.
 *
 * <p>No {@code debugId} must appear in any response body — verified in each test that
 * inspects a response JSON string.
 *
 * <p>If Docker is not available the entire class is skipped (not failed) via the
 * assumption in {@link AbstractControllerIT#requireDocker()}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderControllerIT extends AbstractControllerIT {

    private static final String ORDERS_URL = "/api/v1/orders";

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Returns a fresh {@code Idempotency-Key} header value (random UUID). */
    private static String newIdempotencyKey() {
        return UUID.randomUUID().toString();
    }

    /** Builds an {@link HttpHeaders} map with a fresh idempotency key. */
    private static HttpHeaders idempotencyHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", newIdempotencyKey());
        return headers;
    }

    private OrderRequest validRequest() {
        OrderRequest req = new OrderRequest();
        req.setCustomerId(UUID.randomUUID());
        req.setTotalAmount(new BigDecimal("99.99"));
        return req;
    }

    private OrderRequest validRequest(UUID customerId) {
        OrderRequest req = new OrderRequest();
        req.setCustomerId(customerId);
        req.setTotalAmount(new BigDecimal("49.00"));
        return req;
    }

    /** POST helper that always includes a fresh {@code Idempotency-Key} header. */
    private ResponseEntity<OrderResponse> createOrder(OrderRequest request) {
        return restTemplate.exchange(
                ORDERS_URL,
                HttpMethod.POST,
                new HttpEntity<>(request, idempotencyHeaders()),
                OrderResponse.class);
    }

    // ── POST /api/v1/orders ───────────────────────────────────────────────────

    /**
     * Happy path: 201 response, Location header, valid body, no debugId.
     */
    @Test
    @org.junit.jupiter.api.Order(1)
    void createOrder_happyPath_returns201() {
        ResponseEntity<OrderResponse> response = createOrder(validRequest());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();

        OrderResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getId()).isNotNull();
        assertThat(body.getStatus()).isNotNull();
        assertThat(body.getTotalAmount()).isEqualByComparingTo(new BigDecimal("99.99"));
        assertThat(body.getCreatedAt()).isNotNull();
        assertThat(body.getUpdatedAt()).isNotNull();

        // debugId must not appear in any response
        ResponseEntity<String> raw = restTemplate.exchange(
                ORDERS_URL,
                HttpMethod.POST,
                new HttpEntity<>(validRequest(), idempotencyHeaders()),
                String.class);
        assertThat(raw.getBody()).doesNotContain("debugId");
    }

    /**
     * Invalid body (null required fields) → 400.
     */
    @Test
    @org.junit.jupiter.api.Order(2)
    void createOrder_invalidBody_returns400() {
        OrderRequest bad = new OrderRequest(); // no customerId, no totalAmount
        ResponseEntity<String> response = restTemplate.exchange(
                ORDERS_URL,
                HttpMethod.POST,
                new HttpEntity<>(bad, idempotencyHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ── GET /api/v1/orders/{id} ───────────────────────────────────────────────

    /**
     * Found: 200 with correct data.
     */
    @Test
    @org.junit.jupiter.api.Order(3)
    void getOrder_found_returns200() {
        OrderResponse created = createOrder(validRequest()).getBody();
        assertThat(created).isNotNull();

        ResponseEntity<OrderResponse> response =
                restTemplate.getForEntity(ORDERS_URL + "/" + created.getId(), OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getId()).isEqualTo(created.getId());
    }

    /**
     * Unknown id → 404.
     */
    @Test
    @org.junit.jupiter.api.Order(4)
    void getOrder_unknownId_returns404() {
        ResponseEntity<String> response =
                restTemplate.getForEntity(ORDERS_URL + "/" + UUID.randomUUID(), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).doesNotContain("debugId");
    }

    // ── GET /api/v1/orders ────────────────────────────────────────────────────

    /**
     * List all: 200 with valid OrderListResponse.
     */
    @Test
    @org.junit.jupiter.api.Order(5)
    void listOrders_all_returns200() {
        ResponseEntity<OrderListResponse> response =
                restTemplate.getForEntity(ORDERS_URL, OrderListResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getContent()).isNotNull();
    }

    /**
     * List with customerId filter: only matching orders returned.
     */
    @Test
    @org.junit.jupiter.api.Order(6)
    void listOrders_filteredByCustomerId_returnsOnlyMatchingOrders() {
        UUID targetCustomer = UUID.randomUUID();
        UUID otherCustomer = UUID.randomUUID();

        createOrder(validRequest(targetCustomer));
        createOrder(validRequest(otherCustomer));

        ResponseEntity<OrderListResponse> response = restTemplate.getForEntity(
                ORDERS_URL + "?customerId=" + targetCustomer, OrderListResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getContent())
                .isNotEmpty()
                .allMatch(o -> targetCustomer.equals(o.getCustomerId()));
    }

    // ── PUT /api/v1/orders/{id} ───────────────────────────────────────────────

    /**
     * Happy path: 200 with updated fields.
     */
    @Test
    @org.junit.jupiter.api.Order(7)
    void updateOrder_happyPath_returns200() {
        OrderResponse created = createOrder(validRequest()).getBody();
        assertThat(created).isNotNull();

        OrderRequest updateReq = new OrderRequest();
        updateReq.setCustomerId(created.getCustomerId());
        updateReq.setTotalAmount(new BigDecimal("199.00"));

        ResponseEntity<OrderResponse> response = restTemplate.exchange(
                ORDERS_URL + "/" + created.getId(),
                HttpMethod.PUT,
                new HttpEntity<>(updateReq, idempotencyHeaders()),
                OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getTotalAmount()).isEqualByComparingTo(new BigDecimal("199.00"));
    }

    /**
     * Unknown id → 404.
     */
    @Test
    @org.junit.jupiter.api.Order(8)
    void updateOrder_unknownId_returns404() {
        ResponseEntity<String> response = restTemplate.exchange(
                ORDERS_URL + "/" + UUID.randomUUID(),
                HttpMethod.PUT,
                new HttpEntity<>(validRequest(), idempotencyHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ── DELETE /api/v1/orders/{id} ────────────────────────────────────────────

    /**
     * Happy path: 204; subsequent GET returns 404.
     */
    @Test
    @org.junit.jupiter.api.Order(9)
    void deleteOrder_happyPath_returns204_thenGetReturns404() {
        OrderResponse created = createOrder(validRequest()).getBody();
        assertThat(created).isNotNull();

        ResponseEntity<Void> deleteResponse = restTemplate.exchange(
                ORDERS_URL + "/" + created.getId(),
                HttpMethod.DELETE,
                new HttpEntity<>(null, idempotencyHeaders()),
                Void.class);

        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        ResponseEntity<String> getResponse =
                restTemplate.getForEntity(ORDERS_URL + "/" + created.getId(), String.class);
        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    /**
     * Unknown id → 404.
     */
    @Test
    @org.junit.jupiter.api.Order(10)
    void deleteOrder_unknownId_returns404() {
        ResponseEntity<String> response = restTemplate.exchange(
                ORDERS_URL + "/" + UUID.randomUUID(),
                HttpMethod.DELETE,
                new HttpEntity<>(null, idempotencyHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
