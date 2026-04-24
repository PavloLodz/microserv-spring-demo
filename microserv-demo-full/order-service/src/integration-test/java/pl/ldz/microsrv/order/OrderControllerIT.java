package pl.ldz.microsrv.order;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
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
 * (via Testcontainers) with a fully started Spring Boot application context.
 *
 * <p>Tests cover the full HTTP request/response cycle: routing, serialization,
 * business logic through {@code OrderService}, persistence via JPA + Flyway,
 * and error responses through {@code GlobalExceptionHandler}.
 *
 * <p>No {@code debugId} must appear in any response body — verified in each test that
 * inspects a response JSON string.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderControllerIT extends AbstractControllerIT {

    private static final String ORDERS_URL = "/api/v1/orders";

    // ── Helper ────────────────────────────────────────────────────────────────

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

    // ── POST /api/v1/orders ───────────────────────────────────────────────────

    /**
     * 8.3 — Happy path: 201 response, Location header, valid body.
     */
    @Test
    @org.junit.jupiter.api.Order(1)
    void createOrder_happyPath_returns201() {
        ResponseEntity<OrderResponse> response =
                restTemplate.postForEntity(ORDERS_URL, validRequest(), OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();

        OrderResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getId()).isNotNull();
        assertThat(body.getStatus()).isNotNull();
        assertThat(body.getTotalAmount()).isEqualByComparingTo(new BigDecimal("99.99"));
        assertThat(body.getCreatedAt()).isNotNull();
        assertThat(body.getUpdatedAt()).isNotNull();

        // 8.13 — debugId must not appear
        ResponseEntity<String> raw =
                restTemplate.postForEntity(ORDERS_URL, validRequest(), String.class);
        assertThat(raw.getBody()).doesNotContain("debugId");
    }

    /**
     * 8.4 — Invalid body (null required fields) → 400.
     */
    @Test
    @org.junit.jupiter.api.Order(2)
    void createOrder_invalidBody_returns400() {
        OrderRequest bad = new OrderRequest(); // no customerId, no totalAmount
        ResponseEntity<String> response =
                restTemplate.postForEntity(ORDERS_URL, bad, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ── GET /api/v1/orders/{id} ───────────────────────────────────────────────

    /**
     * 8.5 — Found: 200 with correct data.
     */
    @Test
    @org.junit.jupiter.api.Order(3)
    void getOrder_found_returns200() {
        // Create an order first
        OrderResponse created = restTemplate
                .postForEntity(ORDERS_URL, validRequest(), OrderResponse.class)
                .getBody();
        assertThat(created).isNotNull();

        ResponseEntity<OrderResponse> response =
                restTemplate.getForEntity(ORDERS_URL + "/" + created.getId(), OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getId()).isEqualTo(created.getId());
    }

    /**
     * 8.6 — Unknown id → 404.
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
     * 8.7 — List all: 200 with valid OrderListResponse.
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
     * 8.8 — List with customerId filter: only matching orders returned.
     */
    @Test
    @org.junit.jupiter.api.Order(6)
    void listOrders_filteredByCustomerId_returnsOnlyMatchingOrders() {
        UUID targetCustomer = UUID.randomUUID();
        UUID otherCustomer = UUID.randomUUID();

        // Create one order for the target customer and one for another
        restTemplate.postForEntity(ORDERS_URL, validRequest(targetCustomer), OrderResponse.class);
        restTemplate.postForEntity(ORDERS_URL, validRequest(otherCustomer), OrderResponse.class);

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
     * 8.9 — Happy path: 200 with updated fields.
     */
    @Test
    @org.junit.jupiter.api.Order(7)
    void updateOrder_happyPath_returns200() {
        OrderResponse created = restTemplate
                .postForEntity(ORDERS_URL, validRequest(), OrderResponse.class)
                .getBody();
        assertThat(created).isNotNull();

        OrderRequest updateReq = new OrderRequest();
        updateReq.setCustomerId(created.getCustomerId());
        updateReq.setTotalAmount(new BigDecimal("199.00"));

        ResponseEntity<OrderResponse> response = restTemplate.exchange(
                ORDERS_URL + "/" + created.getId(),
                HttpMethod.PUT,
                new HttpEntity<>(updateReq),
                OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getTotalAmount()).isEqualByComparingTo(new BigDecimal("199.00"));
    }

    /**
     * 8.10 — Unknown id → 404.
     */
    @Test
    @org.junit.jupiter.api.Order(8)
    void updateOrder_unknownId_returns404() {
        ResponseEntity<String> response = restTemplate.exchange(
                ORDERS_URL + "/" + UUID.randomUUID(),
                HttpMethod.PUT,
                new HttpEntity<>(validRequest()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ── DELETE /api/v1/orders/{id} ────────────────────────────────────────────

    /**
     * 8.11 — Happy path: 204; subsequent GET returns 404.
     */
    @Test
    @org.junit.jupiter.api.Order(9)
    void deleteOrder_happyPath_returns204_thenGetReturns404() {
        OrderResponse created = restTemplate
                .postForEntity(ORDERS_URL, validRequest(), OrderResponse.class)
                .getBody();
        assertThat(created).isNotNull();

        ResponseEntity<Void> deleteResponse = restTemplate.exchange(
                ORDERS_URL + "/" + created.getId(),
                HttpMethod.DELETE,
                HttpEntity.EMPTY,
                Void.class);

        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        // Subsequent GET must return 404
        ResponseEntity<String> getResponse =
                restTemplate.getForEntity(ORDERS_URL + "/" + created.getId(), String.class);
        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    /**
     * 8.12 — Unknown id → 404.
     */
    @Test
    @org.junit.jupiter.api.Order(10)
    void deleteOrder_unknownId_returns404() {
        ResponseEntity<String> response = restTemplate.exchange(
                ORDERS_URL + "/" + UUID.randomUUID(),
                HttpMethod.DELETE,
                HttpEntity.EMPTY,
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
