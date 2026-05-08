package pl.ldz.microsrv.order;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import pl.ldz.microsrv.order.api.model.OrderListResponse;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Class 1 — Basic HTTP contract for {@code OrderController}.
 *
 * <p>Covers status codes, headers (Location, X-Debug-Id), body shape,
 * pagination fields, and soft-delete visibility.
 *
 * <p>Every test method is fully self-sufficient: it creates whatever data it
 * needs inline. This avoids cross-method state dependencies and ensures that
 * the standard {@code @BeforeEach} truncation pattern (used by every class in
 * this suite) works without special-casing.
 *
 * <p><strong>Why no static {@code @BeforeAll(@Autowired ...)} here?</strong><br>
 * Passing a Spring-managed bean as an {@code @Autowired} parameter to a static
 * {@code @BeforeAll} method forces the Spring TestContext Framework to use a
 * different context-loading path. This causes Spring to create a <em>second</em>
 * application context (with its own HikariPool) instead of reusing the cached
 * one. When the first context closes, its Postgres container port is gone, so
 * the second pool's connection attempts fail with
 * {@code Connection refused} / {@code CannotGetJdbcConnectionException}.
 * The fix is to inject {@code JdbcTemplate} as a plain {@code @Autowired}
 * instance field and call truncation from {@code @BeforeEach} as usual.
 *
 * <p>Corresponds to tasks 1.1 – 1.12 in {@code prompts/mini-tasks-01.md}.
 */
// ── Task 1.1 ──────────────────────────────────────────────────────────────────
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderController_CrudOrderedIT extends AbstractControllerIT {

  private static final String ORDERS_URL = "/api/v1/orders";

  // ── Task 1.2 ─────────────────────────────────────────────────────────────

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() {
    jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
  }

  // ── Task 1.3 — Helpers ────────────────────────────────────────────────────

  private static String newIdempotencyKey() {
    return UUID.randomUUID().toString();
  }

  private static HttpHeaders idempotencyHeaders() {
    HttpHeaders h = new HttpHeaders();
    h.set("Idempotency-Key", newIdempotencyKey());
    return h;
  }

  private static HttpHeaders idempotencyHeaders(String key) {
    HttpHeaders h = new HttpHeaders();
    h.set("Idempotency-Key", key);
    return h;
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

  /**
   * POST helper: creates an order and asserts 201; returns the response body.
   */
  private OrderResponse createOrder(OrderRequest req) {
    ResponseEntity<OrderResponse> resp = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(resp.getBody()).isNotNull();
    return resp.getBody();
  }

  // ── Task 1.4 — @Order(1): POST happy path ────────────────────────────────

  @Test
  @Order(1)
  void post_happyPath_returns201WithLocationAndDebugIdHeader() {
    UUID customerId = UUID.randomUUID();
    OrderRequest req = validRequest(customerId);

    ResponseEntity<OrderResponse> response = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders()),
        OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(response.getHeaders().getLocation())
        .as("Location header must be set on 201 response")
        .isNotNull();

    OrderResponse body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.getId()).isNotNull();
    assertThat(body.getCustomerId()).isEqualTo(customerId);
    assertThat(body.getTotalAmount()).isEqualByComparingTo(new BigDecimal("49.00"));
    assertThat(body.getStatus()).isNotNull();
    assertThat(body.getCreatedAt()).isNotNull();
    assertThat(body.getUpdatedAt()).isNotNull();

    assertThat(response.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 201 response")
        .isNotBlank();
  }

  // ── Task 1.5 — @Order(2): GET by id ──────────────────────────────────────

  /**
   * GET by id: 200, returned body matches the created order.
   *
   * <p>Creates its own order inline — fully independent of @Order(1).
   * Because {@code @BeforeEach} truncates tables before every test,
   * any row from a previous test is always gone when this method runs.
   */
  @Test
  @Order(2)
  void get_byId_returns200WithMatchingOrder() {
    UUID customerId = UUID.randomUUID();
    OrderResponse created = createOrder(validRequest(customerId));

    ResponseEntity<OrderResponse> response = restTemplate.getForEntity(
        ORDERS_URL + "/" + created.getId(), OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    OrderResponse body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.getId()).isEqualTo(created.getId());
    assertThat(body.getCustomerId()).isEqualTo(customerId);
  }

  // ── Task 1.6 — @Order(3): GET unknown id ─────────────────────────────────

  @Test
  @Order(3)
  void get_unknownId_returns404() {
    ResponseEntity<String> response = restTemplate.getForEntity(
        ORDERS_URL + "/" + UUID.randomUUID(), String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  // ── Task 1.7 — @Order(4): GET list (no filter) ───────────────────────────

  @Test
  @Order(4)
  void get_list_noFilter_returns200WithPaginationFields() {
    createOrder(validRequest()); // seed at least one row

    ResponseEntity<OrderListResponse> response = restTemplate.getForEntity(
        ORDERS_URL, OrderListResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    OrderListResponse body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.getPage()).as("page field must be present").isNotNull();
    assertThat(body.getSize()).as("size field must be present").isNotNull();
    assertThat(body.getTotalElements()).as("totalElements must be present").isNotNull();
    assertThat(body.getTotalPages()).as("totalPages must be present").isNotNull();
    assertThat(body.getContent()).as("content array must be present").isNotNull();
  }

  // ── Task 1.8 — @Order(5): GET list filtered by customerId ────────────────

  @Test
  @Order(5)
  void get_list_filteredByCustomerId_returnsOnlyMatchingOrders() {
    UUID targetCustomer = UUID.randomUUID();
    UUID otherCustomer = UUID.randomUUID();

    createOrder(validRequest(targetCustomer));
    createOrder(validRequest(otherCustomer));

    ResponseEntity<OrderListResponse> response = restTemplate.getForEntity(
        ORDERS_URL + "?customerId=" + targetCustomer, OrderListResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    OrderListResponse body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.getContent())
        .isNotEmpty()
        .allMatch(o -> targetCustomer.equals(o.getCustomerId()),
            "every returned order must belong to the filtered customerId");
  }

  // ── Task 1.9 — @Order(6): PUT happy path ─────────────────────────────────

  @Test
  @Order(6)
  void put_happyPath_returns200WithUpdatedFields() {
    OrderResponse created = createOrder(validRequest());

    OrderRequest updateReq = new OrderRequest();
    updateReq.setCustomerId(created.getCustomerId());
    updateReq.setTotalAmount(new BigDecimal("199.00"));

    ResponseEntity<OrderResponse> response = restTemplate.exchange(
        ORDERS_URL + "/" + created.getId(),
        HttpMethod.PUT,
        new HttpEntity<>(updateReq, idempotencyHeaders()),
        OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    OrderResponse updated = response.getBody();
    assertThat(updated).isNotNull();
    assertThat(updated.getTotalAmount()).isEqualByComparingTo(new BigDecimal("199.00"));
    assertThat(updated.getUpdatedAt())
        .as("updatedAt must not be before original updatedAt")
        .isAfterOrEqualTo(created.getUpdatedAt());
  }

  // ── Task 1.10 — @Order(7): PUT unknown id ────────────────────────────────

  @Test
  @Order(7)
  void put_unknownId_returns404() {
    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL + "/" + UUID.randomUUID(),
        HttpMethod.PUT,
        new HttpEntity<>(validRequest(), idempotencyHeaders()),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  // ── Task 1.11 — @Order(8): DELETE happy path ─────────────────────────────

  @Test
  @Order(8)
  void delete_happyPath_returns204_subsequentGetReturns404() {
    OrderResponse created = createOrder(validRequest());

    ResponseEntity<Void> deleteResponse = restTemplate.exchange(
        ORDERS_URL + "/" + created.getId(),
        HttpMethod.DELETE,
        new HttpEntity<>(null, idempotencyHeaders()),
        Void.class);

    assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

    ResponseEntity<String> getAfterDelete = restTemplate.getForEntity(
        ORDERS_URL + "/" + created.getId(), String.class);
    assertThat(getAfterDelete.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  // ── Task 1.12 — @Order(9): DELETE unknown id ─────────────────────────────

  @Test
  @Order(9)
  void delete_unknownId_returns404() {
    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL + "/" + UUID.randomUUID(),
        HttpMethod.DELETE,
        new HttpEntity<>(null, idempotencyHeaders()),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }
}
