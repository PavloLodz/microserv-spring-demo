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
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Class 2 — {@code GlobalExceptionHandler} surface: Bean Validation failures
 * and the guarantee that {@code debugId} never leaks into any error response body.
 *
 * <p>Every test in this class sends a deliberately malformed request and verifies:
 * <ul>
 *   <li>The correct HTTP status code is returned.</li>
 *   <li>Where relevant, the error message names the offending field.</li>
 *   <li>{@code debugId} is absent from the response body.</li>
 *   <li>The {@link pl.ldz.microsrv.order.api.model.ErrorResponse} envelope fields
 *       ({@code status}, {@code message}) are present.</li>
 * </ul>
 *
 * <p>Corresponds to tasks 2.1 – 2.9 in {@code prompts/mini-tasks-01.md}.
 */
// ── Task 2.1 ─────────────────────────────────────────────────────────────────
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderController_ValidationOrderedIT extends AbstractControllerIT {

  private static final String ORDERS_URL = "/api/v1/orders";

  // ── Task 2.2 ─────────────────────────────────────────────────────────────

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() {
    jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
  }

  // ── Task 2.3 — Helpers ────────────────────────────────────────────────────

  /**
   * Returns a fresh random {@code Idempotency-Key} value.
   */
  private static String newIdempotencyKey() {
    return UUID.randomUUID().toString();
  }

  /**
   * Builds {@link HttpHeaders} containing a single fresh {@code Idempotency-Key}.
   */
  private static HttpHeaders idempotencyHeaders() {
    HttpHeaders h = new HttpHeaders();
    h.set("Idempotency-Key", newIdempotencyKey());
    return h;
  }

  // ── Task 2.4 — @Order(1): POST null body → 400 ───────────────────────────

  /**
   * POST with a null / missing body must return 400 Bad Request.
   *
   * <p>Spring will reject the request before it reaches any service logic because
   * the {@code requestBody} is declared {@code required: true} in the OpenAPI spec.
   */
  @Test
  @Order(1)
  void post_nullBody_returns400() {
    // Send a POST with an explicitly null body but a valid Idempotency-Key header
    HttpHeaders headers = idempotencyHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(null, headers),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  // ── Task 2.5 — @Order(2): POST missing customerId → 400 + field name ─────

  /**
   * POST with a body that omits the required {@code customerId} field must return
   * 400 Bad Request and the error message must mention {@code customerId}.
   */
  @Test
  @Order(2)
  void post_missingCustomerId_returns400WithFieldName() {
    // Build a request with totalAmount only — customerId is absent
    OrderRequest req = new OrderRequest();
    req.setTotalAmount(new BigDecimal("25.00"));
    // customerId intentionally not set

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders()),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

    String body = response.getBody();
    assertThat(body)
        .as("Error message must reference the offending field 'customerId'")
        .contains("customerId");
  }

  // ── Task 2.6 — @Order(3): POST missing Idempotency-Key header → 400 ──────

  /**
   * POST without the required {@code Idempotency-Key} header must return 400 Bad Request.
   *
   * <p>The header is declared {@code required: true} for POST in the OpenAPI spec.
   * Spring's generated interface binding raises
   * {@code MissingRequestHeaderException} before business logic runs, which the
   * {@code GlobalExceptionHandler} (or Spring's default error resolver) maps to 400.
   */
  @Test
  @Order(3)
  void post_missingIdempotencyKeyHeader_returns400() {
    OrderRequest req = new OrderRequest();
    req.setCustomerId(UUID.randomUUID());
    req.setTotalAmount(new BigDecimal("10.00"));

    // No Idempotency-Key header — only Content-Type
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(req, headers),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  // ── Task 2.7 — @Order(4): PUT null body → 400 ────────────────────────────

  /**
   * PUT with a null body must return 400 Bad Request.
   *
   * <p>Creates a valid order first, then attempts to update it with no body.
   */
  @Test
  @Order(4)
  void put_nullBody_returns400() {
    // Create a valid order to have a real id to PUT against
    OrderRequest createReq = new OrderRequest();
    createReq.setCustomerId(UUID.randomUUID());
    createReq.setTotalAmount(new BigDecimal("50.00"));

    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);

    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    // PUT with null body
    HttpHeaders headers = idempotencyHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL + "/" + orderId,
        HttpMethod.PUT,
        new HttpEntity<>(null, headers),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  // ── Task 2.8 — @Order(5): PUT missing customerId → 400 + field name ──────

  /**
   * PUT with a body that omits the required {@code customerId} field must return
   * 400 Bad Request and the error message must mention {@code customerId}.
   */
  @Test
  @Order(5)
  void put_missingCustomerId_returns400WithFieldName() {
    // Create a valid order first
    OrderRequest createReq = new OrderRequest();
    createReq.setCustomerId(UUID.randomUUID());
    createReq.setTotalAmount(new BigDecimal("75.00"));

    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);

    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    // PUT with totalAmount only — customerId absent
    OrderRequest updateReq = new OrderRequest();
    updateReq.setTotalAmount(new BigDecimal("80.00"));
    // customerId intentionally not set

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL + "/" + orderId,
        HttpMethod.PUT,
        new HttpEntity<>(updateReq, idempotencyHeaders()),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

    String body = response.getBody();
    assertThat(body)
        .as("Error message must reference the offending field 'customerId'")
        .contains("customerId");
  }

  // ── Task 2.9 — @Order(6): error response schema & no debugId leak ─────────

  /**
   * Every error response must:
   * <ul>
   *   <li>Not contain {@code debugId} in the body (internal identifier must never leak).</li>
   *   <li>Contain the {@code ErrorResponse} schema fields {@code status} and {@code message}.</li>
   * </ul>
   *
   * <p>This test exercises a 400 and a 404 to confirm the invariant holds across
   * the two most common error paths in this class.
   */
  @Test
  @Order(6)
  void errorResponse_noDebugIdLeak_andSchemaFieldsPresent() {
    // ── 400 path: POST with missing customerId ────────────────────────────
    OrderRequest badReq = new OrderRequest();
    badReq.setTotalAmount(new BigDecimal("1.00"));

    ResponseEntity<String> bad400 = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(badReq, idempotencyHeaders()),
        String.class);

    assertThat(bad400.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    String body400 = bad400.getBody();
    assertThat(body400)
        .as("400 response body must not contain 'debugId'")
        .doesNotContain("debugId");
    assertThat(body400)
        .as("400 response body must contain 'status' field")
        .contains("\"status\"");
    assertThat(body400)
        .as("400 response body must contain 'message' field")
        .contains("\"message\"");

    // ── 404 path: GET unknown id ──────────────────────────────────────────
    ResponseEntity<String> bad404 = restTemplate.getForEntity(
        ORDERS_URL + "/" + UUID.randomUUID(), String.class);

    assertThat(bad404.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    String body404 = bad404.getBody();
    assertThat(body404)
        .as("404 response body must not contain 'debugId'")
        .doesNotContain("debugId");
    assertThat(body404)
        .as("404 response body must contain 'status' field")
        .contains("\"status\"");
    assertThat(body404)
        .as("404 response body must contain 'message' field")
        .contains("\"message\"");

    // ── 422 path: POST same key, different body (idempotency mismatch) ────
    String idemKey = newIdempotencyKey();

    OrderRequest first = new OrderRequest();
    first.setCustomerId(UUID.randomUUID());
    first.setTotalAmount(new BigDecimal("10.00"));
    restTemplate.exchange(ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(first, headersWithKey(idemKey)), OrderResponse.class);

    OrderRequest second = new OrderRequest();
    second.setCustomerId(first.getCustomerId());
    second.setTotalAmount(new BigDecimal("99.00")); // different hash

    ResponseEntity<String> bad422 = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(second, headersWithKey(idemKey)),
        String.class);

    assertThat(bad422.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    String body422 = bad422.getBody();
    assertThat(body422)
        .as("422 response body must not contain 'debugId'")
        .doesNotContain("debugId");
    assertThat(body422)
        .as("422 response body must contain 'status' field")
        .contains("\"status\"");
    assertThat(body422)
        .as("422 response body must contain 'message' field")
        .contains("\"message\"");
  }

  // ── Task 2.10 — @Order(7): POST negative totalAmount → 400 ──────────────

  /**
   * POST with a negative {@code totalAmount} must return 400 Bad Request.
   *
   * <p>The {@code minimum: 0.01} constraint on {@code OrderRequest.totalAmount} in
   * {@code orders-api.yaml} causes the OpenAPI Generator to emit a
   * {@code @DecimalMin("0.01")} annotation on the generated field. Spring's Bean
   * Validation picks this up and rejects the request before any service logic runs.
   *
   * <p>Covers both negative values and zero (both are below the 0.01 minimum).
   */
  @Test
  @Order(7)
  void post_negativeTotalAmount_returns400() {
    OrderRequest req = new OrderRequest();
    req.setCustomerId(UUID.randomUUID());
    req.setTotalAmount(new BigDecimal("-1.00")); // below minimum: 0.01

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders()),
        String.class);

    assertThat(response.getStatusCode())
        .as("Negative totalAmount must be rejected with 400 Bad Request")
        .isEqualTo(HttpStatus.BAD_REQUEST);

    // Zero is also below the minimum
    OrderRequest zeroReq = new OrderRequest();
    zeroReq.setCustomerId(UUID.randomUUID());
    zeroReq.setTotalAmount(BigDecimal.ZERO);

    ResponseEntity<String> zeroResponse = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(zeroReq, idempotencyHeaders()),
        String.class);

    assertThat(zeroResponse.getStatusCode())
        .as("Zero totalAmount must be rejected with 400 Bad Request")
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }

  // ── Private helper ────────────────────────────────────────────────────────

  private static HttpHeaders headersWithKey(String key) {
    HttpHeaders h = new HttpHeaders();
    h.set("Idempotency-Key", key);
    return h;
  }
}
