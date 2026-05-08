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
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Class 3 — {@code IdempotencyService} end-to-end: replay, hash mismatch,
 * in-progress lock, and concurrent first-write serialisation.
 *
 * <p>Uses {@link AbstractControllerIT} (Postgres + Kafka Testcontainers singletons).
 *
 * <p>{@code @TestMethodOrder} is justified here because replay tests require a prior
 * request to have already stored a key. The concurrent POST test (@Order(8)) is
 * placed last to avoid leaving dirty state for the simpler ordered assertions.
 *
 * <p>Corresponds to tasks 3.1 – 3.11 in {@code prompts/mini-tasks-01.md}.
 */
// ── Task 3.1 ─────────────────────────────────────────────────────────────────
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderController_IdempotencyOrderedIT extends AbstractControllerIT {

  private static final String ORDERS_URL = "/api/v1/orders";

  // ── Task 3.2 ─────────────────────────────────────────────────────────────

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() {
    jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
  }

  // ── Task 3.3 — Helpers ────────────────────────────────────────────────────

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

  /**
   * Builds {@link HttpHeaders} containing the given {@code Idempotency-Key}.
   */
  private static HttpHeaders idempotencyHeaders(String key) {
    HttpHeaders h = new HttpHeaders();
    h.set("Idempotency-Key", key);
    return h;
  }

  /**
   * Creates a valid {@link OrderRequest} with a random {@code customerId}.
   */
  private static OrderRequest validRequest() {
    OrderRequest req = new OrderRequest();
    req.setCustomerId(UUID.randomUUID());
    req.setTotalAmount(new BigDecimal("49.99"));
    return req;
  }

  // ── Task 3.4 — @Order(1): POST same key twice, same body → both 201, one row ──

  /**
   * POST with the same {@code Idempotency-Key} and identical body twice.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Both responses are 201 Created.</li>
   *   <li>Response bodies (orderId, customerId, totalAmount) are identical.</li>
   *   <li>Exactly one row exists in the {@code orders} table.</li>
   *   <li>Exactly one {@code COMPLETED} record exists in {@code idempotency_key}.</li>
   * </ul>
   */
  @Test
  @Order(1)
  void post_sameKeyTwice_sameBody_bothReturn201_oneOrderRow() {
    String key = newIdempotencyKey();
    OrderRequest req = validRequest();

    ResponseEntity<OrderResponse> first = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders(key)),
        OrderResponse.class);

    ResponseEntity<OrderResponse> second = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders(key)),
        OrderResponse.class);

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    OrderResponse body1 = first.getBody();
    OrderResponse body2 = second.getBody();
    assertThat(body1).isNotNull();
    assertThat(body2).isNotNull();

    assertThat(body2.getId())
        .as("Replay must return the same orderId")
        .isEqualTo(body1.getId());
    assertThat(body2.getCustomerId())
        .as("Replay must return the same customerId")
        .isEqualTo(body1.getCustomerId());
    assertThat(body2.getTotalAmount())
        .as("Replay must return the same totalAmount")
        .isEqualByComparingTo(body1.getTotalAmount());

    Integer orderCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM orders", Integer.class);
    assertThat(orderCount)
        .as("Exactly one order row must exist after idempotent replay")
        .isEqualTo(1);

    Integer completedKeyCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM idempotency_key WHERE key = ? AND status = 'COMPLETED'",
        Integer.class, key);
    assertThat(completedKeyCount)
        .as("Exactly one COMPLETED idempotency record must exist")
        .isEqualTo(1);
  }

  // ── Task 3.5 — @Order(2): POST same key, different body → 422 ────────────

  /**
   * POST with the same {@code Idempotency-Key} but a different body must return
   * 422 Unprocessable Entity (hash mismatch).
   *
   * <p>Assertions:
   * <ul>
   *   <li>Second request returns 422.</li>
   *   <li>Response body carries the {@code ErrorResponse} schema fields.</li>
   *   <li>{@code debugId} is absent from the response body.</li>
   * </ul>
   */
  @Test
  @Order(2)
  void post_sameKey_differentBody_returns422() {
    String key = newIdempotencyKey();

    OrderRequest first = new OrderRequest();
    first.setCustomerId(UUID.randomUUID());
    first.setTotalAmount(new BigDecimal("10.00"));
    restTemplate.exchange(ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(first, idempotencyHeaders(key)), OrderResponse.class);

    OrderRequest second = new OrderRequest();
    second.setCustomerId(first.getCustomerId());
    second.setTotalAmount(new BigDecimal("99.99")); // different hash

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(second, idempotencyHeaders(key)),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

    String body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body).as("422 body must contain 'status'").contains("\"status\"");
    assertThat(body).as("422 body must contain 'message'").contains("\"message\"");
    assertThat(body).as("debugId must not leak into error body").doesNotContain("debugId");
  }

  // ── Task 3.6 — @Order(3): POST with key pre-inserted as IN_PROGRESS → 409 ─

  /**
   * POST with a key that is already stored as {@code IN_PROGRESS} must return
   * 409 Conflict (another request is still processing the same key).
   *
   * <p>Simulates the race by directly inserting an {@code IN_PROGRESS} row into
   * {@code idempotency_key} before the request arrives.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Response is 409 Conflict.</li>
   *   <li>{@code debugId} is absent from the error body.</li>
   * </ul>
   */
  @Test
  @Order(3)
  void post_keyPreInsertedAsInProgress_returns409() throws Exception {
    String key = newIdempotencyKey();
    // Use the SAME object for hash computation and the HTTP request.
    // validRequest() generates a random UUID each call, so calling it twice
    // produces two different objects with different hashes — the service would
    // then see a mismatch and return 422 instead of the expected 409.
    OrderRequest req = validRequest();

    // The service checks hash mismatch BEFORE checking the IN_PROGRESS status,
    // so the pre-inserted row must carry the real SHA-256 of the serialised request.
    byte[] reqJson = objectMapper.writeValueAsBytes(req);
    java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
    String realHash = java.util.HexFormat.of().formatHex(md.digest(reqJson));

    jdbcTemplate.update(
        "INSERT INTO idempotency_key(id, key, request_hash, status, created_at, expires_at) "
            + "VALUES (gen_random_uuid(), ?, ?, 'IN_PROGRESS', now(), now() + interval '1 day')",
        key, realHash);

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders(key)),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

    String body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body).as("debugId must not leak into error body").doesNotContain("debugId");
  }

  // ── Task 3.7 — @Order(4): PUT same key twice, same body → both 200, same body ─

  /**
   * PUT with the same {@code Idempotency-Key} and identical body twice.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Both responses are 200 OK.</li>
   *   <li>Response bodies are identical (same orderId, totalAmount, etc.).</li>
   * </ul>
   */
  @Test
  @Order(4)
  void put_sameKeyTwice_sameBody_bothReturn200_responsesIdentical() {
    // Create an order to update
    OrderRequest createReq = validRequest();
    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    String putKey = newIdempotencyKey();
    OrderRequest updateReq = new OrderRequest();
    updateReq.setCustomerId(createReq.getCustomerId());
    updateReq.setTotalAmount(new BigDecimal("199.00"));

    ResponseEntity<OrderResponse> first = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.PUT,
        new HttpEntity<>(updateReq, idempotencyHeaders(putKey)),
        OrderResponse.class);

    ResponseEntity<OrderResponse> second = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.PUT,
        new HttpEntity<>(updateReq, idempotencyHeaders(putKey)),
        OrderResponse.class);

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);

    OrderResponse body1 = first.getBody();
    OrderResponse body2 = second.getBody();
    assertThat(body1).isNotNull();
    assertThat(body2).isNotNull();

    assertThat(body2.getId()).isEqualTo(body1.getId());
    assertThat(body2.getTotalAmount()).isEqualByComparingTo(body1.getTotalAmount());
  }

  // ── Task 3.8 — @Order(5): PUT same key, different body → 422 ─────────────

  /**
   * PUT with the same {@code Idempotency-Key} but a different body must return
   * 422 Unprocessable Entity (hash mismatch).
   */
  @Test
  @Order(5)
  void put_sameKey_differentBody_returns422() {
    // Create an order to update
    OrderRequest createReq = validRequest();
    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    String putKey = newIdempotencyKey();

    OrderRequest firstUpdate = new OrderRequest();
    firstUpdate.setCustomerId(createReq.getCustomerId());
    firstUpdate.setTotalAmount(new BigDecimal("100.00"));
    restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.PUT,
        new HttpEntity<>(firstUpdate, idempotencyHeaders(putKey)),
        OrderResponse.class);

    OrderRequest secondUpdate = new OrderRequest();
    secondUpdate.setCustomerId(createReq.getCustomerId());
    secondUpdate.setTotalAmount(new BigDecimal("200.00")); // different hash

    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.PUT,
        new HttpEntity<>(secondUpdate, idempotencyHeaders(putKey)),
        String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

  // ── Task 3.9 — @Order(6): DELETE same key twice → both 204 ───────────────

  /**
   * DELETE with the same {@code Idempotency-Key} twice.
   *
   * <p>The first DELETE performs the soft-delete (204). The second is an idempotency
   * replay that also returns 204 (no-op — the stored response is replayed).
   */
  @Test
  @Order(6)
  void delete_sameKeyTwice_bothReturn204() {
    // Create an order to delete
    OrderRequest createReq = validRequest();
    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    String deleteKey = newIdempotencyKey();

    ResponseEntity<Void> first = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.DELETE,
        new HttpEntity<>(null, idempotencyHeaders(deleteKey)),
        Void.class);

    ResponseEntity<Void> second = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.DELETE,
        new HttpEntity<>(null, idempotencyHeaders(deleteKey)),
        Void.class);

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
  }

  // ── Task 3.10 — @Order(7): response_body in idempotency_key must not contain debugId ─

  /**
   * Verifies that the {@code response_body} column stored in the {@code idempotency_key}
   * table does not contain the string {@code "debugId"}.
   *
   * <p>Creates one order (POST) and one update (PUT), then queries the DB directly
   * to assert the stored blobs are clean.
   */
  @Test
  @Order(7)
  void idempotencyResponseBody_doesNotContainDebugId() {
    // POST
    OrderRequest createReq = validRequest();
    String postKey = newIdempotencyKey();
    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders(postKey)),
        OrderResponse.class);
    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    // PUT
    String putKey = newIdempotencyKey();
    OrderRequest updateReq = new OrderRequest();
    updateReq.setCustomerId(createReq.getCustomerId());
    updateReq.setTotalAmount(new BigDecimal("77.00"));
    restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.PUT,
        new HttpEntity<>(updateReq, idempotencyHeaders(putKey)),
        OrderResponse.class);

    // Assert neither stored response body contains "debugId"
    List<Map<String, Object>> rows = jdbcTemplate.queryForList(
        "SELECT key, response_body FROM idempotency_key WHERE key IN (?, ?)",
        postKey, putKey);

    assertThat(rows).hasSize(2);
    for (Map<String, Object> row : rows) {
      Object responseBody = row.get("response_body");
      if (responseBody != null) {
        assertThat(responseBody.toString())
            .as("response_body for key '%s' must not contain 'debugId'", row.get("key"))
            .doesNotContain("debugId");
      }
    }
  }

  // ── Task 3.11 — @Order(8): Concurrent POST same key → exactly one 201, one 409 ─

  /**
   * Two threads fire POST requests with the same {@code Idempotency-Key} simultaneously.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Exactly one response is 201 Created.</li>
   *   <li>Exactly one response is 409 Conflict.</li>
   *   <li>Exactly one row exists in {@code orders}.</li>
   *   <li>No 500 responses are returned.</li>
   * </ul>
   *
   * <p>Placed last in the ordered sequence to avoid leaving dirty concurrent state
   * (orphaned {@code IN_PROGRESS} rows, partial order inserts) for earlier tests.
   */
  @Test
  @Order(8)
  void post_concurrentSameKey_exactlyOne201_oneConflict_oneOrderRow() throws Exception {
    String sharedKey = newIdempotencyKey();
    OrderRequest req = validRequest();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Callable<ResponseEntity<String>> task = () -> restTemplate.exchange(
          ORDERS_URL, HttpMethod.POST,
          new HttpEntity<>(req, idempotencyHeaders(sharedKey)),
          String.class);

      Future<ResponseEntity<String>> f1 = executor.submit(task);
      Future<ResponseEntity<String>> f2 = executor.submit(task);

      ResponseEntity<String> r1 = f1.get();
      ResponseEntity<String> r2 = f2.get();

      List<HttpStatus> statuses = new ArrayList<>();
      statuses.add((HttpStatus) r1.getStatusCode());
      statuses.add((HttpStatus) r2.getStatusCode());

      assertThat(statuses)
          .as("No 500 responses allowed on concurrent duplicate key")
          .noneMatch(s -> s == HttpStatus.INTERNAL_SERVER_ERROR);

      long createdCount = statuses.stream()
          .filter(s -> s == HttpStatus.CREATED).count();
      long conflictCount = statuses.stream()
          .filter(s -> s == HttpStatus.CONFLICT).count();

      assertThat(createdCount)
          .as("Exactly one 201 Created expected")
          .isEqualTo(1);
      assertThat(conflictCount)
          .as("Exactly one 409 Conflict expected")
          .isEqualTo(1);

      Integer orderCount = jdbcTemplate.queryForObject(
          "SELECT COUNT(*) FROM orders", Integer.class);
      assertThat(orderCount)
          .as("Exactly one order row must exist after concurrent duplicate key")
          .isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }
}
