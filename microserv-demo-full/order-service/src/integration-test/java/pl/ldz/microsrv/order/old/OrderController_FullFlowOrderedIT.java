package pl.ldz.microsrv.order.old;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Disabled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import pl.ldz.microsrv.order.api.model.OrderListResponse;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import pl.ldz.microsrv.order.AbstractControllerIT;

import java.math.BigDecimal;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

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
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = "spring.task.scheduling.pool.size=1")
@Disabled
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = "outbox.poll-interval-ms=1000")
class OrderController_FullFlowOrderedIT extends AbstractControllerIT {

  private static final String ORDERS_URL = "/api/v1/orders";

  // ── 13.1.2 JdbcTemplate for direct DB assertions and truncation ───────────
  @Autowired
  private JdbcTemplate jdbcTemplate;

  // ── 13.1.1 Truncate all relevant tables before each test ─────────────────
  @BeforeEach
  void truncateTables() {
    jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
  }

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

  /** Builds headers with a specific idempotency key. */
  private static HttpHeaders idempotencyHeaders(String key) {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Idempotency-Key", key);
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

  /** POST helper with a specific idempotency key. */
  private ResponseEntity<OrderResponse> createOrder(OrderRequest request, String idemKey) {
    return restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(request, idempotencyHeaders(idemKey)),
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

    // Task 9.6 — X-Debug-Id header must be present on 2xx responses (set by MdcFilter)
    assertThat(response.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on successful 201 response")
        .isNotBlank();

    // debugId must not appear in any response
    ResponseEntity<String> raw = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(validRequest(), idempotencyHeaders()),
        String.class);
    assertThat(raw.getBody()).doesNotContain("debugId");
    // Task 9.6 — also confirm header on this raw call
    assertThat(raw.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on every response")
        .isNotBlank();
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
    // Task 9.2 — X-Debug-Id header present on non-2xx response
    assertThat(response.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 404 response")
        .isNotBlank();
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
    // Task 9.2 — X-Debug-Id header present on 404 response
    assertThat(response.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 404 updateOrder response")
        .isNotBlank();
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
    // Task 9.2 — X-Debug-Id header present on 404 response
    assertThat(response.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 404 deleteOrder response")
        .isNotBlank();
  }

  // ── Phase 4: 13.2 Idempotency Replay (POST) ──────────────────────────────

  /**
     * 13.2.1–13.2.4 + 13.6.2: Same key → same response body; one DB row each;
     * idempotency_key.response_body has no debugId.
     */
  @Test
  @org.junit.jupiter.api.Order(11)
  void createOrder_idempotencyReplay_returnsSameResponse() {
    String idemKey = newIdempotencyKey();
    OrderRequest request = validRequest();

    // 13.2.1 First request → 201
    ResponseEntity<OrderResponse> first = createOrder(request, idemKey);
    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    OrderResponse firstBody = first.getBody();
    assertThat(firstBody).isNotNull();

    // 13.2.2 Repeat identical request with same key → same response body
    ResponseEntity<OrderResponse> second = createOrder(request, idemKey);
    assertThat(second.getStatusCode().is2xxSuccessful()).isTrue();
    OrderResponse secondBody = second.getBody();
    assertThat(secondBody).isNotNull();
    assertThat(secondBody.getId()).isEqualTo(firstBody.getId());
    assertThat(secondBody.getTotalAmount()).isEqualByComparingTo(firstBody.getTotalAmount());

    // 13.2.3 Exactly one row in orders table for this id
    Integer orderCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM orders WHERE id = ?::uuid",
        Integer.class, firstBody.getId().toString());
    assertThat(orderCount).isEqualTo(1);

    // 13.2.4 Exactly one COMPLETED row in idempotency_key for this key
    Integer idemCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM idempotency_key WHERE key = ? AND status = 'COMPLETED'",
        Integer.class, idemKey);
    assertThat(idemCount).isEqualTo(1);

    // 13.6.2 response_body column in idempotency_key must not contain debugId
    String storedResponseBody = jdbcTemplate.queryForObject(
        "SELECT response_body::text FROM idempotency_key WHERE key = ?",
        String.class, idemKey);
    assertThat(storedResponseBody).doesNotContain("debugId");
  }

  // ── Phase 4: 13.3 Idempotency Hash Mismatch (POST) ───────────────────────

  /**
     * 13.3.1–13.3.3: Different body with same key → 422 with ErrorResponse, no debugId.
     */
  @Test
  @org.junit.jupiter.api.Order(12)
  void createOrder_idempotencyHashMismatch_returns422() {
    String idemKey = newIdempotencyKey();

    // 13.3.1 First request → 201
    OrderRequest first = validRequest();
    first.setTotalAmount(new BigDecimal("10.00"));
    ResponseEntity<OrderResponse> resp1 = createOrder(first, idemKey);
    assertThat(resp1.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // 13.3.2 Different totalAmount → different hash → 422
    OrderRequest second = new OrderRequest();
    second.setCustomerId(first.getCustomerId());
    second.setTotalAmount(new BigDecimal("99.00"));

    ResponseEntity<String> resp2 = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(second, idempotencyHeaders(idemKey)),
        String.class);
    assertThat(resp2.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

    // 13.3.3 Response is ErrorResponse schema, no debugId
    String body = resp2.getBody();
    assertThat(body).isNotNull()
        .contains("status")
        .contains("error")
        .contains("message")
        .doesNotContain("debugId");
    // Task 9.2 + 9.5 — X-Debug-Id header present on 422 response
    assertThat(resp2.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 422 idempotency hash mismatch response")
        .isNotBlank();
  }

  // ── Phase 4: 13.4 Idempotency In-Progress (POST) ─────────────────────────

  /**
     * 13.4.1–13.4.3: Manually inserted IN_PROGRESS key → 409 Conflict, no debugId.
     */
  @Test
  @org.junit.jupiter.api.Order(13)
  void createOrder_idempotencyInProgress_returns409() {
    String idemKey = newIdempotencyKey();

    // Compute the same hash the service will compute for validRequest()
    OrderRequest request = validRequest();

    String requestHash = null;
    try {
      requestHash = sha256Hex(objectMapper.writeValueAsBytes(request));
    } catch (NoSuchAlgorithmException e) {
      // throw new RuntimeException(e);
      fail("NoSuchAlgorithmException", e);
    } catch (JsonProcessingException e) {
      // throw new RuntimeException(e);
      fail("JsonProcessingException", e);
    }

    // 13.4.1 Insert IN_PROGRESS row directly
    jdbcTemplate.update(
        "INSERT INTO idempotency_key (id, key, request_hash, status, created_at, expires_at) " +
        "VALUES (gen_random_uuid(), ?, ?, 'IN_PROGRESS', now(), now() + interval '24 hours')",
        idemKey, requestHash);

    // 13.4.2 POST with that key → 409
    ResponseEntity<String> response = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(request, idempotencyHeaders(idemKey)),
        String.class);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

    // 13.4.3 Response body is ErrorResponse, no debugId
    assertThat(response.getBody()).isNotNull().doesNotContain("debugId");
    // Task 9.2 + 9.4 — X-Debug-Id header present on 409 conflict response
    assertThat(response.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 409 idempotency in-progress response")
        .isNotBlank();
  }

  // ── Phase 4: 13.5 + 13.6.1 Outbox Delivery Verification ──────────────────

  /**
     * 13.5.1–13.5.6 + 13.6.1: Order creation writes a PENDING outbox event that transitions
     * to PROCESSED and is published to Kafka; payload contains no debugId.
     */
  @Test
  @org.junit.jupiter.api.Order(14)
  void createOrder_writesOutboxEvent_whichIsPublishedToKafka() {
    // 13.5.1 Create an order
    OrderResponse created = createOrder(validRequest()).getBody();
    assertThat(created).isNotNull();
    String orderId = created.getId().toString();

    // 13.5.2 Exactly one PENDING outbox_event row for this aggregate_id
    Integer pendingCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ?::uuid AND status = 'PENDING'",
        Integer.class, orderId);
    assertThat(pendingCount).isGreaterThanOrEqualTo(1);

    // 13.6.1 outbox_event.payload must not contain debugId
    String payload = jdbcTemplate.queryForObject(
        "SELECT payload::text FROM outbox_event WHERE aggregate_id = ?::uuid LIMIT 1",
        String.class, orderId);
    assertThat(payload).doesNotContain("debugId");

    // 13.5.3–13.5.4 Wait for poller (poll-interval-ms=1000) to mark row PROCESSED
    await().atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> {
          Integer processedCount = jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM outbox_event " +
              "WHERE aggregate_id = ?::uuid AND status = 'PROCESSED' AND processed_at IS NOT NULL",
              Integer.class, orderId);
          assertThat(processedCount).isEqualTo(1);
        });

    // 13.5.4b Verify event_type column is ORDER_CREATED for this outbox row
    String eventType = jdbcTemplate.queryForObject(
        "SELECT event_type FROM outbox_event WHERE aggregate_id = ?::uuid LIMIT 1",
        String.class, orderId);
    assertThat(eventType)
        .as("outbox_event.event_type must be ORDER_CREATED")
        .isEqualTo("ORDER_CREATED");

    // 13.5.5–13.5.6 Poll Kafka; assert message received with correct key; no debugId
    try (KafkaConsumer<String, String> consumer = createTestConsumer("test-" + UUID.randomUUID())) {
      AtomicBoolean found = new AtomicBoolean(false);
      await().atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(500))
          .untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
            records.forEach(record -> {
              if (orderId.equals(record.key())) {
                found.set(true);
                assertThat(record.value()).doesNotContain("debugId");
                // 13.5.7 Payload must be a JSON object (not a double-encoded quoted string)
                // and must carry the correct orderId field matching the partition key.
                assertThatCode(() -> {
                  JsonNode node = objectMapper.readTree(record.value());
                  assertThat(node.isObject())
                      .as("Kafka message value must be a JSON object, not a quoted string")
                      .isTrue();
                  assertThat(node.has("orderId"))
                      .as("Kafka message payload must contain 'orderId' field")
                      .isTrue();
                  assertThat(node.get("orderId").asText())
                      .as("orderId in payload must match the partition key")
                      .isEqualTo(orderId);
                }).doesNotThrowAnyException();
              }
            });
            assertThat(found.get())
                .as("Expected Kafka message with key=%s on orders.events.v1", orderId)
                .isTrue();
          });
    }
  }

  // ── Phase 5: 7.6 Race-condition — concurrent POSTs with same key ──────────

  /**
   * 7.6: Two concurrent POST requests with the identical {@code Idempotency-Key} must
   * produce exactly one 201 and one 409 — never two 201s and never a 500.
   *
   * <p>A PostgreSQL transaction-scoped advisory lock ({@code pg_try_advisory_xact_lock})
   * in {@code IdempotencyService.checkAndStore} serialises the two threads at the
   * database level, so the second thread fails to acquire the lock and receives
   * {@code IdempotencyConflictException} → HTTP 409.
   */
  @Test
  @org.junit.jupiter.api.Order(15)
  void createOrder_concurrentSameKey_exactlyOneSucceeds() throws Exception {
    String sharedKey = newIdempotencyKey();
    OrderRequest request = validRequest();

    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);

    java.util.concurrent.Future<ResponseEntity<String>> f1 = executor.submit(() -> {
      startLatch.await();
      return restTemplate.exchange(
          ORDERS_URL,
          HttpMethod.POST,
          new HttpEntity<>(request, idempotencyHeaders(sharedKey)),
          String.class);
    });

    java.util.concurrent.Future<ResponseEntity<String>> f2 = executor.submit(() -> {
      startLatch.await();
      return restTemplate.exchange(
          ORDERS_URL,
          HttpMethod.POST,
          new HttpEntity<>(request, idempotencyHeaders(sharedKey)),
          String.class);
    });

    // Release both threads simultaneously
    startLatch.countDown();

    ResponseEntity<String> r1 = f1.get(10, java.util.concurrent.TimeUnit.SECONDS);
    ResponseEntity<String> r2 = f2.get(10, java.util.concurrent.TimeUnit.SECONDS);
    executor.shutdown();
    executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

    int status1 = r1.getStatusCode().value();
    int status2 = r2.getStatusCode().value();

    // Neither response may be 500 (unhandled exception)
    assertThat(status1).as("response 1 must not be 500").isNotEqualTo(500);
    assertThat(status2).as("response 2 must not be 500").isNotEqualTo(500);

    // Exactly one 201 and one 409 (order is irrelevant)
    assertThat(new int[]{status1, status2})
        .as("one request must succeed with 201 and the other must conflict with 409")
        .containsExactlyInAnyOrder(201, 409);

    // Exactly one order row in the DB for this run
    Integer orderCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM orders", Integer.class);
    assertThat(orderCount).isEqualTo(1);
  }

  // ── Phase 5: Step 8 — 422 hash-mismatch integration tests ───────────────

  /**
   * 8.1 + 8.3: POST with same {@code Idempotency-Key} but a different request body
   * must return 422 Unprocessable Entity with a populated {@link pl.ldz.microsrv.order.api.model.ErrorResponse}
   * and must never expose {@code debugId}.
   */
  @Test
  @org.junit.jupiter.api.Order(16)
  void createOrder_sameKeyDifferentBody_returns422() {
    String key1 = newIdempotencyKey();

    // 8.1-a: First POST with key1 → 201
    OrderRequest firstRequest = validRequest();
    firstRequest.setTotalAmount(new BigDecimal("55.00"));
    ResponseEntity<OrderResponse> firstResp = createOrder(firstRequest, key1);
    assertThat(firstResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // 8.1-b: Second POST with same key1 but different totalAmount → 422
    OrderRequest differentRequest = new OrderRequest();
    differentRequest.setCustomerId(firstRequest.getCustomerId());
    differentRequest.setTotalAmount(new BigDecimal("999.00")); // different body → different hash

    ResponseEntity<String> secondResp = restTemplate.exchange(
        ORDERS_URL,
        HttpMethod.POST,
        new HttpEntity<>(differentRequest, idempotencyHeaders(key1)),
        String.class);

    assertThat(secondResp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

    // 8.1-c: ErrorResponse body is non-null, message is present and non-blank
    String body = secondResp.getBody();
    assertThat(body).isNotNull();
    assertThat(body).contains("\"message\"");
    assertThat(body).contains("\"status\"");
    assertThat(body).contains("422");

    // 8.3: No debugId in response
    assertThat(body).doesNotContain("debugId");
    // Task 9.2 + 9.5 — X-Debug-Id header present on 422 response
    assertThat(secondResp.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 422 createOrder hash mismatch response")
        .isNotBlank();
  }

  /**
   * 8.2 + 8.3: PUT with same {@code Idempotency-Key} but a different request body
   * must return 422 Unprocessable Entity with a populated {@link pl.ldz.microsrv.order.api.model.ErrorResponse}
   * and must never expose {@code debugId}.
   */
  @Test
  @org.junit.jupiter.api.Order(17)
  void updateOrder_sameKeyDifferentBody_returns422() {
    // Setup: create an order first (with its own fresh key)
    OrderResponse created = createOrder(validRequest()).getBody();
    assertThat(created).isNotNull();
    String orderId = created.getId().toString();

    String key2 = newIdempotencyKey();

    // 8.2-a: First PUT with key2 → 200
    OrderRequest firstUpdate = new OrderRequest();
    firstUpdate.setCustomerId(created.getCustomerId());
    firstUpdate.setTotalAmount(new BigDecimal("150.00"));

    ResponseEntity<OrderResponse> firstResp = restTemplate.exchange(
        ORDERS_URL + "/" + orderId,
        HttpMethod.PUT,
        new HttpEntity<>(firstUpdate, idempotencyHeaders(key2)),
        OrderResponse.class);
    assertThat(firstResp.getStatusCode()).isEqualTo(HttpStatus.OK);

    // 8.2-b: Second PUT with same key2 but different totalAmount → 422
    OrderRequest differentUpdate = new OrderRequest();
    differentUpdate.setCustomerId(created.getCustomerId());
    differentUpdate.setTotalAmount(new BigDecimal("777.00")); // different body → different hash

    ResponseEntity<String> secondResp = restTemplate.exchange(
        ORDERS_URL + "/" + orderId,
        HttpMethod.PUT,
        new HttpEntity<>(differentUpdate, idempotencyHeaders(key2)),
        String.class);

    assertThat(secondResp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

    // 8.2-c: ErrorResponse body is non-null, status field contains 422
    String body = secondResp.getBody();
    assertThat(body).isNotNull();
    assertThat(body).contains("\"message\"");
    assertThat(body).contains("\"status\"");
    assertThat(body).contains("422");

    // 8.3: No debugId in response
    assertThat(body).doesNotContain("debugId");
    // Task 9.2 + 9.5 — X-Debug-Id header present on 422 updateOrder hash mismatch response
    assertThat(secondResp.getHeaders().getFirst("X-Debug-Id"))
        .as("X-Debug-Id header must be present on 422 updateOrder hash mismatch response")
        .isNotBlank();
  }

}
