package pl.ldz.microsrv.order;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.springframework.test.context.TestPropertySource;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Class 4 — Kafka event delivery via the outbox pattern.
 *
 * <p>Verifies that every write operation ({@code POST}, {@code PUT}, {@code DELETE})
 * publishes the correct domain event to {@code orders.events.v1}, that the outbox
 * row transitions from {@code PENDING} to {@code PROCESSED}, and that idempotency
 * replay does not cause duplicate events.
 *
 * <p>Uses a real {@link KafkaConsumer} (via {@link #createTestConsumer(String)})
 * and Awaitility to poll until the message arrives or the timeout expires.
 *
 * <p>{@code outbox.poll-interval-ms=1000} is set via {@code @TestPropertySource}
 * so the background poller runs every second.
 *
 * <h3>Timeout rationale (task 6.5)</h3>
 * <p>The original 10 s Awaitility timeout was reviewed against CI environment
 * characteristics. The timeout has been raised to {@value #AWAIT_TIMEOUT_SECONDS} s
 * ({@value #OUTBOX_POLL_INTERVAL_MS} ms server poll interval × 30 cycles) so that
 * slow CI runners — which may experience JVM warm-up delays, container start
 * latency, or CPU throttling — have adequate headroom without making the suite
 * unacceptably slow on developer machines (the assertion resolves in ~1–2 s under
 * normal conditions).
 *
 * <p>All timeout and poll-interval values are declared as named constants at the
 * top of this class so they can be adjusted in one place if future profiling
 * shows the suite is either too slow or too flaky on a given environment.
 *
 * <p>Corresponds to tasks 4.1 – 4.9 in {@code prompts/mini-tasks-01.md}.
 */
// ── Task 4.1 ──────────────────────────────────────────────────────────────────
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = "outbox.poll-interval-ms=1000")
class OrderController_OutboxOrderedIT extends AbstractControllerIT {

  private static final String ORDERS_URL = "/api/v1/orders";

  // ── Task 6.5 — Timeout constants ──────────────────────────────────────────
  // Raised from 10 s → 30 s: gives 30 server poll cycles (at 1 000 ms each),
  // providing 3× headroom for CI environments with JVM warm-up or CPU throttling.
  // DB-poll Awaitility interval kept at 300 ms so assertions resolve quickly in
  // the happy-path case without hammering the DB on every cycle.
  // Kafka consumer poll kept at 500 ms to balance responsiveness vs. overhead.

  /**
   * Server outbox poll interval (mirrors {@code outbox.poll-interval-ms}).
   */
  private static final int OUTBOX_POLL_INTERVAL_MS = 1_000;

  /**
   * Awaitility hard timeout for all {@code await()} blocks in this class.
   * Set to 30 s = 30 × {@value #OUTBOX_POLL_INTERVAL_MS} ms poll cycles.
   * Increase further only if CI profiling shows consistent flakiness above 20 s.
   */
  private static final int AWAIT_TIMEOUT_SECONDS = 30;

  /**
   * Awaitility polling interval for DB-status checks.
   */
  private static final long DB_POLL_INTERVAL_MS = 300;

  /**
   * Awaitility polling interval for Kafka consumer checks.
   */
  private static final long KAFKA_POLL_INTERVAL_MS = 500;

  // ── Task 4.2 ─────────────────────────────────────────────────────────────

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() {
    jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
  }

  // ── Task 4.3 — Helpers ────────────────────────────────────────────────────

  /**
   * Returns a fresh random {@code Idempotency-Key} value.
   */
  private static String newIdempotencyKey() {
    return UUID.randomUUID().toString();
  }

  /**
   * Builds {@link HttpHeaders} with a fresh {@code Idempotency-Key}.
   */
  private static HttpHeaders idempotencyHeaders() {
    HttpHeaders h = new HttpHeaders();
    h.set("Idempotency-Key", newIdempotencyKey());
    return h;
  }

  /**
   * Builds {@link HttpHeaders} with the given {@code Idempotency-Key}.
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

  // ── Task 4.4 — createTestConsumer helper (inherited from AbstractControllerIT) ──
  // AbstractControllerIT already provides createTestConsumer(groupId) — reused below.

  /**
   * Drains all currently available records from {@code consumer} within {@code timeoutMs}
   * milliseconds, accumulating them into a list for assertion.
   */
  private static List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> drainRecords(
      KafkaConsumer<String, String> consumer, long timeoutMs) {
    List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> collected = new ArrayList<>();
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
      records.forEach(collected::add);
      if (!collected.isEmpty()) break;
    }
    return collected;
  }

  // ── Task 4.5 — @Order(1): POST → ORDER_CREATED event on Kafka ─────────────

  /**
   * POST creates an order and the outbox worker publishes an {@code ORDER_CREATED} event.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Exactly one record arrives on {@code orders.events.v1} within 10 s.</li>
   *   <li>Message key equals the order's {@code id} (UUIDv7).</li>
   *   <li>Payload is valid JSON containing {@code orderId} and {@code customerId}.</li>
   *   <li>Payload does <strong>not</strong> contain {@code debugId}.</li>
   * </ul>
   */
  @Test
  @Order(1)
  void post_publishesOrderCreatedEvent_withCorrectKeyAndPayload() throws Exception {
    OrderRequest req = validRequest();
    ResponseEntity<OrderResponse> response = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders()),
        OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = response.getBody().getId();
    UUID customerId = response.getBody().getCustomerId();

    try (KafkaConsumer<String, String> consumer = createTestConsumer("test-post-created-" + orderId)) {
      List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records = new ArrayList<>();

      await()
          .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .pollInterval(Duration.ofMillis(KAFKA_POLL_INTERVAL_MS))
          .untilAsserted(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            assertThat(records)
                .as("At least one ORDER_CREATED record must arrive on orders.events.v1")
                .isNotEmpty();
          });

      // Find the record matching this orderId
      org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record =
          records.stream()
              .filter(r -> orderId.toString().equals(r.key()))
              .findFirst()
              .orElseThrow(() -> new AssertionError(
                  "No record with key=" + orderId + " found on topic"));

      assertThat(record.key())
          .as("Message key must equal orderId")
          .isEqualTo(orderId.toString());

      JsonNode payload = objectMapper.readTree(record.value());

      assertThat(payload.has("orderId"))
          .as("Payload must contain 'orderId'")
          .isTrue();
      assertThat(payload.get("orderId").asText())
          .as("Payload orderId must match the created order")
          .isEqualTo(orderId.toString());

      assertThat(payload.has("customerId"))
          .as("Payload must contain 'customerId'")
          .isTrue();
      assertThat(payload.get("customerId").asText())
          .as("Payload customerId must match the request")
          .isEqualTo(customerId.toString());

      assertThat(record.value())
          .as("Payload must not contain 'debugId'")
          .doesNotContain("debugId");
    }
  }

  // ── Task 4.6 — @Order(2): POST → outbox row PENDING → PROCESSED ───────────

  /**
   * POST creates an outbox row in {@code PENDING} state, which the poller transitions
   * to {@code PROCESSED} after a successful Kafka publish.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Immediately after POST: exactly one row with {@code status='PENDING'}.</li>
   *   <li>Within the poll interval: row transitions to {@code status='PROCESSED'} and
   *       {@code processed_at} is not null.</li>
   * </ul>
   */
  @Test
  @Order(2)
  void post_outboxRow_transitionsPendingToProcessed() {
    OrderRequest req = validRequest();
    ResponseEntity<OrderResponse> response = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders()),
        OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = response.getBody().getId();

    // Immediately after POST: row must be PENDING
    Integer pendingCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ? AND status = 'PENDING'",
        Integer.class, orderId);
    assertThat(pendingCount)
        .as("Outbox row must be PENDING immediately after POST")
        .isEqualTo(1);

    // Within the poll interval (1 s): row must transition to PROCESSED
    await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(DB_POLL_INTERVAL_MS))
        .untilAsserted(() -> {
          Map<String, Object> row = jdbcTemplate.queryForMap(
              "SELECT status, processed_at FROM outbox_event WHERE aggregate_id = ?",
              orderId);
          assertThat(row.get("status"))
              .as("Outbox row must transition to PROCESSED")
              .isEqualTo("PROCESSED");
          assertThat(row.get("processed_at"))
              .as("processed_at must not be null after PROCESSED")
              .isNotNull();
        });
  }

  // ── Task 4.7 — @Order(3): PUT → ORDER_UPDATED event ───────────────────────

  /**
   * PUT publishes an {@code ORDER_UPDATED} event whose payload contains the updated
   * {@code totalAmount} and {@code status}.
   *
   * <p>Assertions:
   * <ul>
   *   <li>A record with key=orderId arrives on the topic within 10 s.</li>
   *   <li>Payload contains {@code totalAmount} matching the PUT request.</li>
   *   <li>Payload does not contain {@code debugId}.</li>
   * </ul>
   */
  @Test
  @Order(3)
  void put_publishesOrderUpdatedEvent_withUpdatedFields() throws Exception {
    // Create an order first
    OrderRequest createReq = validRequest();
    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    // Wait for the ORDER_CREATED outbox row to be PROCESSED before issuing the PUT.
    // This ensures the consumer (auto_offset_reset=earliest) will also see the
    // ORDER_CREATED message, which we need to filter past to find the ORDER_UPDATED one.
    await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(DB_POLL_INTERVAL_MS))
        .untilAsserted(() -> {
          String status = jdbcTemplate.queryForObject(
              "SELECT status FROM outbox_event WHERE aggregate_id = ? AND event_type = 'ORDER_CREATED'",
              String.class, orderId);
          assertThat(status).isEqualTo("PROCESSED");
        });

    // PUT — update totalAmount
    BigDecimal newAmount = new BigDecimal("299.00");
    OrderRequest updateReq = new OrderRequest();
    updateReq.setCustomerId(createReq.getCustomerId());
    updateReq.setTotalAmount(newAmount);

    ResponseEntity<OrderResponse> updated = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.PUT,
        new HttpEntity<>(updateReq, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(updated.getStatusCode()).isEqualTo(HttpStatus.OK);

    // Wait for the ORDER_UPDATED outbox row to reach PROCESSED in the DB.
    // This is the reliable signal that the event was published to Kafka — no need to
    // search for the event-type string inside the payload (it isn't there; the payload
    // is the serialised event object, not a wrapper containing the event type).
    await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(DB_POLL_INTERVAL_MS))
        .untilAsserted(() -> {
          String status = jdbcTemplate.queryForObject(
              "SELECT status FROM outbox_event WHERE aggregate_id = ? AND event_type = 'ORDER_UPDATED'",
              String.class, orderId);
          assertThat(status)
              .as("ORDER_UPDATED outbox row must be PROCESSED (published to Kafka)")
              .isEqualTo("PROCESSED");
        });

    // Now consume from Kafka and assert the payload fields.
    try (KafkaConsumer<String, String> consumer = createTestConsumer("test-put-updated-" + orderId)) {
      List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records = new ArrayList<>();

      await()
          .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .pollInterval(Duration.ofMillis(KAFKA_POLL_INTERVAL_MS))
          .untilAsserted(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            // The payload is the serialised OrderUpdatedEvent JSON.
            // It contains "totalAmount" but NOT the string "ORDER_UPDATED" —
            // that is stored only in the outbox_event.event_type column.
            // Match on key=orderId AND payload contains the updated totalAmount.
            boolean hasUpdated = records.stream()
                .filter(r -> orderId.toString().equals(r.key()))
                .anyMatch(r -> r.value().contains(newAmount.toPlainString()));
            assertThat(hasUpdated)
                .as("An ORDER_UPDATED event must arrive for orderId=" + orderId)
                .isTrue();
          });

      org.apache.kafka.clients.consumer.ConsumerRecord<String, String> updateRecord =
          records.stream()
              .filter(r -> orderId.toString().equals(r.key()))
              .filter(r -> r.value().contains(newAmount.toPlainString()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("No ORDER_UPDATED record found"));

      JsonNode payload = objectMapper.readTree(updateRecord.value());

      assertThat(payload.has("totalAmount"))
          .as("ORDER_UPDATED payload must contain 'totalAmount'")
          .isTrue();

      assertThat(updateRecord.value())
          .as("ORDER_UPDATED payload must not contain 'debugId'")
          .doesNotContain("debugId");
    }
  }

  // ── Task 4.8 — @Order(4): DELETE → ORDER_DELETED event ────────────────────

  /**
   * DELETE publishes an {@code ORDER_DELETED} event with the correct {@code orderId}.
   *
   * <p>Assertions:
   * <ul>
   *   <li>A record with {@code ORDER_DELETED} and key=orderId arrives within 10 s.</li>
   *   <li>Payload contains {@code orderId}.</li>
   *   <li>Payload does not contain {@code debugId}.</li>
   * </ul>
   */
  @Test
  @Order(4)
  void delete_publishesOrderDeletedEvent_withCorrectOrderId() throws Exception {
    // Create an order
    OrderRequest createReq = validRequest();
    ResponseEntity<OrderResponse> created = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(createReq, idempotencyHeaders()),
        OrderResponse.class);
    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = created.getBody().getId();

    // Wait for ORDER_CREATED to be published before issuing DELETE
    await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(DB_POLL_INTERVAL_MS))
        .untilAsserted(() -> {
          String status = jdbcTemplate.queryForObject(
              "SELECT status FROM outbox_event WHERE aggregate_id = ? AND event_type = 'ORDER_CREATED'",
              String.class, orderId);
          assertThat(status).isEqualTo("PROCESSED");
        });

    // DELETE
    ResponseEntity<Void> deleteResp = restTemplate.exchange(
        ORDERS_URL + "/" + orderId, HttpMethod.DELETE,
        new HttpEntity<>(null, idempotencyHeaders()),
        Void.class);
    assertThat(deleteResp.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

    // Wait for the ORDER_DELETED outbox row to reach PROCESSED in the DB.
    // The payload is the serialised OrderDeletedEvent JSON — it does NOT contain
    // the string "ORDER_DELETED"; that string lives only in outbox_event.event_type.
    // Using the DB status as the publication signal is the reliable approach.
    await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(DB_POLL_INTERVAL_MS))
        .untilAsserted(() -> {
          String status = jdbcTemplate.queryForObject(
              "SELECT status FROM outbox_event WHERE aggregate_id = ? AND event_type = 'ORDER_DELETED'",
              String.class, orderId);
          assertThat(status)
              .as("ORDER_DELETED outbox row must be PROCESSED (published to Kafka)")
              .isEqualTo("PROCESSED");
        });

    // Consume from Kafka and assert payload fields
    try (KafkaConsumer<String, String> consumer = createTestConsumer("test-delete-deleted-" + orderId)) {
      List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records = new ArrayList<>();

      await()
          .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .pollInterval(Duration.ofMillis(KAFKA_POLL_INTERVAL_MS))
          .untilAsserted(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            // Match on key=orderId; the payload contains orderId but NOT
            // the string "ORDER_DELETED" (that is the event_type column value).
            boolean hasDeleted = records.stream()
                .filter(r -> orderId.toString().equals(r.key()))
                .anyMatch(r -> r.value().contains(orderId.toString()));
            assertThat(hasDeleted)
                .as("An ORDER_DELETED event must arrive for orderId=" + orderId)
                .isTrue();
          });

      org.apache.kafka.clients.consumer.ConsumerRecord<String, String> deleteRecord =
          records.stream()
              .filter(r -> orderId.toString().equals(r.key()))
              .filter(r -> r.value().contains(orderId.toString()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("No ORDER_DELETED record found"));

      JsonNode payload = objectMapper.readTree(deleteRecord.value());

      assertThat(payload.has("orderId"))
          .as("ORDER_DELETED payload must contain 'orderId'")
          .isTrue();
      assertThat(payload.get("orderId").asText())
          .as("ORDER_DELETED orderId must match the deleted order")
          .isEqualTo(orderId.toString());

      assertThat(deleteRecord.value())
          .as("ORDER_DELETED payload must not contain 'debugId'")
          .doesNotContain("debugId");
    }
  }

  // ── Task 4.9 — @Order(5): POST idempotency replay → no duplicate event ────

  /**
   * POST with the same {@code Idempotency-Key} twice must not cause a duplicate
   * {@code ORDER_CREATED} event on the topic for the same {@code orderId}.
   *
   * <p>Strategy: consume all events for the given key, wait long enough for a duplicate
   * to appear if it were going to, then assert exactly one record for that orderId.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Both HTTP responses are 201 Created (idempotency replay).</li>
   *   <li>Exactly one Kafka record exists for the orderId (no duplicate).</li>
   * </ul>
   */
  @Test
  @Order(5)
  void post_idempotencyReplay_doesNotPublishDuplicateEvent() throws Exception {
    String key = newIdempotencyKey();
    OrderRequest req = validRequest();

    // First POST
    ResponseEntity<OrderResponse> first = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders(key)),
        OrderResponse.class);
    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID orderId = first.getBody().getId();

    // Wait for the first outbox row to reach PROCESSED before replaying
    await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(DB_POLL_INTERVAL_MS))
        .untilAsserted(() -> {
          String status = jdbcTemplate.queryForObject(
              "SELECT status FROM outbox_event WHERE aggregate_id = ?",
              String.class, orderId);
          assertThat(status).isEqualTo("PROCESSED");
        });

    // Second POST — idempotency replay (same key, same body)
    ResponseEntity<OrderResponse> second = restTemplate.exchange(
        ORDERS_URL, HttpMethod.POST,
        new HttpEntity<>(req, idempotencyHeaders(key)),
        OrderResponse.class);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // Give the poller extra time to potentially publish a duplicate, then assert it didn't
    Thread.sleep(3000);

    // Assert exactly one outbox_event row for this orderId (no second row from replay)
    Integer outboxCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ?",
        Integer.class, orderId);
    assertThat(outboxCount)
        .as("Exactly one outbox_event row must exist — no duplicate from idempotency replay")
        .isEqualTo(1);

    // Assert Kafka received exactly one record for this orderId
    try (KafkaConsumer<String, String> consumer = createTestConsumer("test-replay-nodup-" + orderId)) {
      List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records = new ArrayList<>();

      // Give the consumer a few seconds to drain everything already on the topic
      long deadline = System.currentTimeMillis() + 4000;
      while (System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(200)).forEach(records::add);
      }

      long countForOrder = records.stream()
          .filter(r -> orderId.toString().equals(r.key()))
          .count();

      assertThat(countForOrder)
          .as("Exactly one Kafka event must exist for orderId=%s — replay must not publish a duplicate", orderId)
          .isEqualTo(1);
    }
  }
}
