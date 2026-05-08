package pl.ldz.microsrv.order;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Class 5 — Broker-unavailable scenario: outbox {@code FAILED} terminal state.
 *
 * <p>Replaces and supersedes the original {@code OutboxFailedStateIT}. All tests from
 * that class are migrated here into an ordered method sequence, and four additional
 * ordered assertions are added to verify the full PENDING → FAILED lifecycle.
 *
 * <p>Extends {@link AbstractNoKafkaControllerIT}: Postgres container starts normally;
 * the Kafka bootstrap address is set to {@code localhost:9} (unreachable). The real
 * {@link KafkaTemplate} bean is replaced by a {@link MockBean} that always returns a
 * failed {@link CompletableFuture}, so the outbox poller always fails and exhausts
 * its retry budget.
 *
 * <p>{@code outbox.max-retry=2} and {@code outbox.poll-interval-ms=200} are set via
 * {@code @TestPropertySource} so retry exhaustion happens within a few seconds.
 *
 * <p>Corresponds to tasks 5.1 – 5.9 in {@code prompts/mini-tasks-01.md}.
 */
// ── Tasks 5.1 & 5.2 ───────────────────────────────────────────────────────────
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = {
    "outbox.max-retry=2",
    "outbox.poll-interval-ms=200"
})
class OrderController_OutboxFailedStateOrderedIT extends AbstractNoKafkaControllerIT {

  /**
   * Replaces the real {@code KafkaTemplate} bean for this context.
   * Every {@code send()} call returns a failed {@link CompletableFuture} so the
   * outbox poller always fails and eventually transitions the row to {@code FAILED}.
   */
  @MockBean
  private KafkaTemplate<String, String> kafkaTemplate;

  // ── Task 5.3 ──────────────────────────────────────────────────────────────

  @Autowired
  private JdbcTemplate jdbcTemplate;

  // State shared across ordered test methods (order-dependent by design)
  private static UUID sharedOrderId;

  @BeforeEach
  void setUp() {
    // Configure mock to always fail — every send() returns a completed-exceptionally future
    when(kafkaTemplate.send(anyString(), anyString(), anyString()))
        .thenAnswer(inv -> {
          CompletableFuture<Object> f = new CompletableFuture<>();
          f.completeExceptionally(new RuntimeException("Kafka unavailable (mock)"));
          return f;
        });

    // Only truncate before the first test; subsequent ordered tests rely on the
    // row inserted by @Order(1) to be present. We guard with a null check on
    // sharedOrderId: null means @Order(1) hasn't run yet.
    if (sharedOrderId == null) {
      jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
      jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
      jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
    }
  }

  // ── Task 5.4 — Helpers ────────────────────────────────────────────────────

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
   * Creates a valid {@link OrderRequest} with a random {@code customerId}.
   */
  private static OrderRequest validRequest() {
    OrderRequest req = new OrderRequest();
    req.setCustomerId(UUID.randomUUID());
    req.setTotalAmount(new BigDecimal("49.00"));
    return req;
  }

  // ── Task 5.5 — @Order(1): POST with Kafka unreachable → 201 ───────────────

  /**
   * POST creates an order and returns 201 even though Kafka is unreachable.
   *
   * <p>The outbox pattern decouples the HTTP write from the Kafka publish: the order
   * and the outbox row are persisted in the same transaction. The HTTP response is sent
   * immediately; the Kafka publish is deferred to the background poller.
   *
   * <p>Assertion: 201 Created.
   */
  @Test
  @Order(1)
  void post_kafkaUnavailable_returns201_outboxDecouplesWrite() {
    OrderRequest req = validRequest();
    HttpHeaders headers = idempotencyHeaders();

    ResponseEntity<OrderResponse> response = restTemplate.exchange(
        "/api/v1/orders", HttpMethod.POST,
        new HttpEntity<>(req, headers),
        OrderResponse.class);

    assertThat(response.getStatusCode())
        .as("201 Created must be returned even when Kafka is unreachable")
        .isEqualTo(HttpStatus.CREATED);

    assertThat(response.getBody()).isNotNull();
    sharedOrderId = response.getBody().getId();
    assertThat(sharedOrderId).isNotNull();
  }

  // ── Task 5.6 — @Order(2): Outbox row created in PENDING state ─────────────

  /**
   * Immediately after POST the outbox row must exist with {@code status='PENDING'}.
   *
   * <p>Uses the {@code sharedOrderId} set by {@code @Order(1)}.
   */
  @Test
  @Order(2)
  void post_outboxRow_createdAsPending_immediately() {
    assertThat(sharedOrderId)
        .as("@Order(1) must have run first and set sharedOrderId")
        .isNotNull();

    Integer pendingCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ? AND status = 'PENDING'",
        Integer.class, sharedOrderId);

    assertThat(pendingCount)
        .as("Outbox row must be created in PENDING state immediately after POST")
        .isEqualTo(1);
  }

  // ── Task 5.7 — @Order(3): After retry exhaustion → FAILED, retry_count = 2 ─

  /**
   * After the outbox poller exhausts its 2 retries the row must transition to
   * {@code FAILED} with {@code retry_count = 2} and {@code processed_at = NULL}.
   *
   * <p>This is the core assertion migrated from the original {@code OutboxFailedStateIT}.
   */
  @Test
  @Order(3)
  void outbox_afterRetryExhaustion_transitionsToFailed_retryCountEquals2() {
    assertThat(sharedOrderId)
        .as("@Order(1) must have run first and set sharedOrderId")
        .isNotNull();

    await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(() -> {
          Map<String, Object> row = jdbcTemplate.queryForMap(
              "SELECT status, retry_count, processed_at FROM outbox_event WHERE aggregate_id = ?",
              sharedOrderId);

          assertThat(row.get("status"))
              .as("status must be FAILED after retry exhaustion")
              .isEqualTo("FAILED");
          assertThat(((Number) row.get("retry_count")).intValue())
              .as("retry_count must equal outbox.max-retry (2)")
              .isEqualTo(2);
          assertThat(row.get("processed_at"))
              .as("processed_at must remain null — event was never successfully published")
              .isNull();
        });
  }

  // ── Task 5.8 — @Order(4): Row stays FAILED, not retried further ───────────

  /**
   * After reaching the {@code FAILED} terminal state the row must not be retried again.
   *
   * <p>Waits for two additional poll cycles (400 ms at 200 ms interval) then asserts
   * {@code retry_count} has not increased beyond 2.
   */
  @Test
  @Order(4)
  void outbox_postFailed_rowRemainsFailedNotRetriedFurther() throws Exception {
    assertThat(sharedOrderId)
        .as("@Order(3) must have completed and left the row in FAILED state")
        .isNotNull();

    // Allow two more poll cycles to pass — if the poller retried, retry_count would increase
    Thread.sleep(600);

    Map<String, Object> row = jdbcTemplate.queryForMap(
        "SELECT status, retry_count, processed_at FROM outbox_event WHERE aggregate_id = ?",
        sharedOrderId);

    assertThat(row.get("status"))
        .as("FAILED row must not change status after reaching the terminal state")
        .isEqualTo("FAILED");
    assertThat(((Number) row.get("retry_count")).intValue())
        .as("retry_count must remain 2 — FAILED rows are not retried")
        .isEqualTo(2);
    assertThat(row.get("processed_at"))
        .as("processed_at must remain null for a FAILED row")
        .isNull();
  }

  // ── Task 5.9 — @Order(5): Order data intact in orders table ───────────────

  /**
   * The order row in the {@code orders} table must be intact regardless of the outbox
   * {@code FAILED} state. The outbox failure is a publish-side concern and must not
   * affect the durability of the business data.
   *
   * <p>Assertions:
   * <ul>
   *   <li>Exactly one row exists in {@code orders} for {@code sharedOrderId}.</li>
   *   <li>{@code deleted_at IS NULL} — the order was not soft-deleted.</li>
   * </ul>
   */
  @Test
  @Order(5)
  void outbox_failed_orderDataIntact_inOrdersTable() {
    assertThat(sharedOrderId)
        .as("@Order(1) must have run first and set sharedOrderId")
        .isNotNull();

    Map<String, Object> orderRow = jdbcTemplate.queryForMap(
        "SELECT id, customer_id, status, total_amount, deleted_at FROM orders WHERE id = ?",
        sharedOrderId);

    assertThat(orderRow)
        .as("Order row must exist in the orders table despite outbox FAILED state")
        .isNotEmpty();
    assertThat(orderRow.get("id"))
        .as("orders.id must match sharedOrderId")
        .isNotNull();
    assertThat(orderRow.get("deleted_at"))
        .as("deleted_at must be null — order was not soft-deleted")
        .isNull();
    assertThat(orderRow.get("status"))
        .as("status must be set (not null)")
        .isNotNull();
  }
}
