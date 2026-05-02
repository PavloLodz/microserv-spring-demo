package pl.ldz.microsrv.order;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
 * T9 — Integration test for the outbox {@code FAILED} terminal state.
 *
 * <p>This is a standalone top-level {@code @SpringBootTest} class rather than a nested class
 * inside {@link OrderController_FullFlowOrderedIT}. JUnit 5 nested classes with {@code @SpringBootTest} share
 * the enclosing class's Spring context, which means a {@code @MockBean} declared inside a
 * nested class does NOT create a fresh isolated context — it is applied to the already-loaded
 * enclosing context, causing race conditions and unpredictable mock behaviour.
 *
 * <p>By keeping this as a top-level class, Spring creates a fully independent application
 * context where {@code @MockBean KafkaTemplate} is reliably wired before any test method runs.
 *
 * <p>Properties override {@code outbox.max-retry=2} and {@code outbox.poll-interval-ms=200}
 * so the poller retries quickly and the test completes in a few seconds.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
    "outbox.max-retry=2",
    "outbox.poll-interval-ms=200"
})
class OutboxFailedStateIT extends AbstractNoKafkaControllerIT {

  /**
   * Replaces the real {@code KafkaTemplate} bean for this context.
   * Every {@code send()} call returns a failed {@link CompletableFuture} so the
   * outbox poller always fails and eventually transitions the row to {@code FAILED}.
   */
  @MockBean
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() {
    // Configure mock to always fail — every send() returns a completed-exceptionally future
    when(kafkaTemplate.send(anyString(), anyString(), anyString()))
        .thenAnswer(inv -> {
          CompletableFuture<Object> f = new CompletableFuture<>();
          f.completeExceptionally(new RuntimeException("Kafka unavailable (mock)"));
          return f;
        });

    jdbcTemplate.execute("TRUNCATE TABLE idempotency_key CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE outbox_event CASCADE");
    jdbcTemplate.execute("TRUNCATE TABLE orders CASCADE");
  }

  /**
   * T9.1 — Creates an order, then waits for the outbox poller to exhaust its 2 retries
   * and transition the row to {@code FAILED} status.
   *
   * <p>Asserts:
   * <ul>
   *   <li>{@code status = 'FAILED'} (terminal state after max retries)</li>
   *   <li>{@code retry_count = 2} (equals {@code outbox.max-retry})</li>
   *   <li>{@code processed_at IS NULL} (event was never successfully published)</li>
   * </ul>
   */
  @Test
  void outbox_failedEvent_markedFailed_afterMaxRetry() {
    // Create an order — this writes a PENDING outbox row atomically
    OrderRequest req = new OrderRequest();
    req.setCustomerId(UUID.randomUUID());
    req.setTotalAmount(new BigDecimal("49.00"));

    HttpHeaders headers = new HttpHeaders();
    headers.set("Idempotency-Key", UUID.randomUUID().toString());

    ResponseEntity<OrderResponse> response = restTemplate.exchange(
        "/api/v1/orders",
        HttpMethod.POST,
        new HttpEntity<>(req, headers),
        OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID aggregateId = response.getBody().getId();

    // Wait until the outbox poller exhausts retries (max-retry=2, poll interval=200ms)
    // and marks the row FAILED with retry_count=2 and processed_at NULL
    await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(() -> {
          Map<String, Object> row = jdbcTemplate.queryForMap(
              "SELECT status, retry_count, processed_at FROM outbox_event WHERE aggregate_id = ?",
              aggregateId);
          assertThat(row.get("status")).isEqualTo("FAILED");
          assertThat(((Number) row.get("retry_count")).intValue()).isEqualTo(2);
          assertThat(row.get("processed_at")).isNull();
        });
  }
}
