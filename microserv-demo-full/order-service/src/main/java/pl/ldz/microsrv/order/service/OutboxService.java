package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.microsrv.order.entity.OutboxEvent;
import pl.ldz.microsrv.order.exception.OutboxSerializationException;
import pl.ldz.microsrv.order.repository.OutboxEventRepository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Owns the transactional outbox pattern:
 * <ul>
 *   <li>{@link #saveEvent} — write path, called within an active {@code @Transactional} scope.</li>
 *   <li>{@link #pollAndPublish} — background worker that publishes pending events to Kafka.</li>
 * </ul>
 *
 * <p>Never references {@code OrderRepository} or {@code IdempotencyKeyRepository}.
 * No {@code debugId} is present anywhere in this class or the payloads it writes.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

  private static final String PENDING = "PENDING";
  private static final String PROCESSED = "PROCESSED";
  private static final String FAILED = "FAILED";

  private final OutboxEventRepository outboxEventRepository;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  @Value("${outbox.max-retry:5}")
  private int maxRetry;

  @Value("${outbox.poll-batch-size:100}")
  private int pollBatchSize;

  @Value("${kafka.topic.orders-events:orders.events.v1}")
  private String topic;

  /**
   * Self-reference to allow Spring's proxy to intercept {@code @Transactional(REQUIRES_NEW)}
   * methods called from within the same bean.
   * {@code @Lazy} breaks the circular-reference detection introduced in Spring Boot 3.x
   * while still providing the AOP proxy needed for {@code REQUIRES_NEW} semantics.
   */
  @Lazy
  @Autowired
  private OutboxService self;

  // ── Write Path ────────────────────────────────────────────────────────────

  /**
   * Persists an outbox event within the caller's active transaction.
   * If serialisation fails, an {@link OutboxSerializationException} is thrown so the
   * wrapping transaction rolls back.
   *
   * @param aggregateId the order UUID this event belongs to
   * @param eventType   e.g. {@code "ORDER_CREATED"}
   * @param payload     domain event object to serialise to JSON
   */
  public void saveEvent(UUID aggregateId, String eventType, Object payload) {
    String json;
    try {
      json = objectMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new OutboxSerializationException(eventType, e);
    }

    OutboxEvent event = new OutboxEvent();
    event.setId(Generators.timeBasedEpochGenerator().generate());
    event.setAggregateType("ORDER");
    event.setAggregateId(aggregateId);
    event.setEventType(eventType);
    event.setPayload(json);
    event.setStatus(PENDING);
    event.setCreatedAt(OffsetDateTime.now());

    outboxEventRepository.save(event);
    log.debug("Outbox event saved: id={}, eventType={}", event.getId(), eventType);
  }

  // ── Status Transition Methods (REQUIRES_NEW) ──────────────────────────────

  /**
   * Marks a row as {@code PROCESSED} in its own transaction so the commit
   * is independent of the polling loop.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void markProcessed(OutboxEvent row) {
    row.setStatus(PROCESSED);
    row.setProcessedAt(OffsetDateTime.now());
    outboxEventRepository.save(row);
    log.info("Outbox event published: id={}, eventType={}", row.getId(), row.getEventType());
  }

  /**
   * Increments {@code retryCount} and, once the count reaches {@code maxRetry},
   * transitions the row to the terminal {@code FAILED} state. Each update is
   * committed in its own transaction so partial failures don't affect other rows.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void markFailed(OutboxEvent row) {
    row.setRetryCount(row.getRetryCount() + 1);
    if (row.getRetryCount() >= maxRetry) {
      row.setStatus(FAILED);
      log.error("Outbox event permanently failed after {} retries: id={}, eventType={}",
                maxRetry, row.getId(), row.getEventType());
    } else {
      log.warn("Outbox event retry {}/{}: id={}, eventType={}",
               row.getRetryCount(), maxRetry, row.getId(), row.getEventType());
    }
    outboxEventRepository.save(row);
  }

  // ── Background Worker ─────────────────────────────────────────────────────

  /**
   * Polls for pending outbox events and publishes them to Kafka.
   * Runs on a fixed delay (default 5 s, configurable via {@code outbox.poll-interval-ms}).
   *
   * <p><strong>Delivery guarantee: at-least-once.</strong>
   * The Kafka send completes before the row is marked {@code PROCESSED}.
   * If the service crashes after a successful send but before the DB commit,
   * the row remains {@code PENDING} and will be re-published on the next cycle.
   * Consumers of {@code orders.events.v1} must be idempotent on {@code orderId}.
   *
   * <p>A failed Kafka send increments {@code retryCount}. Once the count reaches
   * {@code maxRetry}, the row transitions to the terminal {@code FAILED} state
   * and is no longer retried.
   *
   * <p>Status updates are committed in {@code REQUIRES_NEW} sub-transactions via
   * {@code self.markProcessed} / {@code self.markFailed}, so each row's outcome
   * is persisted independently of the others in the batch.
   *
   * <p>The batch is limited to {@code pollBatchSize} rows per cycle to bound
   * memory usage under backlog conditions.
   */
  @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:5000}")
  public void pollAndPublish() {
    try {
      List<OutboxEvent> pending = outboxEventRepository
          .findByStatusOrderByCreatedAtAsc(PENDING, PageRequest.of(0, pollBatchSize))
          .getContent();

      for (OutboxEvent row : pending) {
        try (var mdcOrderId = MDC.putCloseable("orderId", row.getAggregateId().toString())) {
          MDC.put("eventType", row.getEventType());
          try {
            // Blocking send — ensures success/failure is known before marking the row
            kafkaTemplate.send(topic, row.getAggregateId().toString(), row.getPayload()).get();
            self.markProcessed(row);
          } catch (Exception e) {
            self.markFailed(row);
          } finally {
            MDC.remove("eventType");
          }
        }
      }
    } catch (Exception e) {
      // Guard against unexpected errors to prevent killing the scheduler thread
      log.error("Unexpected error in OutboxService.pollAndPublish", e);
    }
  }
}
