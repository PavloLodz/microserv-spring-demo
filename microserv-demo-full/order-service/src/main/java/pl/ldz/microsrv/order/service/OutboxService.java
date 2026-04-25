package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.microsrv.order.entity.OutboxEvent;
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

  private static final String TOPIC = "orders.events.v1";
  private static final String PENDING = "PENDING";
  private static final String PROCESSED = "PROCESSED";

  private final OutboxEventRepository outboxEventRepository;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  // ── Write Path ────────────────────────────────────────────────────────────

  /**
     * Persists an outbox event within the caller's active transaction.
     * If serialisation fails, a {@link RuntimeException} is thrown so the
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
      throw new RuntimeException("Failed to serialise outbox payload for eventType=" + eventType, e);
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

  // ── Background Worker ─────────────────────────────────────────────────────

  /**
     * Polls for pending outbox events and publishes them to Kafka.
     * Runs on a fixed delay (default 5 s, configurable via {@code outbox.poll-interval-ms}).
     *
     * <p>A failed Kafka send is logged at ERROR and the row is left as {@code PENDING}
     * for the next cycle — the method itself never throws so the scheduler thread
     * remains alive.
     */
  @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:5000}")
  @Transactional
  public void pollAndPublish() {
    try {
      List<OutboxEvent> pending = outboxEventRepository.findByStatusOrderByCreatedAtAsc(PENDING);

      for (OutboxEvent row : pending) {
        try {
          // Blocking send — ensures success/failure is known before marking the row
          kafkaTemplate.send(TOPIC, row.getAggregateId().toString(), row.getPayload()).get();

          row.setStatus(PROCESSED);
          row.setProcessedAt(OffsetDateTime.now());
          outboxEventRepository.save(row);
          log.info("Outbox event published: id={}, eventType={}", row.getId(), row.getEventType());

        } catch (Exception e) {
          log.error("Failed to publish outbox event: id={}, eventType={}", row.getId(), row.getEventType(), e);
          // Leave row as PENDING for the next poll cycle — do not rethrow
        }
      }
    } catch (Exception e) {
      // Guard against unexpected errors to prevent killing the scheduler thread
      log.error("Unexpected error in OutboxService.pollAndPublish", e);
    }
  }
}
