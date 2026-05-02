package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import pl.ldz.microsrv.order.entity.OutboxEvent;
import pl.ldz.microsrv.order.repository.OutboxEventRepository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxService_RetryTest {

  @Mock
  private OutboxEventRepository outboxEventRepository;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private ObjectMapper objectMapper;

  @InjectMocks
  private OutboxService outboxService;

  private static final UUID AGGREGATE_ID = UUID.randomUUID();
  private static final String EVENT_TYPE = "ORDER_CREATED";
  private static final String PAYLOAD_JSON = "{\"orderId\":\"" + AGGREGATE_ID + "\"}";

  @BeforeEach
  void injectSelf() {
    ReflectionTestUtils.setField(outboxService, "self", outboxService);
    ReflectionTestUtils.setField(outboxService, "maxRetry", 5);
    ReflectionTestUtils.setField(outboxService, "pollBatchSize", 100);
    ReflectionTestUtils.setField(outboxService, "topic", "orders.events.v1");
  }

  private OutboxEvent buildPendingEvent() {
    OutboxEvent e = new OutboxEvent();
    e.setId(UUID.randomUUID());
    e.setAggregateType("ORDER");
    e.setAggregateId(AGGREGATE_ID);
    e.setEventType(EVENT_TYPE);
    e.setPayload(PAYLOAD_JSON);
    e.setStatus("PENDING");
    e.setCreatedAt(OffsetDateTime.now());
    e.setRetryCount(0);
    return e;
  }

  // ── T8.2 — retry counter increments below threshold ───────────────────────

  @Nested
  @DisplayName("T8.2 pollAndPublish — failure below maxRetry increments count, stays PENDING")
  class PollAndPublishRetryBelowThreshold {

    @Test
    @DisplayName("T8.2.1 increments retryCount and leaves status as PENDING")
    void pollAndPublish_failureBeforeMaxRetry_incrementsCountStaysPending() {
      ReflectionTestUtils.setField(outboxService, "maxRetry", 3);

      OutboxEvent pending = buildPendingEvent();
      Page<OutboxEvent> page = new PageImpl<>(List.of(pending));
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq("PENDING"), any(Pageable.class)))
          .thenReturn(page);

      CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
      failed.completeExceptionally(new RuntimeException("Kafka unavailable"));
      when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

      outboxService.pollAndPublish();

      ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
      verify(outboxEventRepository).save(captor.capture());
      OutboxEvent saved = captor.getValue();
      assertThat(saved.getRetryCount()).isEqualTo(1);
      assertThat(saved.getStatus()).isEqualTo("PENDING");
    }
  }

  // ── T8.3 — row transitions to FAILED at threshold ─────────────────────────

  @Nested
  @DisplayName("T8.3 pollAndPublish — failure at maxRetry sets status FAILED")
  class PollAndPublishRetryAtThreshold {

    @Test
    @DisplayName("T8.3.1 sets status FAILED when retryCount reaches maxRetry")
    void pollAndPublish_failureAtMaxRetry_setsStatusFailed() {
      int maxRetry = 3;
      ReflectionTestUtils.setField(outboxService, "maxRetry", maxRetry);

      OutboxEvent pending = buildPendingEvent();
      pending.setRetryCount(maxRetry - 1);
      Page<OutboxEvent> page = new PageImpl<>(List.of(pending));
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq("PENDING"), any(Pageable.class)))
          .thenReturn(page);

      CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
      failed.completeExceptionally(new RuntimeException("Kafka unavailable"));
      when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

      outboxService.pollAndPublish();

      ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
      verify(outboxEventRepository).save(captor.capture());
      OutboxEvent saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo("FAILED");
      assertThat(saved.getRetryCount()).isEqualTo(maxRetry);
    }
  }
}
