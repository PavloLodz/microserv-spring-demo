package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import pl.ldz.microsrv.order.exception.OutboxSerializationException;
import pl.ldz.microsrv.order.repository.OutboxEventRepository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {

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
    // Inject self-reference (normally done by Spring via @Autowired)
    ReflectionTestUtils.setField(outboxService, "self", outboxService);
    ReflectionTestUtils.setField(outboxService, "maxRetry", 5);
    ReflectionTestUtils.setField(outboxService, "pollBatchSize", 100);
  }

  // ── 15.2 saveEvent — persists correct row ─────────────────────────────────

  @Nested
  @DisplayName("15.2 saveEvent — persists correct row")
  class SaveEvent {

    @Test
    @DisplayName("15.2.1-3 saves OutboxEvent with PENDING status and correct fields")
    void saveEvent_persistsOutboxEventWithPendingStatus() throws Exception {
      when(objectMapper.writeValueAsString(any())).thenReturn(PAYLOAD_JSON);

      outboxService.saveEvent(AGGREGATE_ID, EVENT_TYPE, new Object());

      ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
      verify(outboxEventRepository).save(captor.capture());

      OutboxEvent saved = captor.getValue();
      assertThat(saved.getId()).isNotNull();
      assertThat(saved.getAggregateType()).isEqualTo("ORDER");
      assertThat(saved.getAggregateId()).isEqualTo(AGGREGATE_ID);
      assertThat(saved.getEventType()).isEqualTo(EVENT_TYPE);
      assertThat(saved.getStatus()).isEqualTo("PENDING");
      assertThat(saved.getCreatedAt()).isNotNull();
      assertThat(saved.getPayload()).isNotEmpty();
    }
  }

  // ── 15.3 saveEvent — serialisation failure propagates ────────────────────

  @Nested
  @DisplayName("15.3 saveEvent — serialisation failure propagates")
  class SaveEventSerializationFailure {

    @Test
    @DisplayName("15.3.1-3 throws OutboxSerializationException and never calls save")
    void saveEvent_serialisationFailure_throwsRuntimeException() throws Exception {
      when(objectMapper.writeValueAsString(any()))
          .thenThrow(mock(JsonProcessingException.class));

      // T8.5.1 — assert specifically OutboxSerializationException, not just RuntimeException
      assertThatThrownBy(() -> outboxService.saveEvent(AGGREGATE_ID, EVENT_TYPE, new Object()))
          .isInstanceOf(OutboxSerializationException.class);

      verify(outboxEventRepository, never()).save(any());
    }
  }

  // ── 15.4 pollAndPublish — successful send marks row processed ─────────────

  @Nested
  @DisplayName("15.4 pollAndPublish — successful send marks row processed")
  class PollAndPublishSuccess {

    @Test
    @DisplayName("15.4.1-5 marks row PROCESSED and calls kafka with correct args")
    void pollAndPublish_successfulSend_marksRowProcessed() {
      OutboxEvent pending = buildPendingEvent();
      Page<OutboxEvent> page = new PageImpl<>(List.of(pending));
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq("PENDING"), any(Pageable.class)))
          .thenReturn(page);

      CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
      when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

      outboxService.pollAndPublish();

      ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
      verify(outboxEventRepository).save(captor.capture());

      OutboxEvent saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo("PROCESSED");
      assertThat(saved.getProcessedAt()).isNotNull();

      verify(kafkaTemplate).send("orders.events.v1", AGGREGATE_ID.toString(), PAYLOAD_JSON);
    }
  }

  // ── 15.5 pollAndPublish — Kafka failure leaves row as pending ─────────────

  @Nested
  @DisplayName("15.5 pollAndPublish — Kafka failure leaves row as pending")
  class PollAndPublishKafkaFailure {

    @Test
    @DisplayName("15.5.1-3 does not throw, increments retryCount, never saves PROCESSED status")
    void pollAndPublish_kafkaFailure_rowRemainsAsPending() {
      OutboxEvent pending = buildPendingEvent();
      Page<OutboxEvent> page = new PageImpl<>(List.of(pending));
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq("PENDING"), any(Pageable.class)))
          .thenReturn(page);

      // Fail the future so the blocking .get() throws
      CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
      failed.completeExceptionally(new RuntimeException("Kafka unavailable"));
      when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

      // Must not throw
      assertThatCode(() -> outboxService.pollAndPublish()).doesNotThrowAnyException();

      // T8.1.2 — assert retryCount is incremented
      ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
      verify(outboxEventRepository).save(captor.capture());
      OutboxEvent saved = captor.getValue();
      assertThat(saved.getRetryCount()).isEqualTo(1);

      // save should never be called with PROCESSED
      verify(outboxEventRepository, never()).save(argThat(e -> "PROCESSED".equals(e.getStatus())));
    }
  }

  // ── T8.2 — retry counter increments below threshold ───────────────────────

  @Nested
  @DisplayName("T8.2 pollAndPublish — failure below maxRetry increments count, stays PENDING")
  class PollAndPublishRetryBelowThreshold {

    @Test
    @DisplayName("T8.2.1 increments retryCount and leaves status as PENDING")
    void pollAndPublish_failureBeforeMaxRetry_incrementsCountStaysPending() {
      ReflectionTestUtils.setField(outboxService, "maxRetry", 3);

      OutboxEvent pending = buildPendingEvent(); // retryCount = 0
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

      // One attempt away from permanently failing
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

  // ── T8.4 — batch size limit is applied ────────────────────────────────────

  @Nested
  @DisplayName("T8.4 pollAndPublish — batch size limit applied via Pageable query")
  class PollAndPublishBatchLimit {

    @Test
    @DisplayName("T8.4.1 uses pageable query with correct page size")
    void pollAndPublish_batchLimitApplied_usesPageableQuery() {
      Page<OutboxEvent> emptyPage = new PageImpl<>(List.of());
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq("PENDING"), any(Pageable.class)))
          .thenReturn(emptyPage);

      outboxService.pollAndPublish();

      ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
      verify(outboxEventRepository).findByStatusOrderByCreatedAtAsc(eq("PENDING"), pageableCaptor.capture());
      assertThat(pageableCaptor.getValue().getPageSize()).isEqualTo(100);

      // Verify the old unbounded overload is NOT called
      verify(outboxEventRepository, never()).findByStatusOrderByCreatedAtAsc(anyString());
    }
  }

  // ── helper ────────────────────────────────────────────────────────────────

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
}
