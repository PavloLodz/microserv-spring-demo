package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import pl.ldz.microsrv.order.entity.OutboxEvent;
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
    @DisplayName("15.3.1-3 throws RuntimeException and never calls save")
    void saveEvent_serialisationFailure_throwsRuntimeException() throws Exception {
      when(objectMapper.writeValueAsString(any()))
          .thenThrow(mock(JsonProcessingException.class));

      assertThatThrownBy(() -> outboxService.saveEvent(AGGREGATE_ID, EVENT_TYPE, new Object()))
          .isInstanceOf(RuntimeException.class);

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
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING"))
          .thenReturn(List.of(pending));

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
    @DisplayName("15.5.1-3 does not throw, never saves PROCESSED status")
    void pollAndPublish_kafkaFailure_rowRemainsAsPending() {
      OutboxEvent pending = buildPendingEvent();
      when(outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING"))
          .thenReturn(List.of(pending));

      // Fail the future so the blocking .get() throws
      CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
      failed.completeExceptionally(new RuntimeException("Kafka unavailable"));
      when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

      // Must not throw
      assertThatCode(() -> outboxService.pollAndPublish()).doesNotThrowAnyException();

      // save should never be called with PROCESSED
      verify(outboxEventRepository, never()).save(argThat(e -> "PROCESSED".equals(e.getStatus())));
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
    return e;
  }
}
