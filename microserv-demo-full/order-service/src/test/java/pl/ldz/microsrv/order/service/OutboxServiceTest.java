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
import org.springframework.kafka.core.KafkaTemplate;
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
    ReflectionTestUtils.setField(outboxService, "topic", "orders.events.v1");
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
