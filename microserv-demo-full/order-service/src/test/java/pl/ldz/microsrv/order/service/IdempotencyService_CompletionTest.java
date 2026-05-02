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
import pl.ldz.microsrv.order.entity.IdempotencyKey;
import pl.ldz.microsrv.order.entity.IdempotencyStatus;
import pl.ldz.microsrv.order.exception.IdempotencySerializationException;
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyService_CompletionTest {

  @Mock
  private IdempotencyKeyRepository idempotencyKeyRepository;

  @Mock
  private ObjectMapper objectMapper;

  @InjectMocks
  private IdempotencyService idempotencyService;

  private static final String KEY = UUID.randomUUID().toString();
  private static final String REQUEST_OBJ = "{\"customerId\":\"abc\",\"totalAmount\":\"99.99\"}";
  private static final String COMPUTED_HASH = computeSha256("mock-bytes");

  @BeforeEach
  void setTtlHours() throws Exception {
    Field ttl = IdempotencyService.class.getDeclaredField("ttlHours");
    ttl.setAccessible(true);
    ttl.set(idempotencyService, 24L);
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static String computeSha256(String input) {
    try {
      var digest = java.security.MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(input.getBytes());
      return java.util.HexFormat.of().formatHex(hash);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private IdempotencyKey buildKey(IdempotencyStatus status, String requestHash) {
    IdempotencyKey k = new IdempotencyKey();
    k.setId(UUID.randomUUID());
    k.setKey(KEY);
    k.setStatus(status);
    k.setRequestHash(requestHash);
    k.setCreatedAt(OffsetDateTime.now());
    k.setExpiresAt(OffsetDateTime.now().plusHours(24));
    return k;
  }

  // ── 14.6 markCompleted ────────────────────────────────────────────────────

  @Nested
  @DisplayName("14.6 markCompleted updates status and body")
  class MarkCompleted {

    @Test
    @DisplayName("14.6.1-3 sets status=COMPLETED and responseBody on saved entity")
    void markCompleted_updatesStatusAndResponseBody() {
      // markCompleted uses findByKey (plain, non-locking) — no advisory lock involved
      IdempotencyKey existing = buildKey(IdempotencyStatus.IN_PROGRESS, COMPUTED_HASH);
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      String responseBody = "{\"id\":\"" + UUID.randomUUID() + "\"}";
      idempotencyService.markCompleted(KEY, responseBody);

      ArgumentCaptor<IdempotencyKey> captor = ArgumentCaptor.forClass(IdempotencyKey.class);
      verify(idempotencyKeyRepository).save(captor.capture());

      IdempotencyKey saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo(IdempotencyStatus.COMPLETED);
      assertThat(saved.getResponseBody()).isEqualTo(responseBody);
    }
  }

  // ── 6.8–6.10 Key validation ───────────────────────────────────────────────

  @Nested
  @DisplayName("6.x Key validation — illegal keys throw IllegalArgumentException before hitting DB")
  class KeyValidation {

    @Test
    @DisplayName("6.8 null key throws IllegalArgumentException")
    void checkAndStore_nullKey_throwsIllegalArgument() {
      assertThatThrownBy(() -> idempotencyService.checkAndStore(null, REQUEST_OBJ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Idempotency-Key must be between 1 and 255 characters");
      // Guard fires before advisory lock or any repository call
      verify(idempotencyKeyRepository, never()).tryAdvisoryLock(anyLong());
      verify(idempotencyKeyRepository, never()).findByKey(any());
    }

    @Test
    @DisplayName("6.9 blank (whitespace-only) key throws IllegalArgumentException")
    void checkAndStore_blankKey_throwsIllegalArgument() {
      assertThatThrownBy(() -> idempotencyService.checkAndStore("   ", REQUEST_OBJ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Idempotency-Key must be between 1 and 255 characters");
      verify(idempotencyKeyRepository, never()).tryAdvisoryLock(anyLong());
      verify(idempotencyKeyRepository, never()).findByKey(any());
    }

    @Test
    @DisplayName("6.10 256-character key throws IllegalArgumentException")
    void checkAndStore_oversizedKey_throwsIllegalArgument() {
      String longKey = "a".repeat(256);
      assertThatThrownBy(() -> idempotencyService.checkAndStore(longKey, REQUEST_OBJ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Idempotency-Key must be between 1 and 255 characters");
      verify(idempotencyKeyRepository, never()).tryAdvisoryLock(anyLong());
      verify(idempotencyKeyRepository, never()).findByKey(any());
    }
  }

  // ── 8.5–8.6 Serialisation failure ────────────────────────────────────────

  @Nested
  @DisplayName("8.5-8.6 Serialisation failure in computeHash")
  class SerializationFailure {

    /**
     * Task 8.5: Mocks ObjectMapper.writeValueAsBytes to throw JsonProcessingException.
     * Task 8.6: The thrown exception must be IdempotencySerializationException
     *           (not bare RuntimeException), with the original cause preserved.
     */
    @Test
    @DisplayName("8.5-8.6 Jackson failure in computeHash wraps into IdempotencySerializationException")
    void computeHash_jacksonFailure_throwsIdempotencySerializationException() throws Exception {
      // Advisory lock granted so the service proceeds past the concurrency gate into computeHash
      when(idempotencyKeyRepository.tryAdvisoryLock(anyLong())).thenReturn(true);

      // Task 8.5: mock writeValueAsBytes to throw JsonProcessingException
      when(objectMapper.writeValueAsBytes(any()))
          .thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("simulated failure") {});

      // Task 8.6: must throw IdempotencySerializationException (not bare RuntimeException)
      assertThatThrownBy(() -> idempotencyService.checkAndStore(KEY, REQUEST_OBJ))
          .isInstanceOf(IdempotencySerializationException.class)
          .hasMessageContaining("Failed to serialise request for hashing")
          .hasCauseInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class);
    }
  }
}
